"""
Main Constellation Service

Integrates all Constellation components to provide distributed container orchestration
with high availability and automatic failover.
"""
from __future__ import annotations

import asyncio
import signal
import sys

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import docker
import structlog
import uvicorn

from fastapi import FastAPI, HTTPException

from constellation.config.logging import configure_logging
from constellation.discovery import DiscoveryConfig, NodeDiscoveryService, NodeEvent
from constellation.failover import FailoverManager

if TYPE_CHECKING:
    from constellation.failover import FailoverRule

logger = structlog.get_logger(__name__)


class ConstellationService:
    """Main Constellation orchestration service."""

    def __init__(self, config_path: Path | None = None):
        """
        Initialize Constellation service.

        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        self.config = self._load_config(config_path)

        # Initialize logging
        configure_logging(
            log_level=self.config.get("logging", {}).get("level", "INFO"),
            json_logs=self.config.get("logging", {}).get("json_format", False),
        )

        # Initialize Docker client
        self.docker_client = docker.from_env()

        # Initialize discovery service
        discovery_config = DiscoveryConfig(
            service_port=self.config.get("node", {}).get("port", 5443),
            bind_address=self.config.get("node", {}).get("bind", "0.0.0.0"),
            tailscale_enabled=self.config.get("tailscale", {}).get("enabled", True),
        )
        self.discovery_service = NodeDiscoveryService(discovery_config)

        # Initialize failover manager
        self.failover_manager = FailoverManager(
            docker_client=self.docker_client,
            node_discovery_service=self.discovery_service,
        )

        # Service state
        self._running = False
        self._shutdown_event = asyncio.Event()

        # FastAPI app for REST API
        self.app = self._create_fastapi_app()

        logger.info("Constellation service initialized")

    def _load_config(self, config_path: Path | None) -> dict[str, Any]:
        """Load configuration from file or use defaults."""
        default_config = {
            "node": {"port": 5443, "bind": "0.0.0.0", "name": "constellation-node"},
            "tailscale": {"enabled": True, "timeout": 10},
            "docker": {
                "socket": "/var/run/docker.sock",
                "default_network": "constellation",
            },
            "failover": {
                "health_check_interval": 30,
                "max_restart_attempts": 3,
                "enable_failover": True,
            },
            "logging": {"level": "INFO", "json_format": False},
        }

        if config_path and config_path.exists():
            try:
                import yaml

                with open(config_path, "r") as f:
                    file_config = yaml.safe_load(f)
                    # Merge with defaults
                    default_config.update(file_config)
            except Exception as e:
                logger.warning(
                    "Failed to load config file, using defaults",
                    config_path=str(config_path),
                    error=str(e),
                )

        return default_config

    def _create_fastapi_app(self) -> FastAPI:
        """Create FastAPI application with API endpoints."""
        app = FastAPI(
            title="Constellation",
            description="Distributed Container Orchestration Service",
            version="0.1.0",
        )

        @app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

        @app.get("/status")
        async def get_status():
            """Get service status."""
            try:
                cluster_status = await self.discovery_service.get_cluster_status()
                return {
                    "service": "constellation",
                    "running": self._running,
                    "cluster": cluster_status,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            except Exception as e:
                logger.error("Failed to get status", error=str(e))
                raise HTTPException(status_code=500, detail="Failed to get status")

        @app.get("/nodes")
        async def get_nodes():
            """Get all discovered nodes."""
            try:
                cluster_status = await self.discovery_service.get_cluster_status()
                return cluster_status.get("nodes", {})
            except Exception as e:
                logger.error("Failed to get nodes", error=str(e))
                raise HTTPException(status_code=500, detail="Failed to get nodes")

        @app.get("/nodes/{node_id}")
        async def get_node(node_id: str):
            """Get specific node information."""
            try:
                node = await self.discovery_service.registry.get_node(node_id)
                if not node:
                    raise HTTPException(status_code=404, detail="Node not found")

                return {
                    "node_id": node.node_id,
                    "hostname": node.hostname,
                    "ip_address": node.ip_address,
                    "status": node.status,
                    "last_seen": node.last_seen.isoformat() if node.last_seen else None,
                    "is_healthy": node.is_healthy(),
                    "capabilities": list(node.capabilities),
                    "metadata": node.metadata,
                }
            except HTTPException:
                raise
            except Exception as e:
                logger.error("Failed to get node", node_id=node_id, error=str(e))
                raise HTTPException(status_code=500, detail="Failed to get node")

        @app.get("/containers")
        async def get_containers():
            """Get all monitored containers."""
            try:
                return self.failover_manager.get_all_container_status()
            except Exception as e:
                logger.error("Failed to get containers", error=str(e))
                raise HTTPException(status_code=500, detail="Failed to get containers")

        @app.get("/containers/{container_name}")
        async def get_container(container_name: str):
            """Get specific container status."""
            try:
                status = self.failover_manager.get_container_status(container_name)
                if not status:
                    raise HTTPException(status_code=404, detail="Container not found")
                return status
            except HTTPException:
                raise
            except Exception as e:
                logger.error(
                    "Failed to get container", container=container_name, error=str(e)
                )
                raise HTTPException(status_code=500, detail="Failed to get container")

        @app.post("/containers/{container_name}/failover")
        async def trigger_failover(container_name: str):
            """Manually trigger failover for a container."""
            try:
                # Check if container is registered
                status = self.failover_manager.get_container_status(container_name)
                if not status:
                    raise HTTPException(status_code=404, detail="Container not found")

                # Trigger failover
                await self.failover_manager._trigger_failover(container_name)

                return {"message": f"Failover triggered for {container_name}"}
            except HTTPException:
                raise
            except Exception as e:
                logger.error(
                    "Failed to trigger failover", container=container_name, error=str(e)
                )
                raise HTTPException(
                    status_code=500, detail="Failed to trigger failover"
                )

        return app

    async def start(self) -> None:
        """Start the Constellation service."""
        if self._running:
            logger.warning("Constellation service already running")
            return

        logger.info("Starting Constellation service")

        try:
            # Start discovery service
            await self.discovery_service.start()

            # Start failover manager
            await self.failover_manager.start()

            # Set up event handlers
            self._setup_event_handlers()

            self._running = True
            logger.info("Constellation service started successfully")

        except Exception as e:
            logger.error("Failed to start Constellation service", error=str(e))
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the Constellation service."""
        if not self._running:
            return

        logger.info("Stopping Constellation service")
        self._running = False

        try:
            # Stop failover manager
            await self.failover_manager.stop()

            # Stop discovery service
            await self.discovery_service.stop()

            # Close Docker client
            self.docker_client.close()

            logger.info("Constellation service stopped")

        except Exception as e:
            logger.error("Error stopping Constellation service", error=str(e))

        self._shutdown_event.set()

    def _setup_event_handlers(self) -> None:
        """Set up event handlers for service integration."""

        # Handle node discovery events
        self.discovery_service.add_event_handler(
            NodeEvent.NODE_DISCOVERED, self._on_node_discovered
        )

        self.discovery_service.add_event_handler(
            NodeEvent.NODE_LEFT, self._on_node_left
        )

        self.discovery_service.add_event_handler(
            NodeEvent.NODE_HEALTH_CHANGED, self._on_node_health_changed
        )

    async def _on_node_discovered(self, event: NodeEvent, **kwargs) -> None:
        """Handle node discovery events."""
        node = kwargs.get("node")
        if node:
            logger.info(
                "New node discovered",
                node_id=node.node_id,
                hostname=node.hostname,
                ip=node.ip_address,
            )

    async def _on_node_left(self, event: NodeEvent, **kwargs) -> None:
        """Handle node departure events."""
        node_id = kwargs.get("node_id")
        if node_id:
            logger.info("Node left cluster", node_id=node_id)

            # TODO: Handle container redistribution if needed

    async def _on_node_health_changed(self, event: NodeEvent, **kwargs) -> None:
        """Handle node health change events."""
        node_id = kwargs.get("node_id")
        healthy = kwargs.get("healthy", False)

        if node_id:
            logger.info("Node health changed", node_id=node_id, healthy=healthy)

            if not healthy:
                # TODO: Consider triggering failover for containers on unhealthy nodes
                pass

    async def register_container(
        self, container_name: str, failover_rule: FailoverRule | None = None
    ) -> None:
        """
        Register a container for monitoring and failover.

        Args:
            container_name: Name of the container
            failover_rule: Failover configuration
        """
        try:
            # Get current node ID
            node_id = self.discovery_service.node_id

            # Register with failover manager
            self.failover_manager.register_container(
                container_name=container_name,
                node_id=node_id,
                failover_rule=failover_rule,
            )

            logger.info(
                "Container registered for monitoring",
                container=container_name,
                node=node_id,
            )

        except Exception as e:
            logger.error(
                "Failed to register container", container=container_name, error=str(e)
            )
            raise

    async def unregister_container(
        self, container_name: str) -> None:
        """Unregister a container from monitoring.

        Args:
            container_name: Name of the container
        """
        try:
            self.failover_manager.unregister_container(container_name)
            logger.info("Container unregistered", container=container_name)
        except Exception as e:
            logger.error( "Failed to unregister container", container=container_name, error=f"{e.__class__.__name__}: {e}")
            raise

    async def run_api_server(self) -> None:
        """Run the FastAPI server."""
        config = uvicorn.Config(
            app=self.app,
            host=self.config["node"]["bind"],
            port=self.config["node"]["port"],
            log_config=None,  # We handle logging ourselves
            access_log=False,
        )

        server = uvicorn.Server(config)

        # Set up signal handlers
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal", signal=signum)
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            await server.serve()
        except Exception as e:
            logger.error("API server error", error=f"{e.__class__.__name__}: {e}")
            raise

    async def run(self) -> None:
        """Run the complete Constellation service."""
        try:
            # Start all services
            await self.start()

            # Run API server
            await self.run_api_server()

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error("Service error", error=f"{e.__class__.__name__}: {e}")
            raise
        finally:
            await self.stop()


async def main():
    """Main entry point for Constellation service."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Constellation Distributed Container Orchestration"
    )
    parser.add_argument("--config", "-c", type=Path, help="Configuration file path")
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    args = parser.parse_args()

    # Create and run service
    service = ConstellationService(config_path=args.config)

    try:
        await service.run()
    except Exception as e:
        logger.error("Failed to run Constellation service", error=f"{e.__class__.__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
