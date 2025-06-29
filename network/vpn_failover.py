"""
Distributed VPN Failover System for Constellation

Handles VPN container failover across multiple nodes, managing network routing
and ensuring containers can seamlessly switch between VPN providers when failures occur.
"""

from __future__ import annotations

import asyncio

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

import structlog

from constellation.failover.manager import FailoverEvent

if TYPE_CHECKING:
    from docker.client import DockerClient

    from constellation.discovery.node_discovery import NodeDiscoveryService, NodeInfo
    from constellation.failover.manager import FailoverManager

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class VPNType(str, Enum):
    """Supported VPN types."""

    GLUETUN = "gluetun"
    CLOUDFLARE_WARP = "warp"
    TAILSCALE = "tailscale"
    NORDLYNX = "nordlynx"
    WIREGUARD = "wireguard"
    OPENVPN = "openvpn"


class VPNStatus(str, Enum):
    """VPN container status."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    FAILED = "failed"
    STARTING = "starting"
    UNKNOWN = "unknown"


@dataclass
class VPNContainer:
    """Represents a VPN container in the cluster."""

    name: str
    node_id: str
    vpn_type: VPNType
    ip_address: str
    port: int = 8080  # Default health check port
    priority: int = 100  # Lower number = higher priority
    status: VPNStatus = VPNStatus.UNKNOWN
    last_health_check: datetime | None = None
    dependent_containers: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        """Check if VPN container is healthy."""
        return self.status == VPNStatus.HEALTHY

    def get_health_age(self) -> timedelta | None:
        """Get time since last health check."""
        if not self.last_health_check:
            return None
        return datetime.utcnow() - self.last_health_check


@dataclass
class VPNRoute:
    """Represents a VPN routing configuration."""

    dependent_container: str
    primary_vpn: str
    fallback_vpns: list[str] = field(default_factory=list)
    current_vpn: str | None = None
    failover_count: int = 0
    last_failover: datetime | None = None

    def get_next_fallback(self) -> str | None:
        """Get the next fallback VPN in the list."""
        if self.failover_count < len(self.fallback_vpns):
            return self.fallback_vpns[self.failover_count]
        return None

    def reset_to_primary(self) -> None:
        """Reset route to primary VPN."""
        self.current_vpn = self.primary_vpn
        self.failover_count = 0


class DistributedVPNFailover:
    """Manages VPN failover across multiple nodes in the constellation."""

    def __init__(
        self,
        node_discovery: NodeDiscoveryService,
        failover_manager: FailoverManager,
        docker_client: DockerClient,
    ):
        """
        Initialize distributed VPN failover system.

        Args:
            node_discovery: Node discovery service for cluster communication
            failover_manager: Local failover manager
            docker_client: Docker client for container operations
        """
        self.node_discovery: NodeDiscoveryService = node_discovery
        self.failover_manager: FailoverManager = failover_manager
        self.docker_client: DockerClient = docker_client

        # VPN container registry
        self.vpn_containers: dict[str, VPNContainer] = {}
        self.vpn_routes: dict[str, VPNRoute] = {}

        # Service state
        self._running: bool = False
        self._health_check_task: asyncio.Task | None = None
        self._sync_task: asyncio.Task | None = None

        # Configuration
        self.health_check_interval: timedelta = timedelta(seconds=30)
        self.sync_interval: timedelta = timedelta(seconds=60)
        self.failover_cooldown: timedelta = timedelta(minutes=2)

        logger.info("Distributed VPN failover system initialized")

    async def start(self) -> None:
        """Start the VPN failover service."""
        if self._running:
            logger.warning("VPN failover service already running")
            return

        self._running: bool = True

        # Register for failover events
        self.failover_manager.add_event_handler(
            FailoverEvent.CONTAINER_FAILED, self._handle_container_failure
        )
        self.failover_manager.add_event_handler(
            FailoverEvent.CONTAINER_UNHEALTHY, self._handle_container_unhealthy
        )

        # Start background tasks
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._sync_task = asyncio.create_task(self._sync_loop())

        logger.info("VPN failover service started")

    async def stop(self) -> None:
        """Stop the VPN failover service."""
        self._running = False

        # Cancel background tasks
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        logger.info("VPN failover service stopped")

    def register_vpn_container(
        self,
        name: str,
        node_id: str,
        vpn_type: VPNType,
        ip_address: str,
        priority: int = 100,
        **kwargs,
    ) -> None:
        """Register a VPN container for monitoring."""
        vpn_container = VPNContainer(
            name=name,
            node_id=node_id,
            vpn_type=vpn_type,
            ip_address=ip_address,
            priority=priority,
            **kwargs,
        )

        self.vpn_containers[name] = vpn_container
        logger.info(
            "Registered VPN container",
            container=name,
            node=node_id,
            type=vpn_type.value,
            priority=priority,
        )

    def register_vpn_route(
        self,
        dependent_container: str,
        primary_vpn: str,
        fallback_vpns: list[str] | None = None,
    ) -> None:
        """
        Register a VPN routing configuration.

        Args:
            dependent_container: Container that depends on VPN routing
            primary_vpn: Primary VPN container name
            fallback_vpns: List of fallback VPN container names
        """
        route = VPNRoute(
            dependent_container=dependent_container,
            primary_vpn=primary_vpn,
            fallback_vpns=fallback_vpns or [],
            current_vpn=primary_vpn,
        )

        self.vpn_routes[dependent_container] = route

        # Add dependent container to VPN container tracking
        if primary_vpn in self.vpn_containers:
            self.vpn_containers[primary_vpn].dependent_containers.add(dependent_container)

        for fallback in route.fallback_vpns:
            if fallback in self.vpn_containers:
                self.vpn_containers[fallback].dependent_containers.add(dependent_container)

        logger.info(
            "Registered VPN route",
            dependent=dependent_container,
            primary=primary_vpn,
            fallbacks=fallback_vpns,
        )

    async def _health_check_loop(self) -> None:
        """Background task for VPN health checking."""
        while self._running:
            try:
                await self._check_all_vpn_health()
                await asyncio.sleep(self.health_check_interval.total_seconds())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in VPN health check loop", error=f"{e.__class__.__name__}: {e}")
                await asyncio.sleep(5)

    async def _sync_loop(self) -> None:
        """Background task for syncing VPN state across nodes."""
        while self._running:
            try:
                await self._sync_vpn_state()
                await asyncio.sleep(self.sync_interval.total_seconds())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in VPN sync loop", error=f"{e.__class__.__name__}: {e}")
                await asyncio.sleep(10)

    async def _check_all_vpn_health(self) -> None:
        """Check health of all registered VPN containers."""
        for vpn_name, vpn_container in self.vpn_containers.items():
            try:
                is_healthy: bool = await self._check_vpn_health(vpn_container)

                old_status: VPNStatus = vpn_container.status
                vpn_container.status = (
                    VPNStatus.HEALTHY
                    if is_healthy
                    else VPNStatus.UNHEALTHY
                )
                vpn_container.last_health_check = datetime.now(timezone.utc)

                # Handle status changes
                if old_status != vpn_container.status:
                    if vpn_container.status == VPNStatus.UNHEALTHY:
                        await self._handle_vpn_failure(vpn_name)
                    elif vpn_container.status == VPNStatus.HEALTHY:
                        await self._handle_vpn_recovery(vpn_name)

            except Exception as e:
                logger.error(
                    "Failed to check VPN health",
                    vpn=vpn_name,
                    error=f"{e.__class__.__name__}: {e}",
                )
                vpn_container.status = VPNStatus.UNKNOWN

    async def _check_vpn_health(self, vpn_container: VPNContainer) -> bool:
        """Check if a VPN container is healthy.

        This can be customized based on VPN type:
        - For Gluetun: Check control server port
        - For WARP: Check connectivity
        - For Tailscale: Check tailscale status
        """
        try:
            if vpn_container.node_id == self.node_discovery.node_id:
                # Local container - check directly
                return await self._check_local_vpn_health(vpn_container)
            else:
                # Remote container - check via node API
                return await self._check_remote_vpn_health(vpn_container)
        except Exception as e:
            logger.error(
                "VPN health check failed",
                vpn=vpn_container.name,
                node=vpn_container.node_id,
                error=f"{e.__class__.__name__}: {e}",
            )
            return False

    async def _check_local_vpn_health(self, vpn_container: VPNContainer) -> bool:
        """Check health of a local VPN container."""
        try:
            container = self.docker_client.containers.get(vpn_container.name)

            # Check if container is running
            if container.status != "running":
                return False

            # Check container health if available
            health = container.attrs.get("State", {}).get("Health", {})
            if health:
                return health.get("Status") == "healthy"

            # For containers without health checks, assume healthy if running
            return True

        except Exception as e:
            logger.debug(
                "Local VPN health check failed",
                vpn=vpn_container.name,
                error=f"{e.__class__.__name__}: {e}",
            )
            return False

    async def _check_remote_vpn_health(self, vpn_container: VPNContainer) -> bool:
        """Check health of a remote VPN container via node API."""
        try:
            node = await self.node_discovery.registry.get_node(vpn_container.node_id)
            if not node or not node.is_healthy():
                return False

            # Make HTTP request to node's constellation API
            import aiohttp

            async with aiohttp.ClientSession() as session:
                url = f"http://{node.ip_address}:{node.port}/containers/{vpn_container.name}"
                timeout = aiohttp.ClientTimeout(total=5)
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("status") == "healthy"
                    return False

        except Exception as e:
            logger.debug(
                "Remote VPN health check failed",
                vpn=vpn_container.name,
                node=vpn_container.node_id,
                error=f"{e.__class__.__name__}: {e}",
            )
            return False

    async def _handle_vpn_failure(self, vpn_name: str) -> None:
        """Handle VPN container failure."""
        vpn_container = self.vpn_containers.get(vpn_name)
        if not vpn_container:
            return

        logger.warning(
            "VPN container failed",
            vpn=vpn_name,
            node=vpn_container.node_id,
            dependents=list(vpn_container.dependent_containers),
        )

        # Find all routes that use this VPN as primary or current
        affected_routes: list[tuple[str, VPNRoute]] = []
        for container_name, route in self.vpn_routes.items():
            if route.current_vpn == vpn_name:
                affected_routes.append((container_name, route))

        # Trigger failover for affected routes
        for container_name, route in affected_routes:
            await self._trigger_vpn_failover(container_name, route)

    async def _handle_vpn_recovery(self, vpn_name: str) -> None:
        """Handle VPN container recovery."""
        vpn_container = self.vpn_containers.get(vpn_name)
        if not vpn_container:
            return

        logger.info("VPN container recovered", vpn=vpn_name, node=vpn_container.node_id)

        # Consider recovery for routes that have failed over
        for container_name, route in self.vpn_routes.items():
            if (
                route.primary_vpn == vpn_name
                and route.current_vpn != vpn_name
                and route.last_failover
            ):
                # Check if enough time has passed since failover
                if (datetime.now(timezone.utc) - route.last_failover) > self.failover_cooldown:
                    await self._consider_vpn_recovery(container_name, route)

    async def _trigger_vpn_failover(self, container_name: str, route: VPNRoute) -> None:
        """Trigger VPN failover for a dependent container."""
        next_vpn = route.get_next_fallback()

        if not next_vpn:
            logger.error(
                "No fallback VPN available",
                container=container_name,
                current_vpn=route.current_vpn,
            )
            return

        # Check if fallback VPN is healthy
        fallback_container = self.vpn_containers.get(next_vpn)
        if not fallback_container or not fallback_container.is_healthy():
            logger.warning(
                "Fallback VPN is not healthy, trying next",
                container=container_name,
                fallback=next_vpn,
            )
            route.failover_count += 1
            await self._trigger_vpn_failover(container_name, route)
            return

        logger.info(
            "Triggering VPN failover",
            container=container_name,
            from_vpn=route.current_vpn,
            to_vpn=next_vpn,
        )

        # Update route state
        route.current_vpn = next_vpn
        route.failover_count += 1
        route.last_failover = datetime.now(timezone.utc)

        # Perform the actual failover
        await self._perform_vpn_failover(container_name, next_vpn)

    async def _perform_vpn_failover(self, container_name: str, new_vpn: str) -> None:
        """
        Perform the actual VPN failover by updating container networking.

        This involves:
        1. Stopping the dependent container
        2. Updating its network configuration to use the new VPN
        3. Restarting the container
        """
        try:
            # Get the dependent container
            container = self.docker_client.containers.get(container_name)

            # Get current container configuration
            config = container.attrs["Config"]
            host_config = container.attrs["HostConfig"]

            # Update network mode to use new VPN
            host_config["NetworkMode"] = f"service:{new_vpn}"

            logger.info(
                "Updating container network mode",
                container=container_name,
                new_vpn=new_vpn,
            )

            # Stop the container
            container.stop(timeout=10)

            # Remove the container
            container.remove()

            # Recreate with new network configuration
            new_container = self.docker_client.containers.run(
                image=config["Image"],
                name=container_name,
                environment=config.get("Env", []),
                volumes=host_config.get("Binds", []),
                ports=host_config.get("PortBindings", {}),
                network_mode=f"service:{new_vpn}",
                restart_policy=host_config.get("RestartPolicy", {}),
                detach=True,
                **self._extract_additional_config(config, host_config),
            )

            logger.info(
                "Container recreated with new VPN",
                container=container_name,
                new_container_id=new_container.id[:12],
                vpn=new_vpn,
            )

        except Exception as e:
            logger.error(
                "Failed to perform VPN failover",
                container=container_name,
                new_vpn=new_vpn,
                error=f"{e.__class__.__name__}: {e}",
            )
            raise

    def _extract_additional_config(
        self,
        config: dict[str, Any],
        host_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract additional configuration parameters for container recreation."""
        additional_config: dict[str, Any] = {}

        # Add command if specified
        if config.get("Cmd"):
            additional_config["command"] = config["Cmd"]

        # Add entrypoint if specified
        if config.get("Entrypoint"):
            additional_config["entrypoint"] = config["Entrypoint"]

        # Add working directory
        if config.get("WorkingDir"):
            additional_config["working_dir"] = config["WorkingDir"]

        # Add user
        if config.get("User"):
            additional_config["user"] = config["User"]

        # Add labels
        if config.get("Labels"):
            additional_config["labels"] = config["Labels"]

        # Add capabilities
        if host_config.get("CapAdd"):
            additional_config["cap_add"] = host_config["CapAdd"]

        if host_config.get("CapDrop"):
            additional_config["cap_drop"] = host_config["CapDrop"]

        # Add privileged mode
        if host_config.get("Privileged"):
            additional_config["privileged"] = True

        return additional_config

    async def _consider_vpn_recovery(
        self, container_name: str, route: VPNRoute
    ) -> None:
        """Consider recovering to primary VPN if it's healthy."""
        primary_vpn = self.vpn_containers.get(route.primary_vpn)
        if not primary_vpn or not primary_vpn.is_healthy():
            return

        logger.info(
            "Considering VPN recovery to primary",
            container=container_name,
            primary=route.primary_vpn,
            current=route.current_vpn,
        )

        # Perform recovery
        await self._perform_vpn_failover(container_name, route.primary_vpn)
        route.reset_to_primary()

        logger.info(
            "VPN recovery completed",
            container=container_name,
            recovered_to=route.primary_vpn,
        )

    async def _sync_vpn_state(self) -> None:
        """Sync VPN state with other nodes in the cluster."""
        try:
            # Get current cluster state
            cluster_nodes: list[NodeInfo] = await self.node_discovery.get_available_nodes()

            # Prepare state data to share
            local_state: dict[str, Any] = {
                "node_id": self.node_discovery.node_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "vpn_containers": {
                    name: {
                        "status": vpn.status.value,
                        "last_health_check": vpn.last_health_check.isoformat()
                        if vpn.last_health_check
                        else None,
                        "priority": vpn.priority,
                        "dependent_containers": list(vpn.dependent_containers),
                    }
                    for name, vpn in self.vpn_containers.items()
                    if vpn.node_id == self.node_discovery.node_id
                },
                "vpn_routes": {
                    name: {
                        "current_vpn": route.current_vpn,
                        "failover_count": route.failover_count,
                        "last_failover": route.last_failover.isoformat()
                        if route.last_failover
                        else None,
                    }
                    for name, route in self.vpn_routes.items()
                },
            }

            # Send state to other nodes
            for node in cluster_nodes:
                if node.node_id != self.node_discovery.node_id:
                    await self._send_vpn_state_to_node(node, local_state)

        except Exception as e:
            logger.error("Failed to sync VPN state", error=f"{e.__class__.__name__}: {e}")

    async def _send_vpn_state_to_node(
        self,
        node: NodeInfo,
        state: dict[str, Any],
    ) -> None:
        """Send VPN state to a specific node."""
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                url = f"http://{node.ip_address}:{node.port}/vpn/sync"
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.post(url, json=state, timeout=timeout) as response:
                    if response.status == 200:
                        logger.debug("VPN state synced to node", node=node.node_id)
                    else:
                        logger.warning(
                            "Failed to sync VPN state to node",
                            node=node.node_id,
                            status=response.status,
                        )
        except Exception as e:
            logger.debug(
                "Error syncing VPN state to node",
                node=node.node_id,
                error=f"{e.__class__.__name__}: {e}",
            )

    async def handle_vpn_state_sync(self, remote_state: dict[str, Any]) -> None:
        """Handle incoming VPN state sync from another node."""
        try:
            remote_node_id = remote_state["node_id"]
            remote_vpn_containers = remote_state.get("vpn_containers", {})

            # Update remote VPN container states
            for vpn_name, vpn_data in remote_vpn_containers.items():
                if vpn_name in self.vpn_containers:
                    vpn_container = self.vpn_containers[vpn_name]
                    if vpn_container.node_id == remote_node_id:
                        # Update status from remote node
                        vpn_container.status = VPNStatus(vpn_data["status"])
                        if vpn_data["last_health_check"]:
                            vpn_container.last_health_check = datetime.fromisoformat(
                                vpn_data["last_health_check"]
                            )

            logger.debug("Processed VPN state sync", remote_node=remote_node_id)

        except Exception as e:
            logger.error("Failed to process VPN state sync", error=f"{e.__class__.__name__}: {e}")

    async def _handle_container_failure(
        self,
        container_name: str,
        **kwargs: Any,
    ) -> None:
        """Handle container failure events from the failover manager."""
        # Check if this is a VPN container
        if container_name in self.vpn_containers:
            await self._handle_vpn_failure(container_name)

    async def _handle_container_unhealthy(
        self,
        container_name: str,
        **kwargs: Any,
    ) -> None:
        """Handle container unhealthy events from the failover manager."""
        # Check if this is a VPN container
        if container_name in self.vpn_containers:
            vpn_container = self.vpn_containers[container_name]
            vpn_container.status = VPNStatus.UNHEALTHY
            logger.warning("VPN container marked unhealthy", vpn=container_name)

    def get_vpn_status(self) -> dict[str, Any]:
        """Get current VPN failover status."""
        return {
            "vpn_containers": {
                name: {
                    "node_id": vpn.node_id,
                    "type": vpn.vpn_type.value,
                    "status": vpn.status.value,
                    "priority": vpn.priority,
                    "last_health_check": vpn.last_health_check.isoformat()
                    if vpn.last_health_check
                    else None,
                    "dependent_containers": list(vpn.dependent_containers),
                }
                for name, vpn in self.vpn_containers.items()
            },
            "vpn_routes": {
                name: {
                    "primary_vpn": route.primary_vpn,
                    "current_vpn": route.current_vpn,
                    "fallback_vpns": route.fallback_vpns,
                    "failover_count": route.failover_count,
                    "last_failover": route.last_failover.isoformat()
                    if route.last_failover
                    else None,
                }
                for name, route in self.vpn_routes.items()
            },
        }
