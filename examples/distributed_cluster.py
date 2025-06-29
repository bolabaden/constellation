"""Example of setting up a distributed Constellation cluster.

This example shows how to create a fully distributed cluster where
all nodes are equal peers with no single points of failure.
"""

from __future__ import annotations

import asyncio
import logging
import os

from constellation.core.container import ContainerSpec
from constellation.core.service import Service
from constellation.orchestration.distributed_manager import DistributedContainerManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ConstellationCluster:
    """Example distributed cluster setup."""

    def __init__(self, node_id: str, listen_port: int = 8080):
        self.node_id = node_id
        self.listen_port = listen_port
        self.manager = DistributedContainerManager(
            node_id=node_id, listen_port=listen_port
        )

    async def start(self):
        """Start this node in the cluster."""
        logger.info(f"Starting Constellation node {self.node_id}")
        await self.manager.start()

    async def join_cluster(self, peer_ips: list[str]):
        """Join an existing cluster by connecting to peer nodes."""
        logger.info(f"Joining cluster with peers: {peer_ips}")

        for peer_ip in peer_ips:
            try:
                await self.manager.add_peer(peer_ip, self.listen_port)
                logger.info(f"Connected to peer {peer_ip}")
            except Exception as e:
                logger.warning(f"Failed to connect to peer {peer_ip}: {e}")

    async def deploy_example_service(self):
        """Deploy an example web service across the cluster."""
        logger.info("Deploying example web service")

        # Define a simple web service
        web_container = ContainerSpec(
            name="nginx-web",
            image="nginx:alpine",
            ports={"80": "8080"},
            environment={"NGINX_HOST": "localhost"},
            restart_policy="unless-stopped",
        )

        # Define a Redis cache
        redis_container = ContainerSpec(
            name="redis-cache",
            image="redis:alpine",
            ports={"6379": "6379"},
            restart_policy="unless-stopped",
        )

        # Create service
        web_service = Service(
            name="web-stack",
            containers=[web_container, redis_container],
        )

        # Deploy the service
        success: bool = await self.manager.deploy_service(web_service)
        if success:
            logger.info("Web service deployed successfully!")
        else:
            logger.error("Failed to deploy web service")

        return success

    async def deploy_media_stack(self):
        """Deploy a media server stack similar to the original compose file."""
        logger.info("Deploying media stack")

        # Plex media server
        plex_container = ContainerSpec(
            name="plex",
            image="plexinc/pms-docker:latest",
            ports={"32400": "32400"},
            environment={"PLEX_CLAIM": os.getenv("PLEX_CLAIM", ""), "TZ": "UTC"},
            volumes={"/data/plex/config": "/config", "/data/media": "/media:ro"},
            restart_policy="unless-stopped",
        )

        # Sonarr for TV shows
        sonarr_container = ContainerSpec(
            name="sonarr",
            image="linuxserver/sonarr:latest",
            ports={"8989": "8989"},
            environment={"PUID": "1000", "PGID": "1000", "TZ": "UTC"},
            volumes={
                "/data/sonarr/config": "/config",
                "/data/media/tv": "/tv",
                "/data/downloads": "/downloads",
            },
            restart_policy="unless-stopped",
        )

        # Radarr for movies
        radarr_container = ContainerSpec(
            name="radarr",
            image="linuxserver/radarr:latest",
            ports={"7878": "7878"},
            environment={"PUID": "1000", "PGID": "1000", "TZ": "UTC"},
            volumes={
                "/data/radarr/config": "/config",
                "/data/media/movies": "/movies",
                "/data/downloads": "/downloads",
            },
            restart_policy="unless-stopped",
        )

        # Create media service
        media_service = Service(
            name="media-stack",
            containers=[plex_container, sonarr_container, radarr_container],
        )

        # Deploy the service
        success = await self.manager.deploy_service(media_service)
        if success:
            logger.info("Media stack deployed successfully!")
        else:
            logger.error("Failed to deploy media stack")

        return success

    async def get_cluster_status(self):
        """Get and display cluster status."""
        status = await self.manager.get_cluster_status()

        logger.info("=== Cluster Status ===")
        logger.info(f"Node ID: {status['node_id']}")
        logger.info(f"Healthy Nodes: {status['healthy_nodes']}")
        logger.info(f"Local Containers: {status['total_containers']}")

        logger.info("=== Peer Nodes ===")
        for peer_id, peer_info in status["peers"].items():
            logger.info(
                f"  {peer_id}: {peer_info['containers']} containers, "
                f"load={peer_info['load_score']:.1f}"
            )

        return status

    async def simulate_node_failure(self, container_id: str):
        """Simulate a node failure by stopping a container."""
        logger.info(f"Simulating failure of container {container_id}")

        # This would normally be handled automatically by the health checks
        # but we can trigger it manually for testing
        await self.manager.failover_container(container_id, "failed_node")

    async def stop(self):
        """Stop this node gracefully."""
        logger.info(f"Stopping Constellation node {self.node_id}")
        await self.manager.stop()


async def run_cluster_node(node_id: str, port: int, peer_ips: list[str] | None = None):
    """Run a single cluster node."""
    cluster = ConstellationCluster(node_id, port)

    try:
        # Start the node
        await cluster.start()

        # Join existing cluster if peers provided
        if peer_ips:
            await cluster.join_cluster(peer_ips)

            # Wait a bit for cluster to stabilize
            await asyncio.sleep(5)

        # Show initial cluster status
        await cluster.get_cluster_status()

        # If this is the first node (no peers), deploy some services
        if not peer_ips:
            logger.info("First node - deploying initial services")
            await cluster.deploy_example_service()
            await asyncio.sleep(2)
            await cluster.deploy_media_stack()

        # Keep the node running and periodically show status
        while True:
            await asyncio.sleep(30)
            await cluster.get_cluster_status()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await cluster.stop()


async def run_multi_node_example():
    """Example of running multiple nodes in the same process for testing."""
    logger.info("Starting multi-node cluster example")

    # Create multiple nodes
    nodes = []

    # First node (bootstrap)
    node1 = ConstellationCluster("node-1", 8080)
    await node1.start()
    nodes.append(node1)

    # Wait a bit
    await asyncio.sleep(2)

    # Second node joins the first
    node2 = ConstellationCluster("node-2", 8081)
    await node2.start()
    await node2.join_cluster(["127.0.0.1"])  # Connect to node1
    nodes.append(node2)

    # Wait a bit
    await asyncio.sleep(2)

    # Third node joins the cluster
    node3 = ConstellationCluster("node-3", 8082)
    await node3.start()
    await node3.join_cluster(["127.0.0.1", "127.0.0.1"])  # Connect to both
    nodes.append(node3)

    # Wait for cluster to stabilize
    await asyncio.sleep(5)

    # Deploy services from the first node
    logger.info("Deploying services from node-1")
    await node1.deploy_example_service()

    # Wait and show status from all nodes
    await asyncio.sleep(5)

    for i, node in enumerate(nodes, 1):
        logger.info(f"\n=== Status from Node {i} ===")
        await node.get_cluster_status()

    # Simulate running for a while
    try:
        await asyncio.sleep(60)  # Run for 1 minute
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")

    # Cleanup
    for node in nodes:
        await node.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Run single node
        node_id: str = sys.argv[1]
        port: int = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
        peer_ips: list[str] | None = sys.argv[3:] if len(sys.argv) > 3 else None

        asyncio.run(run_cluster_node(node_id, port, peer_ips))
    else:
        # Run multi-node example
        asyncio.run(run_multi_node_example())
