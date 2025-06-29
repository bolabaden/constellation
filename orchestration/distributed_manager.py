"""Distributed Container Management for Constellation.

This module provides fully distributed container orchestration without
any single points of failure. All nodes are equal peers using consistent hashing
for deterministic container placement - no voting required.
"""
from __future__ import annotations

import asyncio
import hashlib
import socket
import time

from dataclasses import asdict, dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

import structlog

from constellation.network.peer_communication import (
    MessageType,
    PeerCommunicator,
    PeerMessage,
)

if TYPE_CHECKING:
    from constellation.network.peer_communication import (
        NodeState,
    )

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


# Simple abstractions for distributed orchestration
class ContainerStatus(str, Enum):
    """Container status enumeration."""
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    RESTARTING = "restarting"
    REMOVED = "removed"


@dataclass
class ContainerSpec:
    """Container specification for deployment."""
    name: str
    image: str
    ports: dict[str, str] | None = None
    environment: dict[str, str] | None = None
    volumes: list[str] | None = None
    command: str | list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContainerSpec:
        """Create from dictionary."""
        return cls(**data)

@dataclass
class Container:
    """Container instance."""
    id: str
    name: str
    status: ContainerStatus
    node_id: str
    spec: ContainerSpec

@dataclass
class Service:
    """Service containing multiple containers."""
    name: str
    containers: list[ContainerSpec]

class DockerStackManager:
    """Simple Docker stack manager interface."""

    def __init__(self):
        pass

    async def start_container(self, spec: ContainerSpec) -> Container | None:
        """Start a container from spec."""
        # This would integrate with actual Docker API
        logger.info(f"Starting container {spec.name} with image {spec.image}")
        return Container(
            id=f"{spec.name}_{int(time.time())}",
            name=spec.name,
            status=ContainerStatus.RUNNING,
            node_id="local",
            spec=spec
        )

    async def stop_container(self, container_id: str) -> bool:
        """Stop a container."""
        logger.info(f"Stopping container {container_id}")
        return True

    async def check_container_health(self, container_id: str) -> bool:
        """Check if container is healthy."""
        # This would integrate with actual Docker health checks
        logger.debug(f"Checking health of container {container_id}")
        return True  # Assume healthy for now

    async def restart_container(self, container_id: str) -> bool:
        """Restart a container."""
        logger.info(f"Restarting container {container_id}")
        return True

class ConsistentHashRing:
    """Consistent hash ring for deterministic container placement."""

    def __init__(self, replicas: int = 150):
        self.replicas: int = replicas
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []

    def _hash(self, key: str) -> int:
        """Hash function for the ring."""
        # Use SHA-256 instead of MD5 for better security
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str):
        """Add a node to the ring."""
        for i in range(self.replicas):
            key: int = self._hash(f"{node_id}:{i}")
            self.ring[key] = node_id
        self._update_sorted_keys()

    def remove_node(self, node_id: str):
        """Remove a node from the ring."""
        for i in range(self.replicas):
            key: int = self._hash(f"{node_id}:{i}")
            if key in self.ring:
                del self.ring[key]
        self._update_sorted_keys()

    def _update_sorted_keys(self):
        """Update sorted keys for efficient lookup."""
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> str | None:
        """Get the node responsible for a key."""
        if not self.ring:
            return None

        hash_key: int = self._hash(key)

        # Find the first node clockwise from the hash
        for ring_key in self.sorted_keys:
            if hash_key <= ring_key:
                return self.ring[ring_key]

        # Wrap around to the first node
        return self.ring[self.sorted_keys[0]]

    def get_nodes(self, key: str, count: int = 1) -> list[str]:
        """Get multiple nodes for replication."""
        if not self.ring or count <= 0:
            return []

        hash_key: int = self._hash(key)
        nodes: list[str] = []
        seen: set[str] = set()

        # Find starting position
        start_idx: int = 0
        for i, ring_key in enumerate(self.sorted_keys):
            if hash_key <= ring_key:
                start_idx = i
                break

        # Collect unique nodes
        idx: int = start_idx
        while len(nodes) < count and len(seen) < len(set(self.ring.values())):
            node: str = self.ring[self.sorted_keys[idx % len(self.sorted_keys)]]
            if node not in seen:
                nodes.append(node)
                seen.add(node)
            idx += 1

        return nodes


class DistributedContainerManager:
    """Manages containers across a distributed cluster using consistent hashing."""

    def __init__(
        self,
        node_id: str | None = None,
        listen_port: int = 8080,
        docker_stack_manager: DockerStackManager | None = None,
        replication_factor: int = 2,
    ):
        # Generate node ID from hostname/IP if not provided
        if node_id is None:
            hostname: str = socket.gethostname()
            node_id = f"{hostname}_{int(time.time())}"

        self.node_id: str = node_id
        self.listen_port: int = listen_port
        self.docker_stack: DockerStackManager = docker_stack_manager or DockerStackManager()
        self.replication_factor: int = replication_factor

        # Peer communication
        self.peer_communicator: PeerCommunicator = PeerCommunicator(
            node_id=node_id,
            listen_port=listen_port,
            gossip_interval=5.0,
            node_timeout=30.0,
        )

        # Consistent hash ring for container placement
        self.hash_ring: ConsistentHashRing = ConsistentHashRing()
        self.hash_ring.add_node(self.node_id)

        # Local state
        self.managed_containers: dict[str, Container] = {}
        self.is_running: bool = False

        # Register message handlers
        self._register_message_handlers()

    def _register_message_handlers(self):
        """Register handlers for peer messages."""
        self.peer_communicator.register_handler(
            MessageType.FAILOVER_REQUEST,
            self._handle_failover_request,
        )

    async def start(self):
        """Start the distributed container manager."""
        logger.info(f"Starting distributed container manager on node {self.node_id}")

        # Start peer communication
        await self.peer_communicator.start()

        # Start monitoring tasks
        self.monitor_containers_task: asyncio.Task[None] = asyncio.create_task(self._monitor_containers())
        self.health_check_loop_task: asyncio.Task[None] = asyncio.create_task(self._health_check_loop())
        self.sync_cluster_state_task: asyncio.Task[None] = asyncio.create_task(self._sync_cluster_state())

        self.is_running = True
        logger.info("Distributed container manager started")

    async def stop(self):
        """Stop the distributed container manager."""
        logger.info("Stopping distributed container manager")
        self.is_running = False

        # Stop peer communication
        await self.peer_communicator.stop()

    async def add_peer(self, ip_address: str, port: int | None = None):
        """Add a peer node to the cluster."""
        await self.peer_communicator.add_peer(ip_address, port)

    async def deploy_container(self, container_spec: ContainerSpec) -> bool:
        """Deploy a container using consistent hashing."""
        logger.info(f"Deploying container {container_spec.name}")

        # Determine which nodes should handle this container
        target_nodes: list[str] = self.hash_ring.get_nodes(
            container_spec.name,
            self.replication_factor,
        )

        if not target_nodes:
            logger.error("No nodes available for container placement")
            return False

        # If we're one of the target nodes, deploy locally
        if self.node_id and self.node_id.strip() in target_nodes:
            await self._deploy_local_container(container_spec)

        # Send deployment requests to other target nodes
        for target_node in target_nodes:
            if target_node != self.node_id:
                self.peer_comm_task = await self.peer_communicator._send_direct_message(  # noqa: SLF001
                    target_node,
                    self.listen_port,
                    PeerMessage(
                        message_type=MessageType.CONTAINER_DEPLOY,
                        sender_id=self.node_id,
                        timestamp=time.time(),
                        data={"container_spec": container_spec.to_dict()},
                    ),
                )

        logger.info(f"Container {container_spec.name} deployment initiated")
        return True

    async def deploy_service(self, service: Service) -> bool:
        """Deploy a service (multiple containers)."""
        logger.info(f"Deploying service {service.name}")

        # Deploy each container in the service
        results: list[bool] = []
        for container_spec in service.containers:
            result: bool = await self.deploy_container(container_spec)
            results.append(result)

        success: bool = all(results)
        if success:
            logger.info(f"Service {service.name} deployed successfully")
        else:
            logger.error(f"Service {service.name} deployment failed")

        return success

    async def remove_container(self, container_id: str) -> bool:
        """Remove a container from the cluster."""
        logger.info(f"Removing container {container_id}")

        # Determine which nodes should have this container
        target_nodes = self.hash_ring.get_nodes(container_id, self.replication_factor)

        # Remove locally if we should have it
        if self.node_id in target_nodes and container_id in self.managed_containers:
            await self._remove_local_container(container_id)

        # Send removal requests to other nodes
        for target_node in target_nodes:
            if target_node != self.node_id:
                self.peer_comm_task = await self.peer_communicator._send_direct_message(  # noqa: SLF001
                    target_node,
                    self.listen_port,
                    PeerMessage(
                        message_type=MessageType.CONTAINER_REMOVE,
                        sender_id=self.node_id,
                        timestamp=time.time(),
                        data={"container_id": container_id},
                    ),
                )

        return True

    async def failover_container(self, container_id: str, failed_node: str) -> bool:
        """Handle container failover by recalculating placement."""
        logger.info(f"Handling failover for container {container_id} from {failed_node}")

        # Remove failed node from hash ring temporarily
        self.hash_ring.remove_node(failed_node)

        # Recalculate placement
        new_target_nodes = self.hash_ring.get_nodes(container_id, self.replication_factor)

        # Add failed node back (it might come back)
        self.hash_ring.add_node(failed_node)

        # If we're now responsible, deploy the container
        if self.node_id in new_target_nodes:
            # We need the container spec - this would come from cluster state
            # For now, we'll request it from peers
            self.peer_comm_task = await self.peer_communicator._broadcast_message(  # noqa: SLF001
                MessageType.CONTAINER_SPEC_REQUEST,
                {"container_id": container_id, "requesting_node": self.node_id}
            )

        return True

    async def _deploy_local_container(self, container_spec: ContainerSpec):
        """Deploy a container locally on this node."""
        try:
            # Use the docker stack manager to deploy
            container: Container = Container(
                id=f"{container_spec.name}_{int(time.time())}",
                name=container_spec.name,
                status=ContainerStatus.RUNNING,
                node_id=self.node_id,
                spec=container_spec
            )

            # Start the container
            await self.docker_stack.start_container(container_spec)
            self.managed_containers[container_spec.name] = container

            # Broadcast status
            await self.peer_communicator.broadcast_container_status(container_spec.name, ContainerStatus.RUNNING)

            logger.info(f"Successfully deployed container {container_spec.name}")

        except Exception:  # noqa: BLE001, PERF203
            logger.exception(f"Failed to deploy container {container_spec.name}")
            await self.peer_communicator.broadcast_container_status(container_spec.name, ContainerStatus.FAILED)

    async def _remove_local_container(self, container_id: str) -> bool:
        """Remove a container from this node."""
        try:
            if container_id in self.managed_containers:
                _container = self.managed_containers[container_id]
                await self.docker_stack.stop_container(container_id)
                del self.managed_containers[container_id]

                # Broadcast removal
                await self.peer_communicator.broadcast_container_status(container_id, ContainerStatus.REMOVED)

                logger.info(f"Successfully removed container {container_id}")
                return True

        except Exception:  # noqa: BLE001
            logger.exception(f"Failed to remove container {container_id}")

        return False

    async def _monitor_containers(self):
        """Monitor local containers and handle failures."""
        while self.is_running:
            try:
                for container_id, _container in list(self.managed_containers.items()):
                    # Check container health
                    is_healthy = await self.docker_stack.check_container_health(container_id)

                    if not is_healthy:
                        logger.warning(f"Container {container_id} is unhealthy")

                        # Broadcast failure status
                        await self.peer_communicator.broadcast_container_status(container_id, ContainerStatus.FAILED)

                        # Try to restart locally first
                        try:
                            await self.docker_stack.restart_container(container_id)
                            await self.peer_communicator.broadcast_container_status(container_id, ContainerStatus.RUNNING)
                            logger.info(f"Successfully restarted container {container_id}")
                        except Exception:  # noqa: BLE001, PERF203
                            logger.exception(f"Failed to restart container {container_id}")
                            # Remove failed container
                            del self.managed_containers[container_id]

                await asyncio.sleep(10)  # Check every 10 seconds

            except Exception:  # noqa: BLE001, PERF203
                logger.exception("Error in container monitoring")
                await asyncio.sleep(10)

    async def _health_check_loop(self):
        """Periodically check cluster health and trigger actions."""
        while self.is_running:
            try:
                current_time: float = time.time()
                dead_nodes: list[str] = []

                # Check for dead nodes
                for peer_id, peer in self.peer_communicator.peers.items():
                    if current_time - peer.last_seen > 60 and peer.is_healthy:  # Only trigger once  # noqa: PLR2004
                        logger.warning(f"Node {peer_id} appears to be dead")
                        dead_nodes.append(peer_id)
                        peer.is_healthy = False

                # Propose eviction for dead nodes
                for dead_node in dead_nodes:
                    await self.peer_communicator._broadcast_message(  # noqa: SLF001
                        MessageType.NODE_EVICTION,
                        {"node_id": dead_node}
                    )

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception:  # noqa: BLE001, PERF203
                logger.exception("Error in health check loop")
                await asyncio.sleep(30)

    async def _handle_container_deploy(self, message: PeerMessage):
        """Handle container deployment request from another node."""
        data: dict[str, Any] = message.data
        container_spec_dict: dict[str, Any] = data["container_spec"]
        container_spec: ContainerSpec = ContainerSpec.from_dict(container_spec_dict)

        # Deploy the container
        await self.deploy_container(container_spec)

    async def _handle_container_remove(self, message: PeerMessage):
        """Handle container removal request from another node."""
        data: dict[str, Any] = message.data
        container_id: str = data["container_id"]

        # Remove the container
        await self.remove_container(container_id)

    async def _handle_failover_request(self, message: PeerMessage):
        """Handle failover request from another node."""
        data: dict[str, Any] = message.data
        container_id: str | None = data.get("container_id")
        failed_node: str | None = data.get("failed_node")

        if (
            not container_id
            or not container_id.strip()
            or not failed_node
            or not failed_node.strip()
        ):
            logger.warning("Invalid failover request")
            return

        logger.info(f"Received failover request for {container_id} from {failed_node}")
        await self.failover_container(container_id, failed_node)

    async def _sync_cluster_state(self):
        """Periodically sync cluster state with peers."""
        while self.is_running:
            try:
                # Get cluster status from peers
                cluster_status = await self.get_cluster_status()

                # Broadcast cluster status to peers
                self.peer_comm_task = await self.peer_communicator._broadcast_message(  # noqa: SLF001
                    MessageType.CLUSTER_STATUS,
                    cluster_status
                )

                await asyncio.sleep(30)  # Sync every 30 seconds

            except Exception:  # noqa: BLE001, PERF203
                logger.exception("Error in cluster state sync")
                await asyncio.sleep(30)

    async def get_cluster_status(self) -> dict[str, Any]:
        """Get status of the entire cluster."""
        healthy_peers: list[NodeState] = self.peer_communicator.get_healthy_peers()

        cluster_status: dict[str, Any] = {
            "node_id": self.node_id,
            "healthy_nodes": len(healthy_peers) + 1,  # +1 for ourselves
            "total_containers": len(self.managed_containers),
            "peers": {},
        }

        # Add peer information
        for peer in healthy_peers:
            cluster_status["peers"][peer.node_id] = {
                "ip_address": peer.ip_address,
                "port": peer.port,
                "containers": len(peer.containers),
                "load_score": peer.load_score,
                "last_seen": peer.last_seen,
            }

        # Add our own containers
        for container_id, container in self.managed_containers.items():
            await self.peer_communicator.broadcast_container_status(
                container_id,
                container.status,
            )

        return cluster_status
