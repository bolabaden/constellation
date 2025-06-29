"""
Node Discovery Service for Constellation

Provides automatic node discovery using Tailscale network integration,
health monitoring, and distributed node registry management.
"""

import asyncio
import uuid

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

import structlog

from constellation.discovery.tailscale_client import TailscaleClient

if TYPE_CHECKING:
    from constellation.discovery.tailscale_client import TailscalePeer

logger = structlog.get_logger(__name__)


class NodeStatus(str, Enum):
    """Node status enumeration."""

    ONLINE = "online"
    OFFLINE = "offline"
    UNHEALTHY = "unhealthy"
    JOINING = "joining"
    LEAVING = "leaving"


class NodeEvent(str, Enum):
    """Node discovery event types."""

    NODE_DISCOVERED = "node_discovered"
    NODE_JOINED = "node_joined"
    NODE_LEFT = "node_left"
    NODE_HEALTH_CHANGED = "node_health_changed"
    NODE_UPDATED = "node_updated"
    TOPOLOGY_CHANGED = "topology_changed"


@dataclass
class NodeInfo:
    """Information about a discovered node."""

    node_id: str
    hostname: str
    ip_address: str
    port: int = 5443
    status: NodeStatus = NodeStatus.OFFLINE
    last_seen: datetime | None = None
    last_health_check: datetime | None = None
    health_check_failures: int = 0
    capabilities: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)
    tailscale_peer_id: str | None = None

    def is_healthy(self, health_timeout: timedelta = timedelta(minutes=2)) -> bool:
        """Check if node is considered healthy."""
        if self.status != NodeStatus.ONLINE:
            return False

        if not self.last_health_check:
            return False

        return datetime.now(UTC) - self.last_health_check < health_timeout

    def mark_unhealthy(self) -> None:
        """Mark node as unhealthy."""
        self.health_check_failures += 1
        if self.health_check_failures >= 3:
            self.status = NodeStatus.UNHEALTHY

    def mark_healthy(self) -> None:
        """Mark node as healthy."""
        self.health_check_failures = 0
        self.status = NodeStatus.ONLINE
        self.last_health_check = datetime.now(UTC)


@dataclass
class DiscoveryConfig:
    """Configuration for node discovery service."""

    # Service configuration
    service_port: int = 5443
    bind_address: str = "0.0.0.0"

    # Discovery intervals
    discovery_interval: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    health_check_interval: timedelta = field(
        default_factory=lambda: timedelta(seconds=15)
    )

    # Health check configuration
    health_check_timeout: timedelta = field(
        default_factory=lambda: timedelta(seconds=5)
    )
    max_health_check_failures: int = 3
    node_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))

    # Tailscale configuration
    tailscale_enabled: bool = True
    tailscale_timeout: int = 10

    # Node capabilities
    node_capabilities: set[str] = field(
        default_factory=lambda: {"container_host", "constellation"}
    )


class NodeRegistry:
    """Registry for maintaining discovered nodes."""

    def __init__(self):
        self.nodes: dict[str, NodeInfo] = {}
        self._lock = asyncio.Lock()

    async def register_node(self, node: NodeInfo) -> None:
        """Register a new node."""
        async with self._lock:
            self.nodes[node.node_id] = node
            logger.info("Node registered", node_id=node.node_id, hostname=node.hostname)

    async def unregister_node(self, node_id: str) -> NodeInfo | None:
        """Unregister a node."""
        async with self._lock:
            node = self.nodes.pop(node_id, None)
            if node:
                logger.info("Node unregistered", node_id=node_id)
            return node

    async def update_node(self, node_id: str, **updates) -> NodeInfo | None:
        """Update node information."""
        async with self._lock:
            node = self.nodes.get(node_id)
            if node:
                for key, value in updates.items():
                    if hasattr(node, key):
                        setattr(node, key, value)
                logger.debug("Node updated", node_id=node_id, updates=updates)
            return node

    async def get_node(self, node_id: str) -> NodeInfo | None:
        """Get node information."""
        async with self._lock:
            return self.nodes.get(node_id)

    async def get_all_nodes(self) -> dict[str, NodeInfo]:
        """Get all registered nodes."""
        async with self._lock:
            return self.nodes.copy()

    async def get_healthy_nodes(self) -> list[NodeInfo]:
        """Get list of healthy nodes."""
        async with self._lock:
            return [node for node in self.nodes.values() if node.is_healthy()]

    async def get_nodes_by_status(self, status: NodeStatus) -> list[NodeInfo]:
        """Get nodes by status."""
        async with self._lock:
            return [node for node in self.nodes.values() if node.status == status]

    async def cleanup_stale_nodes(self, timeout: timedelta) -> list[str]:
        """Remove nodes that haven't been seen recently."""
        cutoff_time: datetime = datetime.now(UTC) - timeout
        stale_nodes: list[str] = []

        async with self._lock:
            for node_id, node in list(self.nodes.items()):
                if node.last_seen and node.last_seen < cutoff_time:
                    self.nodes.pop(node_id)
                    stale_nodes.append(node_id)
                    logger.info("Removed stale node", node_id=node_id)

        return stale_nodes


class NodeDiscoveryService:
    """Main node discovery service."""

    def __init__(self, config: DiscoveryConfig | None = None):
        """Initialize node discovery service.

        Args:
            config: Discovery configuration (uses defaults if None)
        """
        self.config = config or DiscoveryConfig()
        self.tailscale_client = TailscaleClient(timeout=self.config.tailscale_timeout)
        self.registry = NodeRegistry()

        # Service state
        self.node_id = str(uuid.uuid4())
        self.self_info: NodeInfo | None = None
        self._running: bool = False

        # Background tasks
        self._discovery_task: asyncio.Task | None = None
        self._health_check_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None

        # Event handlers
        self.event_handlers: dict[NodeEvent, list[Callable]] = {}

        logger.info("Node discovery service initialized", node_id=self.node_id)

    async def start(self) -> None:
        """Start the node discovery service."""
        if self._running:
            logger.warning("Node discovery service already running")
            return

        logger.info("Starting node discovery service")

        # Initialize self information
        await self._initialize_self_info()

        # Start background tasks
        self._running = True
        self._discovery_task = asyncio.create_task(self._discovery_loop())
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        logger.info("Node discovery service started")

    async def stop(self) -> None:
        """Stop the node discovery service."""
        if not self._running:
            return

        logger.info("Stopping node discovery service")
        self._running = False

        # Cancel background tasks
        for task in [self._discovery_task, self._health_check_task, self._cleanup_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        logger.info("Node discovery service stopped")

    async def _initialize_self_info(self) -> None:
        """Initialize information about this node."""
        try:
            # Get Tailscale self information
            tailscale_self: TailscalePeer | None = await self.tailscale_client.get_self_info()

            hostname: str = tailscale_self.hostname if tailscale_self else "unknown"
            ip_address: str = tailscale_self.ip_address if tailscale_self else "127.0.0.1"

            self.self_info = NodeInfo(
                node_id=self.node_id,
                hostname=hostname,
                ip_address=ip_address,
                port=self.config.service_port,
                status=NodeStatus.ONLINE,
                last_seen=datetime.now(UTC),
                capabilities=self.config.node_capabilities,
                tailscale_peer_id=tailscale_self.id if tailscale_self else None,
            )

            # Register ourselves
            await self.registry.register_node(self.self_info)

            logger.info(
                "Self node initialized",
                hostname=hostname,
                ip=ip_address,
                capabilities=list(self.config.node_capabilities),
            )

        except Exception as e:
            logger.error("Failed to initialize self info", error=f"{e.__class__.__name__}: {e}")
            # Create minimal self info
            self.self_info = NodeInfo(
                node_id=self.node_id,
                hostname="unknown",
                ip_address="127.0.0.1",
                port=self.config.service_port,
                status=NodeStatus.ONLINE,
                capabilities=self.config.node_capabilities,
            )

    async def _discovery_loop(self) -> None:
        """Main discovery loop."""
        while self._running:
            try:
                await self._discover_nodes()
                await asyncio.sleep(self.config.discovery_interval.total_seconds())
            except Exception as e:
                logger.error("Discovery loop error", error=f"{e.__class__.__name__}: {e}")
                await asyncio.sleep(30)  # Wait longer on error

    async def _health_check_loop(self) -> None:
        """Health check loop."""
        while self._running:
            try:
                await self._check_node_health()
                await asyncio.sleep(self.config.health_check_interval.total_seconds())
            except Exception as e:
                logger.error("Health check loop error", error=f"{e.__class__.__name__}: {e}")
                await asyncio.sleep(30)

    async def _cleanup_loop(self) -> None:
        """Cleanup loop for stale nodes."""
        while self._running:
            try:
                stale_nodes = await self.registry.cleanup_stale_nodes(
                    self.config.node_timeout
                )
                if stale_nodes:
                    await self._emit_event(
                        NodeEvent.TOPOLOGY_CHANGED, removed_nodes=stale_nodes
                    )

                await asyncio.sleep(60)  # Cleanup every minute
            except Exception as e:
                logger.error("Cleanup loop error", error=f"{e.__class__.__name__}: {e}")
                await asyncio.sleep(60)

    async def _discover_nodes(self) -> None:
        """Discover nodes on the Tailscale network."""
        if not self.config.tailscale_enabled:
            return

        try:
            # Find Constellation nodes via Tailscale
            constellation_ips = await self.tailscale_client.find_constellation_nodes(
                self.config.service_port
            )

            # Get Tailscale peer information for context
            peers = await self.tailscale_client.get_peers()
            peer_map = {peer.ip_address: peer for peer in peers}

            discovered_nodes = []

            for ip in constellation_ips:
                # Skip self
                if self.self_info and ip == self.self_info.ip_address:
                    continue

                # Check if we already know about this node
                existing_node = None
                for node in (await self.registry.get_all_nodes()).values():
                    if node.ip_address == ip:
                        existing_node = node
                        break

                if existing_node:
                    # Update existing node
                    await self.registry.update_node(
                        existing_node.node_id,
                        last_seen=datetime.now(UTC),
                        status=NodeStatus.ONLINE,
                    )
                else:
                    # Create new node
                    peer = peer_map.get(ip)
                    node_id = str(uuid.uuid4())

                    new_node = NodeInfo(
                        node_id=node_id,
                        hostname=peer.hostname
                        if peer
                        else f"node-{ip.replace('.', '-')}",
                        ip_address=ip,
                        port=self.config.service_port,
                        status=NodeStatus.ONLINE,
                        last_seen=datetime.now(UTC),
                        capabilities={
                            "constellation"
                        },  # Will be updated via health check
                        tailscale_peer_id=peer.id if peer else None,
                    )

                    await self.registry.register_node(new_node)
                    discovered_nodes.append(new_node)

                    await self._emit_event(NodeEvent.NODE_DISCOVERED, node=new_node)

            if discovered_nodes:
                logger.info("Discovered new nodes", count=len(discovered_nodes))
                await self._emit_event(
                    NodeEvent.TOPOLOGY_CHANGED,
                    added_nodes=[n.node_id for n in discovered_nodes],
                )

        except Exception as e:
            logger.error("Node discovery failed", error=f"{e.__class__.__name__}: {e}")

    async def _check_node_health(self) -> None:
        """Check health of all known nodes."""
        nodes = await self.registry.get_all_nodes()

        for node in nodes.values():
            # Skip self
            if node.node_id == self.node_id:
                continue

            try:
                # Simple connectivity check
                is_healthy = await self._ping_node(node.ip_address, node.port)

                if is_healthy:
                    if node.status != NodeStatus.ONLINE:
                        await self.registry.update_node(
                            node.node_id, status=NodeStatus.ONLINE
                        )
                        await self._emit_event(
                            NodeEvent.NODE_HEALTH_CHANGED,
                            node_id=node.node_id,
                            healthy=True,
                        )

                    node.mark_healthy()
                else:
                    node.mark_unhealthy()

                    if node.status == NodeStatus.UNHEALTHY:
                        await self.registry.update_node(
                            node.node_id, status=NodeStatus.UNHEALTHY
                        )
                        await self._emit_event(
                            NodeEvent.NODE_HEALTH_CHANGED,
                            node_id=node.node_id,
                            healthy=False,
                        )

            except Exception as e:
                logger.error(
                    "Health check failed for node",
                    node_id=node.node_id,
                    error=f"{e.__class__.__name__}: {e}",
                )
                node.mark_unhealthy()

    async def _ping_node(self, ip_address: str, port: int) -> bool:
        """Ping a node to check if it's responsive."""
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(ip_address, port),
                timeout=self.config.health_check_timeout.total_seconds(),
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return False

    def add_event_handler(self, event: NodeEvent, handler: Callable) -> None:
        """Add an event handler."""
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        self.event_handlers[event].append(handler)

    async def _emit_event(self, event: NodeEvent, **kwargs) -> None:
        """Emit an event to registered handlers."""
        if event in self.event_handlers:
            for handler in self.event_handlers[event]:
                try:
                    await handler(event, **kwargs)
                except Exception as e:
                    logger.error(
                        "Event handler failed",
                        event=event,
                        error=f"{e.__class__.__name__}: {e}",
                    )

    async def get_cluster_status(self) -> dict[str, Any]:
        """Get comprehensive cluster status."""
        nodes = await self.registry.get_all_nodes()
        healthy_nodes = await self.registry.get_healthy_nodes()

        return {
            "cluster_id": self.node_id,  # Use self as cluster identifier
            "total_nodes": len(nodes),
            "healthy_nodes": len(healthy_nodes),
            "self_node_id": self.node_id,
            "nodes": {
                node_id: {
                    "hostname": node.hostname,
                    "ip_address": node.ip_address,
                    "status": node.status,
                    "last_seen": node.last_seen.isoformat() if node.last_seen else None,
                    "is_healthy": node.is_healthy(),
                    "capabilities": list(node.capabilities),
                    "metadata": node.metadata,
                }
                for node_id, node in nodes.items()
            },
            "last_updated": datetime.now(UTC).isoformat(),
        }

    async def get_available_nodes(
        self, capability: str | None = None
    ) -> list[NodeInfo]:
        """Get list of available nodes, optionally filtered by capability."""
        healthy_nodes = await self.registry.get_healthy_nodes()

        if capability and capability.strip():
            return [
                node
                for node in healthy_nodes
                if capability.strip() in node.capabilities
            ]

        return healthy_nodes

    async def find_best_node(
        self,
        criteria: dict[str, Any],
    ) -> NodeInfo | None:
        """Find the best node based on given criteria."""
        available_nodes = await self.get_available_nodes()

        if not available_nodes:
            return None

        # Simple selection - could be enhanced with load balancing, etc.
        # For now, just return the first healthy node
        return available_nodes[0]
