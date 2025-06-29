"""Peer-to-Peer Communication for Constellation Nodes.

This module implements a gossip protocol for distributed coordination
without any master/manager nodes. All nodes are equal peers.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time

from contextlib import suppress
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Callable, ClassVar

import aiohttp

from aiohttp import web

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages exchanged between peers."""

    HEARTBEAT = "heartbeat"
    CONTAINER_STATUS = "container_status"
    FAILOVER_REQUEST = "failover_request"
    FAILOVER_ACK = "failover_ack"
    NODE_JOIN = "node_join"
    NODE_LEAVE = "node_leave"
    IMMEDIATE_ACTION = "immediate_action"
    CONSENSUS_PROPOSAL = "consensus_proposal"
    CONSENSUS_VOTE = "consensus_vote"
    NODE_EVICTION = "node_eviction"
    CLUSTER_STATUS = "cluster_status"
    CONTAINER_SPEC_REQUEST = "container_spec_request"
    CONTAINER_SPEC_RESPONSE = "container_spec_response"
    CONTAINER_REMOVE = "container_remove"
    CONTAINER_RESTART = "container_restart"
    CONTAINER_STOP = "container_stop"
    CONTAINER_START = "container_start"
    CONTAINER_HEALTH_CHECK = "container_health_check"
    CONTAINER_HEALTH_CHECK_RESPONSE = "container_health_check_response"
    CONTAINER_HEALTH_CHECK_REQUEST = "container_health_check_request"
    CONTAINER_DEPLOY = "container_deploy"
    CONTAINER_DEPLOY_RESPONSE = "container_deploy_response"
    CONTAINER_DEPLOY_REQUEST = "container_deploy_request"

@dataclass
class PeerMessage:
    """Message structure for peer communication."""

    message_type: MessageType
    sender_id: str
    timestamp: float
    data: dict[str, Any]
    message_id: str | None = None

    def __post_init__(self):
        if self.message_id is None:
            self.message_id = f"{self.sender_id}_{int(self.timestamp * 1000)}"


@dataclass
class NodeState:
    """Current state of a peer node."""

    node_id: str
    ip_address: str
    port: int
    last_seen: float
    containers: dict[str, str]  # container_id -> status
    load_score: float = 0.0
    is_healthy: bool = True


class PeerCommunicator:
    """Handles peer-to-peer communication using gossip protocol."""

    PSUTIL_WARNING_SHOWN: ClassVar[bool] = False

    def __init__(
        self,
        node_id: str,
        listen_port: int = 8080,
        gossip_interval: float = 5.0,
        node_timeout: float = 30.0,
    ):
        self.node_id: str = node_id
        self.listen_port: int = listen_port
        self.gossip_interval: float = gossip_interval
        self.node_timeout: float = node_timeout

        # Peer state tracking
        self.peers: dict[str, NodeState] = {}
        self.my_containers: dict[str, str] = {}
        self.message_handlers: dict[MessageType, Callable] = {}
        self.seen_messages: set[str] = set()

        # Communication
        self.session: aiohttp.ClientSession | None = None
        self.app: web.Application | None = None
        self.server: web.TCPSite | None = None

        # Register default handlers
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register default message handlers."""
        self.message_handlers[MessageType.HEARTBEAT] = self._handle_heartbeat
        self.message_handlers[MessageType.CONTAINER_STATUS] = self._handle_container_status
        self.message_handlers[MessageType.NODE_JOIN] = self._handle_node_join
        self.message_handlers[MessageType.NODE_LEAVE] = self._handle_node_leave
        self.message_handlers[MessageType.IMMEDIATE_ACTION] = self._handle_immediate_action

    async def start(self):
        """Start the peer communicator."""
        logger.info(f"Starting peer communicator on port {self.listen_port}")

        # Start HTTP session
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5.0))

        # Start HTTP server for receiving messages
        self.app = web.Application()
        self.app.router.add_post("/peer/message", self._handle_incoming_message)
        self.app.router.add_get("/peer/status", self._handle_status_request)
        self.app.router.add_post("/peer/action", self._handle_action_request)

        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.listen_port)  # noqa: S104
        await site.start()

        # Start gossip loop
        asyncio.create_task(self._gossip_loop())  # noqa: RUF006
        asyncio.create_task(self._cleanup_loop())  # noqa: RUF006

        logger.info(f"Peer communicator started on {self.listen_port}")

    async def stop(self):
        """Stop the peer communicator."""
        if self.session:
            await self.session.close()

        # Send leave message to peers
        await self._broadcast_message(MessageType.NODE_LEAVE, {})

    def register_handler(self, message_type: MessageType, handler: Callable):
        """Register a custom message handler."""
        self.message_handlers[message_type] = handler

    async def add_peer(
        self,
        ip_address: str,
        port: int | None = None,
    ):
        """Add a new peer to track."""
        if port is None:
            port = self.listen_port

        peer_id = f"{ip_address}:{port}"
        if peer_id not in self.peers:
            self.peers[peer_id] = NodeState(
                node_id=peer_id,
                ip_address=ip_address,
                port=port,
                last_seen=time.time(),
                containers={},
            )
            logger.info(f"Added peer: {peer_id}")

    async def send_immediate_action(
        self,
        target_peer: str,
        action: str,
        params: dict[str, Any],
    ):
        """Send immediate action request to specific peer."""
        message = PeerMessage(
            message_type=MessageType.IMMEDIATE_ACTION,
            sender_id=self.node_id,
            timestamp=time.time(),
            data={"action": action, "params": params, "target": target_peer},
        )

        if target_peer in self.peers:
            peer = self.peers[target_peer]
            await self._send_direct_message(peer.ip_address, peer.port, message)
        else:
            logger.warning(f"Unknown peer for immediate action: {target_peer}")

    async def broadcast_container_status(self, container_id: str, status: str):
        """Broadcast container status change to all peers."""
        self.my_containers[container_id] = status
        await self._broadcast_message(
            MessageType.CONTAINER_STATUS,
            {"container_id": container_id, "status": status},
        )

    def get_healthy_peers(self) -> list[NodeState]:
        """Get list of currently healthy peers."""
        current_time = time.time()
        return [
            peer
            for peer in self.peers.values()
            if peer.is_healthy and (current_time - peer.last_seen) < self.node_timeout
        ]

    def get_peer_with_lowest_load(self) -> NodeState | None:
        """Get the peer with the lowest load score."""
        healthy_peers = self.get_healthy_peers()
        if not healthy_peers:
            return None
        return min(healthy_peers, key=lambda p: p.load_score)

    async def _gossip_loop(self):
        """Main gossip loop - periodically exchange state with peers."""
        while True:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.gossip_interval)
            except Exception:  # noqa: BLE001, PERF203
                logger.exception("Error in gossip loop")
                await asyncio.sleep(self.gossip_interval)

    async def _cleanup_loop(self):
        """Clean up old messages and dead peers."""
        while True:
            try:
                current_time = time.time()

                # Remove old seen messages (keep last hour)
                old_messages = {
                    msg_id
                    for msg_id in self.seen_messages
                    if current_time - float(msg_id.split("_")[-1]) / 1000 > 3600  # noqa: PLR2004
                }
                self.seen_messages -= old_messages

                # Mark peers as unhealthy if not seen recently
                for peer in self.peers.values():
                    if current_time - peer.last_seen > self.node_timeout:
                        peer.is_healthy = False

                await asyncio.sleep(60)  # Cleanup every minute
            except Exception:  # noqa: BLE001, PERF203
                logger.exception("Error in cleanup loop")
                await asyncio.sleep(60)

    async def _send_heartbeats(self):
        """Send heartbeat to all known peers."""
        message = PeerMessage(
            message_type=MessageType.HEARTBEAT,
            sender_id=self.node_id,
            timestamp=time.time(),
            data={
                "containers": self.my_containers,
                "load_score": await self._calculate_load_score(),
            },
        )

        # Send to random subset of peers (gossip protocol)
        healthy_peers: list[NodeState] = self.get_healthy_peers()
        if len(healthy_peers) <= 3:  # noqa: PLR2004
            # Send to all if few peers
            targets: list[NodeState] = healthy_peers
        else:
            # Send to random 3 peers
            targets = random.sample(healthy_peers, 3)

        for peer in targets:
            try:
                await self._send_direct_message(peer.ip_address, peer.port, message)
            except Exception:  # noqa: BLE001, PERF203
                logger.exception(f"Failed to send heartbeat to {peer.node_id}")
                peer.is_healthy = False

    async def _send_direct_message(self, ip: str, port: int, message: PeerMessage):
        """Send message directly to a specific peer."""
        url = f"http://{ip}:{port}/peer/message"

        try:
            async with self.session.post(url, json=asdict(message)) as response:
                if response.status != 200:  # noqa: PLR2004
                    logger.warning(
                        f"Peer {ip}:{port} returned status {response.status}"
                    )
        except Exception:  # noqa: BLE001, PERF203
            logger.exception(f"Failed to send message to {ip}:{port}")
            raise

    async def _broadcast_message(
        self,
        message_type: MessageType,
        data: dict[str, Any],
    ):
        """Broadcast message to all peers."""
        message = PeerMessage(
            message_type=message_type,
            sender_id=self.node_id,
            timestamp=time.time(),
            data=data,
        )

        # Send to all healthy peers
        for peer in self.get_healthy_peers():
            with suppress(Exception):
                await self._send_direct_message(peer.ip_address, peer.port, message)

    async def _handle_incoming_message(self, request):
        """Handle incoming peer message via HTTP."""
        try:
            data = await request.json()
            message = PeerMessage(**data)

            if message.message_id and message.message_id.strip():
                # Prevent message loops
                if message.message_id in self.seen_messages:
                    return web.Response(status=200)

                self.seen_messages.add(message.message_id)
            else:
                logger.warning("Message ID is None")

            # Update peer state
            if message.sender_id.strip() and message.sender_id in self.peers:
                self.peers[message.sender_id].last_seen = time.time()
                self.peers[message.sender_id].is_healthy = True

            # Handle message
            handler = self.message_handlers.get(message.message_type)
            if handler:
                await handler(message)
            else:
                logger.warning(f"No handler for message type: {message.message_type}")

            return web.Response(status=200)

        except Exception:
            logger.exception("Error handling incoming message")
            return web.Response(status=500)

    async def _handle_status_request(self, request):
        """Handle status request from other nodes."""
        status: dict[str, Any] = {
            "node_id": self.node_id,
            "containers": self.my_containers,
            "load_score": await self._calculate_load_score(),
            "peer_count": len(self.get_healthy_peers()),
            "timestamp": time.time(),
        }
        return web.json_response(status)

    async def _handle_action_request(self, request):
        """Handle immediate action requests."""
        try:
            data = await request.json()
            action = data.get("action")
            params = data.get("params", {})

            # Handle the action immediately
            result = await self._execute_action(action, params)

            return web.json_response({"success": True, "result": result})

        except Exception as e:
            logger.exception("Error handling action request")
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def _handle_heartbeat(self, message: PeerMessage):
        """Handle heartbeat message."""
        sender_id = message.sender_id
        data = message.data

        # Update or create peer state
        if sender_id not in self.peers:
            # Extract IP from sender_id (assuming format ip:port)
            ip = sender_id.split(":")[0]
            port = int(sender_id.split(":")[1])
            await self.add_peer(ip, port)

        peer = self.peers[sender_id]
        peer.last_seen = time.time()
        peer.is_healthy = True
        peer.containers = data.get("containers", {})
        peer.load_score = data.get("load_score", 0.0)

    async def _handle_container_status(self, message: PeerMessage):
        """Handle container status update."""
        sender_id: str = message.sender_id
        data: dict[str, Any] = message.data

        if (
            sender_id
            and sender_id.strip()
            and sender_id.strip() in self.peers
        ):
            container_id: str | None = data.get("container_id")
            status: str | None = data.get("status")
            if container_id and container_id.strip() and status and status.strip():
                self.peers[sender_id].containers[container_id] = status
            else:
                logger.warning("Container ID or status is None")

            logger.info(f"Container {container_id} on {sender_id} is now {status}")

    async def _handle_node_join(self, message: PeerMessage):
        """Handle node join notification."""
        # Node discovery will be handled by heartbeats
        logger.info(f"Node {message.sender_id} joined the cluster")

    async def _handle_node_leave(self, message: PeerMessage):
        """Handle node leave notification."""
        sender_id = message.sender_id
        if sender_id in self.peers:
            self.peers[sender_id].is_healthy = False
            logger.info(f"Node {sender_id} left the cluster")

    async def _handle_immediate_action(self, message: PeerMessage):
        """Handle immediate action request."""
        data: dict[str, Any] = message.data
        action: str | None = data.get("action")
        params: dict[str, Any] = data.get("params", {})
        target: str | None = data.get("target")

        if target and target.strip() == (self.node_id or self.node_id.strip()):
            if action:
                await self._execute_action(action, params)
            else:
                logger.warning("Action is None")
        else:
            logger.warning(f"Target is not {self.node_id}: {target}")

    async def _execute_action(
        self,
        action: str,
        params: dict[str, Any],
    ) -> Any:
        """Execute an immediate action."""
        logger.info(f"Executing immediate action: {action} with params: {params}")

        # This would be implemented based on your specific actions
        # For example: start_container, stop_container, failover_container, etc.

        if action == "health_check":
            return {"status": "healthy", "timestamp": time.time()}
        if action == "start_container":
            container_id = params.get("container_id")
            # Implementation would go here
            return {"container_id": container_id, "status": "started"}
        raise ValueError(f"Unknown action: {action}")

    async def _calculate_load_score(self) -> float:
        """Calculate current node load score."""
        # Simple load calculation - you can make this more sophisticated
        container_count: int = len(self.my_containers)

        # Add CPU/memory metrics if available
        try:
            import psutil  # pyright: ignore[reportMissingModuleSource]  # noqa: PLC0415

            cpu_percent: float = psutil.cpu_percent(interval=0.1)
            memory_percent: float = psutil.virtual_memory().percent
            load_score: float = (
                (container_count * 10)
                + (cpu_percent / 10)
                + (memory_percent / 10)
            )
        except ImportError:
            # Fallback to just container count
            load_score: float = container_count * 10
            if not self.PSUTIL_WARNING_SHOWN:
                logger.warning("pip package `psutil` is not installed! This makes it impossible to calculate resource consumption and scale accordingly. Please restart Constellation after installing psutil with `pip install psutil`")  # noqa: E501
                self.__class__.PSUTIL_WARNING_SHOWN = True

        return load_score
