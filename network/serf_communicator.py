"""Serf-based Node Communication for Constellation.

This module provides a production-ready node communication layer using HashiCorp Serf
for gossip protocol, cluster membership, and event handling. It integrates with the
existing peer communication system and provides enhanced reliability and features.
"""
from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiofiles
import aiohttp

logger = logging.getLogger(__name__)


class SerfEventType(Enum):
    """Types of Serf events."""
    MEMBER_JOIN = "member-join"
    MEMBER_LEAVE = "member-leave"
    MEMBER_FAILED = "member-failed"
    MEMBER_UPDATE = "member-update"
    MEMBER_REAP = "member-reap"
    USER = "user"
    QUERY = "query"


@dataclass
class SerfMember:
    """Represents a Serf cluster member."""
    name: str
    addr: str
    port: int
    tags: Dict[str, str]
    status: str
    protocol_min: int
    protocol_max: int
    protocol_cur: int
    delegate_min: int
    delegate_max: int
    delegate_cur: int


@dataclass
class SerfEvent:
    """Represents a Serf event."""
    event_type: SerfEventType
    ltime: Optional[int] = None
    name: Optional[str] = None
    payload: Optional[str] = None
    coalesce: Optional[bool] = None
    members: Optional[List[SerfMember]] = None


@dataclass
class SerfQuery:
    """Represents a Serf query."""
    id: int
    ltime: int
    name: str
    payload: str


class SerfCommunicator:
    """Handles node communication using HashiCorp Serf."""

    def __init__(
        self,
        node_name: str,
        bind_addr: str = "0.0.0.0:7946",
        rpc_addr: str = "127.0.0.1:7373",
        data_dir: Optional[str] = None,
        encrypt_key: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        self.node_name = node_name
        self.bind_addr = bind_addr
        self.rpc_addr = rpc_addr
        self.data_dir = data_dir or f"/tmp/serf-{node_name}"
        self.encrypt_key = encrypt_key
        self.tags = tags or {}
        
        # Internal state
        self.process: Optional[subprocess.Popen] = None
        self.rpc_session: Optional[aiohttp.ClientSession] = None
        self.event_handlers: Dict[SerfEventType, List[Callable]] = {}
        self.query_handlers: Dict[str, Callable] = {}
        self.is_running = False
        
        # Ensure data directory exists
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        """Start the Serf agent."""
        if self.is_running:
            logger.warning("Serf agent is already running")
            return

        logger.info(f"Starting Serf agent: {self.node_name}")
        
        # Build Serf command
        cmd = [
            "serf", "agent",
            f"-node={self.node_name}",
            f"-bind={self.bind_addr}",
            f"-rpc-addr={self.rpc_addr}",
            f"-data-dir={self.data_dir}",
        ]
        
        # Add encryption if provided
        if self.encrypt_key:
            cmd.extend([f"-encrypt={self.encrypt_key}"])
        
        # Add tags
        for key, value in self.tags.items():
            cmd.extend([f"-tag", f"{key}={value}"])
        
        # Start the process
        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait a moment for the agent to start
            await asyncio.sleep(2)
            
            # Check if process is still running
            if self.process.poll() is not None:
                stdout, stderr = self.process.communicate()
                raise RuntimeError(f"Serf agent failed to start: {stderr}")
            
            # Initialize RPC session
            self.rpc_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
            
            # Perform handshake
            await self._rpc_handshake()
            
            self.is_running = True
            logger.info(f"Serf agent started successfully: {self.node_name}")
            
        except Exception as e:
            logger.error(f"Failed to start Serf agent: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the Serf agent."""
        if not self.is_running:
            return

        logger.info(f"Stopping Serf agent: {self.node_name}")
        
        try:
            # Send leave command
            await self._rpc_request("leave", {})
        except Exception as e:
            logger.warning(f"Failed to send leave command: {e}")
        
        # Close RPC session
        if self.rpc_session:
            await self.rpc_session.close()
            self.rpc_session = None
        
        # Terminate process
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
        
        self.is_running = False
        logger.info(f"Serf agent stopped: {self.node_name}")

    async def join(self, addresses: List[str]) -> int:
        """Join existing Serf cluster."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        response = await self._rpc_request("join", {
            "Existing": addresses,
            "Replay": False
        })
        
        num_joined = response.get("Num", 0)
        logger.info(f"Joined {num_joined} nodes in cluster")
        return num_joined

    async def members(self) -> List[SerfMember]:
        """Get list of cluster members."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        response = await self._rpc_request("members", {})
        members = []
        
        for member_data in response.get("Members", []):
            # Convert IP address from list to string
            addr_list = member_data.get("Addr", [])
            addr = ".".join(map(str, addr_list)) if addr_list else ""
            
            member = SerfMember(
                name=member_data.get("Name", ""),
                addr=addr,
                port=member_data.get("Port", 0),
                tags=member_data.get("Tags", {}),
                status=member_data.get("Status", ""),
                protocol_min=member_data.get("ProtocolMin", 0),
                protocol_max=member_data.get("ProtocolMax", 0),
                protocol_cur=member_data.get("ProtocolCur", 0),
                delegate_min=member_data.get("DelegateMin", 0),
                delegate_max=member_data.get("DelegateMax", 0),
                delegate_cur=member_data.get("DelegateCur", 0),
            )
            members.append(member)
        
        return members

    async def send_event(self, name: str, payload: str = "", coalesce: bool = True) -> None:
        """Send a user event to the cluster."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        await self._rpc_request("event", {
            "Name": name,
            "Payload": payload,
            "Coalesce": coalesce
        })
        
        logger.debug(f"Sent event: {name} with payload: {payload}")

    async def send_query(
        self,
        name: str,
        payload: str = "",
        filter_nodes: Optional[List[str]] = None,
        filter_tags: Optional[Dict[str, str]] = None,
        timeout: int = 0
    ) -> List[Dict[str, Any]]:
        """Send a query to the cluster and collect responses."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        query_data = {
            "Name": name,
            "Payload": payload,
            "RequestAck": True,
            "Timeout": timeout
        }
        
        if filter_nodes:
            query_data["FilterNodes"] = filter_nodes
        
        if filter_tags:
            query_data["FilterTags"] = filter_tags
        
        # Send query and collect responses
        responses = []
        async with self._rpc_stream("query", query_data) as stream:
            async for response in stream:
                if response.get("Type") == "response":
                    responses.append(response)
                elif response.get("Type") == "done":
                    break
        
        logger.debug(f"Query {name} received {len(responses)} responses")
        return responses

    async def update_tags(self, tags: Dict[str, str], delete_tags: Optional[List[str]] = None) -> None:
        """Update node tags."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        request_data = {"Tags": tags}
        if delete_tags:
            request_data["DeleteTags"] = delete_tags
        
        await self._rpc_request("tags", request_data)
        self.tags.update(tags)
        
        if delete_tags:
            for tag in delete_tags:
                self.tags.pop(tag, None)
        
        logger.debug(f"Updated tags: {tags}")

    def register_event_handler(self, event_type: SerfEventType, handler: Callable) -> None:
        """Register an event handler."""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
        logger.debug(f"Registered handler for {event_type}")

    def register_query_handler(self, query_name: str, handler: Callable) -> None:
        """Register a query handler."""
        self.query_handlers[query_name] = handler
        logger.debug(f"Registered query handler for {query_name}")

    async def start_event_stream(self) -> None:
        """Start streaming events from Serf."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        logger.info("Starting Serf event stream")
        
        try:
            async with self._rpc_stream("stream", {"Type": "member-join,member-leave,member-failed,user:*,query:*"}) as stream:
                async for event_data in stream:
                    await self._handle_event(event_data)
        except Exception as e:
            logger.error(f"Event stream error: {e}")
            raise

    async def _handle_event(self, event_data: Dict[str, Any]) -> None:
        """Handle incoming events."""
        event_type_str = event_data.get("Event")
        if not event_type_str:
            return
        
        try:
            if event_type_str == "query":
                # Handle query
                query = SerfQuery(
                    id=event_data.get("ID"),
                    ltime=event_data.get("LTime"),
                    name=event_data.get("Name"),
                    payload=event_data.get("Payload", "")
                )
                
                # Call query handler if registered
                handler = self.query_handlers.get(query.name)
                if handler:
                    try:
                        response = await handler(query)
                        if response:
                            await self._rpc_request("respond", {
                                "ID": query.id,
                                "Payload": str(response)
                            })
                    except Exception as e:
                        logger.error(f"Query handler error: {e}")
            
            else:
                # Handle regular events
                event_type = SerfEventType(event_type_str)
                
                event = SerfEvent(
                    event_type=event_type,
                    ltime=event_data.get("LTime"),
                    name=event_data.get("Name"),
                    payload=event_data.get("Payload"),
                    coalesce=event_data.get("Coalesce")
                )
                
                # Parse members for membership events
                if "Members" in event_data:
                    members = []
                    for member_data in event_data["Members"]:
                        addr_list = member_data.get("Addr", [])
                        addr = ".".join(map(str, addr_list)) if addr_list else ""
                        
                        member = SerfMember(
                            name=member_data.get("Name", ""),
                            addr=addr,
                            port=member_data.get("Port", 0),
                            tags=member_data.get("Tags", {}),
                            status=member_data.get("Status", ""),
                            protocol_min=member_data.get("ProtocolMin", 0),
                            protocol_max=member_data.get("ProtocolMax", 0),
                            protocol_cur=member_data.get("ProtocolCur", 0),
                            delegate_min=member_data.get("DelegateMin", 0),
                            delegate_max=member_data.get("DelegateMax", 0),
                            delegate_cur=member_data.get("DelegateCur", 0),
                        )
                        members.append(member)
                    event.members = members
                
                # Call event handlers
                handlers = self.event_handlers.get(event_type, [])
                for handler in handlers:
                    try:
                        await handler(event)
                    except Exception as e:
                        logger.error(f"Event handler error: {e}")
        
        except Exception as e:
            logger.error(f"Error handling event: {e}")

    async def _rpc_handshake(self) -> None:
        """Perform RPC handshake."""
        await self._rpc_request("handshake", {"Version": 1})

    async def _rpc_request(self, command: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Send RPC request to Serf agent."""
        if not self.rpc_session:
            raise RuntimeError("RPC session not initialized")
        
        rpc_url = f"http://{self.rpc_addr}/rpc"
        
        request_data = {
            "Command": command,
            "Seq": int(time.time() * 1000)
        }
        request_data.update(data)
        
        try:
            async with self.rpc_session.post(rpc_url, json=request_data) as response:
                response.raise_for_status()
                result = await response.json()
                
                if result.get("Error"):
                    raise RuntimeError(f"Serf RPC error: {result['Error']}")
                
                return result
        
        except aiohttp.ClientError as e:
            raise RuntimeError(f"RPC request failed: {e}")

    @asynccontextmanager
    async def _rpc_stream(self, command: str, data: Dict[str, Any]):
        """Stream RPC responses from Serf agent."""
        if not self.rpc_session:
            raise RuntimeError("RPC session not initialized")
        
        rpc_url = f"http://{self.rpc_addr}/rpc"
        
        request_data = {
            "Command": command,
            "Seq": int(time.time() * 1000)
        }
        request_data.update(data)
        
        async with self.rpc_session.post(rpc_url, json=request_data) as response:
            response.raise_for_status()
            
            async for line in response.content:
                if line:
                    try:
                        yield json.loads(line.decode().strip())
                    except json.JSONDecodeError:
                        continue

    async def get_stats(self) -> Dict[str, Any]:
        """Get Serf agent statistics."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        return await self._rpc_request("stats", {})

    async def force_leave(self, node_name: str) -> None:
        """Force a node to leave the cluster."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        await self._rpc_request("force-leave", {"Node": node_name})
        logger.info(f"Forced node to leave: {node_name}")

    async def get_coordinate(self, node_name: str) -> Optional[Dict[str, Any]]:
        """Get network coordinate for a node."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        try:
            response = await self._rpc_request("get-coordinate", {"Node": node_name})
            if response.get("Ok"):
                return response.get("Coord")
            return None
        except Exception as e:
            logger.warning(f"Failed to get coordinate for {node_name}: {e}")
            return None

    async def install_key(self, key: str) -> Dict[str, Any]:
        """Install an encryption key."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        return await self._rpc_request("install-key", {"Key": key})

    async def use_key(self, key: str) -> Dict[str, Any]:
        """Set primary encryption key."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        return await self._rpc_request("use-key", {"Key": key})

    async def remove_key(self, key: str) -> Dict[str, Any]:
        """Remove an encryption key."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        return await self._rpc_request("remove-key", {"Key": key})

    async def list_keys(self) -> Dict[str, Any]:
        """List all encryption keys."""
        if not self.is_running:
            raise RuntimeError("Serf agent is not running")
        
        return await self._rpc_request("list-keys", {})


class ConstellationSerfIntegration:
    """Integration layer between Constellation and Serf."""
    
    def __init__(self, serf_communicator: SerfCommunicator):
        self.serf = serf_communicator
        self.container_status_cache: Dict[str, str] = {}
        
    async def start(self) -> None:
        """Start the integration."""
        # Register event handlers
        self.serf.register_event_handler(SerfEventType.MEMBER_JOIN, self._handle_member_join)
        self.serf.register_event_handler(SerfEventType.MEMBER_LEAVE, self._handle_member_leave)
        self.serf.register_event_handler(SerfEventType.MEMBER_FAILED, self._handle_member_failed)
        self.serf.register_event_handler(SerfEventType.USER, self._handle_user_event)
        
        # Register query handlers
        self.serf.register_query_handler("container-status", self._handle_container_status_query)
        self.serf.register_query_handler("health-check", self._handle_health_check_query)
        self.serf.register_query_handler("load-info", self._handle_load_info_query)
        
        # Start Serf agent
        await self.serf.start()
        
        # Start event stream
        asyncio.create_task(self.serf.start_event_stream())
        
    async def stop(self) -> None:
        """Stop the integration."""
        await self.serf.stop()
    
    async def broadcast_container_status(self, container_id: str, status: str) -> None:
        """Broadcast container status change."""
        self.container_status_cache[container_id] = status
        await self.serf.send_event("container-status", json.dumps({
            "container_id": container_id,
            "status": status,
            "timestamp": time.time()
        }))
    
    async def request_failover(self, container_id: str, target_node: Optional[str] = None) -> List[str]:
        """Request container failover."""
        query_payload = json.dumps({
            "container_id": container_id,
            "source_node": self.serf.node_name,
            "target_node": target_node
        })
        
        responses = await self.serf.send_query("failover-request", query_payload)
        available_nodes = []
        
        for response in responses:
            try:
                data = json.loads(response.get("Payload", "{}"))
                if data.get("available", False):
                    available_nodes.append(response.get("From"))
            except json.JSONDecodeError:
                continue
        
        return available_nodes
    
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive cluster status."""
        members = await self.serf.members()
        stats = await self.serf.get_stats()
        
        cluster_info = {
            "members": [asdict(member) for member in members],
            "total_members": len(members),
            "alive_members": len([m for m in members if m.status == "alive"]),
            "failed_members": len([m for m in members if m.status == "failed"]),
            "stats": stats,
            "node_name": self.serf.node_name
        }
        
        return cluster_info
    
    async def _handle_member_join(self, event: SerfEvent) -> None:
        """Handle member join events."""
        if event.members:
            for member in event.members:
                logger.info(f"Node joined cluster: {member.name} ({member.addr}:{member.port})")
                # Update peer tracking or trigger rebalancing
    
    async def _handle_member_leave(self, event: SerfEvent) -> None:
        """Handle member leave events."""
        if event.members:
            for member in event.members:
                logger.info(f"Node left cluster: {member.name}")
                # Trigger failover for containers on this node
    
    async def _handle_member_failed(self, event: SerfEvent) -> None:
        """Handle member failure events."""
        if event.members:
            for member in event.members:
                logger.warning(f"Node failed: {member.name}")
                # Trigger immediate failover
    
    async def _handle_user_event(self, event: SerfEvent) -> None:
        """Handle user events."""
        if event.name == "container-status":
            try:
                data = json.loads(event.payload or "{}")
                container_id = data.get("container_id")
                status = data.get("status")
                logger.debug(f"Container status update: {container_id} -> {status}")
            except json.JSONDecodeError:
                logger.warning(f"Invalid container status event payload: {event.payload}")
    
    async def _handle_container_status_query(self, query: SerfQuery) -> str:
        """Handle container status queries."""
        try:
            data = json.loads(query.payload)
            container_id = data.get("container_id")
            
            if container_id in self.container_status_cache:
                return json.dumps({
                    "container_id": container_id,
                    "status": self.container_status_cache[container_id],
                    "node": self.serf.node_name
                })
            else:
                return json.dumps({"error": "Container not found"})
        except json.JSONDecodeError:
            return json.dumps({"error": "Invalid query payload"})
    
    async def _handle_health_check_query(self, query: SerfQuery) -> str:
        """Handle health check queries."""
        return json.dumps({
            "status": "healthy",
            "node": self.serf.node_name,
            "timestamp": time.time(),
            "containers": len(self.container_status_cache)
        })
    
    async def _handle_load_info_query(self, query: SerfQuery) -> str:
        """Handle load information queries."""
        # This would integrate with your existing load calculation
        return json.dumps({
            "load_score": 0.5,  # Placeholder
            "cpu_usage": 0.3,   # Placeholder
            "memory_usage": 0.4, # Placeholder
            "container_count": len(self.container_status_cache),
            "node": self.serf.node_name
        }) 