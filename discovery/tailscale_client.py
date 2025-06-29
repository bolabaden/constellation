"""
Tailscale Client for Constellation Node Discovery

Integrates with Tailscale network for automatic peer discovery and status monitoring.
"""

from __future__ import annotations

import asyncio
import json

from dataclasses import dataclass, field
from datetime import datetime
from ipaddress import AddressValueError, IPv4Address
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TailscalePeer:
    """Represents a Tailscale peer/node."""

    id: str
    hostname: str
    ip_address: str
    online: bool
    last_seen: datetime | None = None
    os: str | None = None
    tailscale_version: str | None = None
    tags: list[str] = field(default_factory=list)


class TailscaleClient:
    """Client for interacting with Tailscale network."""

    def __init__(self, timeout: int = 10) -> None:
        """
        Initialize Tailscale client.

        Args:
            timeout: Timeout for tailscale commands in seconds
        """
        self.timeout: int = timeout
        self._last_status: dict[str, Any] | None = None
        self._last_status_time: datetime | None = None

    async def get_status(self, force_refresh: bool = False) -> dict[str, Any] | None:
        """
        Get Tailscale network status.

        Args:
            force_refresh: Force refresh even if cached data is recent

        Returns:
            Tailscale status dictionary or None if unavailable
        """
        # Use cached status if recent (within 30 seconds) and not forcing refresh
        if (
            not force_refresh
            and self._last_status
            and self._last_status_time
            and (datetime.utcnow() - self._last_status_time).total_seconds() < 30
        ):
            return self._last_status

        try:
            # Run tailscale status command
            process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                "tailscale",
                "status",
                "--json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout,
            )

            if process.returncode != 0:
                logger.error(
                    "Tailscale status command failed",
                    returncode=process.returncode,
                    stderr=stderr.decode(),
                )
                return None

            status: dict[str, Any] = json.loads(stdout.decode())
            self._last_status = status
            self._last_status_time = datetime.utcnow()

            logger.info(
                "Retrieved Tailscale status",
                peers=len(status.get("Peer", {})),
                status=status.get("Self", {}),
            )
            return status

        except asyncio.TimeoutError:
            logger.error("Tailscale status command timed out")
            return None
        except FileNotFoundError:
            logger.error("Tailscale command not found - is Tailscale installed?")
            return None
        except json.JSONDecodeError as e:
            logger.error(
                "Failed to parse Tailscale status JSON",
                error=f"{e.__class__.__name__}: {e}",
                stdout=stdout.decode(),
                stderr=stderr.decode(),
                status=status,
            )
            return None
        except Exception as e:
            logger.error(
                "Unexpected error getting Tailscale status",
                error=f"{e.__class__.__name__}: {e}",
            )
            return None

    async def get_peers(self) -> list[TailscalePeer]:
        """Get list of Tailscale peers.

        Returns:
            List of TailscalePeer objects
        """
        status = await self.get_status()
        if not status:
            return []

        peers = []
        peer_data: dict[str, Any] = status.get("Peer", {})

        for peer_id, peer_info in peer_data.items():
            try:
                # Parse last seen time if available
                last_seen: datetime | None = None
                if "LastSeen" in peer_info:
                    try:
                        last_seen = datetime.fromisoformat(
                            peer_info["LastSeen"].replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError) as e:
                        logger.debug(
                            "Failed to parse last seen time",
                            peer_id=peer_id,
                            error=f"{e.__class__.__name__}: {e}",
                        )

                # Get primary IP address
                tailscale_ips: list[str] = peer_info.get("TailscaleIPs", [])
                ip_address: str = tailscale_ips[0] if tailscale_ips else ""

                peer = TailscalePeer(
                    id=peer_id,
                    hostname=peer_info.get("HostName", ""),
                    ip_address=ip_address,
                    online=peer_info.get("Online", False),
                    last_seen=last_seen,
                    os=peer_info.get("OS", ""),
                    tailscale_version=peer_info.get("TailscaleVersion", ""),
                    tags=peer_info.get("Tags", []),
                )

                peers.append(peer)

            except Exception as e:
                logger.warning(
                    "Failed to parse peer info",
                    peer_id=peer_id,
                    error=f"{e.__class__.__name__}: {e}",
                )
                continue

        logger.debug("Retrieved Tailscale peers", count=len(peers))
        return peers

    async def get_self_info(self) -> TailscalePeer | None:
        """
        Get information about this Tailscale node.

        Returns:
            TailscalePeer object for this node or None
        """
        status = await self.get_status()
        if not status:
            return None

        self_info: dict[str, Any] | None = status.get("Self")
        if not self_info:
            return None

        try:
            # Parse last seen time if available
            last_seen: datetime | None = None
            if "LastSeen" in self_info:
                try:
                    last_seen = datetime.fromisoformat(
                        self_info["LastSeen"].replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError):
                    pass

            # Get primary IP address
            tailscale_ips: list[str] = self_info.get("TailscaleIPs", [])
            ip_address: str = tailscale_ips[0] if tailscale_ips else ""

            return TailscalePeer(
                id=self_info.get("ID", ""),
                hostname=self_info.get("HostName", ""),
                ip_address=ip_address,
                online=self_info.get("Online", True),  # Self is assumed online
                last_seen=last_seen,
                os=self_info.get("OS", ""),
                tailscale_version=self_info.get("TailscaleVersion", ""),
                tags=self_info.get("Tags", []),
            )

        except Exception as e:
            logger.error(
                "Failed to parse self info", error=f"{e.__class__.__name__}: {e}"
            )
            return None

    async def ping_peer(self, peer_ip: str) -> bool:
        """Ping a Tailscale peer to check connectivity.

        Args:
            peer_ip: IP address of the peer to ping

        Returns:
            True if ping successful, False otherwise
        """
        try:
            # Validate IP address
            _: IPv4Address = IPv4Address(peer_ip)
        except AddressValueError as e:
            logger.error("Invalid IP address for ping", ip=peer_ip, error=str(e))
            return False

        try:
            process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                "tailscale",
                "ping",
                peer_ip,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout,
            )

            success: bool = process.returncode == 0
            if success:
                logger.debug("Ping successful", peer_ip=peer_ip)
            else:
                logger.error(
                    "Ping failed",
                    peer_ip=peer_ip,
                    stderr=stderr.decode(),
                    error=f"{stderr.decode()} (process.returncode: {process.returncode})",
                )

            return success

        except asyncio.TimeoutError:
            logger.warning("Ping timeout", peer_ip=peer_ip)
            return False
        except Exception as e:
            logger.error(
                "Ping error", peer_ip=peer_ip, error=f"{e.__class__.__name__}: {e}"
            )
            return False

    async def is_tailscale_available(self) -> bool:
        """
        Check if Tailscale is available and running.

        Returns:
            True if Tailscale is available, False otherwise
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "tailscale",
                "version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            await asyncio.wait_for(process.communicate(), timeout=5)
            return process.returncode == 0

        except (asyncio.TimeoutError, FileNotFoundError, Exception):
            return False

    async def get_network_map(self) -> dict[str, Any]:
        """Get a comprehensive network map of Tailscale peers.

        Returns:
            Dictionary containing network topology information
        """
        status = await self.get_status()
        if not status:
            return {}

        peers = await self.get_peers()
        self_info = await self.get_self_info()

        network_map = {
            "self": {
                "id": self_info.id if self_info else "",
                "hostname": self_info.hostname if self_info else "",
                "ip": self_info.ip_address if self_info else "",
                "online": True,
            },
            "peers": {},
            "online_count": 0,
            "total_count": len(peers),
            "last_updated": datetime.utcnow().isoformat(),
        }

        for peer in peers:
            network_map["peers"][peer.id] = {
                "hostname": peer.hostname,
                "ip": peer.ip_address,
                "online": peer.online,
                "last_seen": peer.last_seen.isoformat() if peer.last_seen else None,
                "os": peer.os,
                "version": peer.tailscale_version,
                "tags": peer.tags,
            }

            if peer.online:
                network_map["online_count"] += 1

        return network_map

    async def find_constellation_nodes(self, service_port: int = 5443) -> list[str]:
        """Find Tailscale peers that are running Constellation service.

        Args:
            service_port: Port that Constellation service listens on

        Returns:
            List of IP addresses of Constellation nodes
        """
        peers: list[TailscalePeer] = await self.get_peers()
        constellation_nodes: list[str] = []

        # Check each online peer for Constellation service
        for peer in peers:
            if not peer.online or not peer.ip_address:
                continue

            try:
                # Try to connect to the Constellation service port
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(peer.ip_address, service_port),
                    timeout=3,
                )
                writer.close()
                await writer.wait_closed()

                constellation_nodes.append(peer.ip_address)
                logger.debug(
                    "Found Constellation node",
                    ip=peer.ip_address,
                    hostname=peer.hostname,
                )

            except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
                logger.error(
                    "Timeout checking Constellation service",
                    ip=peer.ip_address,
                    error=f"{e.__class__.__name__}: {str(e)}",
                )
                # Service not available on this peer
                continue
            except Exception as e:
                logger.error(
                    "Error checking Constellation service",
                    ip=peer.ip_address,
                    error=f"{e.__class__.__name__}: {e}",
                )
                continue

        logger.info(
            "Found Constellation nodes",
            count=len(constellation_nodes),
            nodes=constellation_nodes,
        )
        return constellation_nodes
