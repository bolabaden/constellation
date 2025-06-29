"""
Constellation Discovery Module

Provides node discovery, health monitoring, and network topology management.
"""

from .tailscale_client import TailscaleClient, TailscalePeer
from .node_discovery import (
    NodeDiscoveryService,
    NodeRegistry,
    NodeInfo,
    NodeStatus,
    NodeEvent,
    DiscoveryConfig,
)

__all__ = [
    "TailscaleClient",
    "TailscalePeer",
    "NodeDiscoveryService",
    "NodeRegistry",
    "NodeInfo",
    "NodeStatus",
    "NodeEvent",
    "DiscoveryConfig",
]
