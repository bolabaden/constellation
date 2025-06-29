"""
Distributed Container Orchestration for Constellation

Provides distributed container management, placement strategies, and cross-node coordination.
"""

from constellation.orchestration.distributed_manager import DistributedContainerManager
from constellation.orchestration.placement import NodeSelector, PlacementStrategy

__all__ = [
    "DistributedContainerManager",
    "PlacementStrategy",
    "NodeSelector",
]
