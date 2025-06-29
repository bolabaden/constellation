"""Container Placement Strategies for Constellation.

Provides intelligent container placement across nodes based on various strategies
and node selection criteria for optimal resource utilization and availability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from constellation.discovery.node_discovery import NodeInfo

logger = structlog.get_logger(__name__)


class PlacementStrategy(str, Enum):
    """Container placement strategies."""

    SPREAD = "spread"  # Distribute containers across different nodes
    PACK = "pack"  # Pack containers onto same nodes
    RANDOM = "random"  # Random placement
    AFFINITY = "affinity"  # Place based on node affinity rules
    RESOURCE = "resource"  # Place based on resource availability


class NodeAffinityType(str, Enum):
    """Node affinity types."""

    REQUIRED = "required"  # Hard requirement
    PREFERRED = "preferred"  # Soft preference


@dataclass
class NodeAffinity:
    """Node affinity rule for container placement."""

    type: NodeAffinityType
    key: str
    operator: str  # "In", "NotIn", "Exists", "DoesNotExist"
    values: list[str] = field(default_factory=list)
    weight: int = 1  # For preferred affinity (1-100)


@dataclass
class ResourceRequirement:
    """Resource requirements for container placement."""

    cpu_cores: float | None = None
    memory_mb: int | None = None
    disk_gb: int | None = None
    gpu_count: int | None = None
    network_bandwidth_mbps: int | None = None


@dataclass
class NodeSelector:
    """Node selection criteria for container placement."""

    # Label-based selection
    node_labels: dict[str, str] = field(default_factory=dict)

    # Capability requirements
    required_capabilities: set[str] = field(default_factory=set)

    # Resource requirements
    resource_requirements: ResourceRequirement | None = None

    # Affinity rules
    node_affinity: list[NodeAffinity] = field(default_factory=list)

    # Geographic constraints
    allowed_zones: set[str] = field(default_factory=set)
    allowed_regions: set[str] = field(default_factory=set)

    # Node exclusions
    excluded_nodes: set[str] = field(default_factory=set)

    def matches(
        self,
        node: NodeInfo,
    ) -> bool:
        """Check if a node matches the selection criteria."""

        # Check excluded nodes
        if node.node_id in self.excluded_nodes:
            return False

        # Check required capabilities
        node_capabilities: set[str] = getattr(node, "capabilities", set())
        if not self.required_capabilities.issubset(node_capabilities):
            return False

        # Check node labels
        node_labels: dict[str, str] = getattr(node, "labels", {})
        for key, value in self.node_labels.items():
            if node_labels.get(key) != value:
                return False

        # Check resource requirements
        if self.resource_requirements:
            if not self._check_resource_requirements(node):
                return False

        # Check affinity rules
        if not self._check_affinity_rules(node):
            return False

        # Check geographic constraints
        if self.allowed_zones:
            node_zone = getattr(node, "zone", None)
            if node_zone not in self.allowed_zones:
                return False

        if self.allowed_regions:
            node_region = getattr(node, "region", None)
            if node_region not in self.allowed_regions:
                return False

        return True

    def _check_resource_requirements(self, node: NodeInfo) -> bool:
        """Check if node meets resource requirements."""
        if not self.resource_requirements:
            return True

        # Get node resources (these would come from node metadata)
        node_resources: dict[str, Any] = getattr(node, "resources", {})

        # Check CPU
        if self.resource_requirements.cpu_cores:
            available_cpu = node_resources.get("available_cpu_cores", 0)
            if available_cpu < self.resource_requirements.cpu_cores:
                return False

        # Check memory
        if self.resource_requirements.memory_mb:
            available_memory = node_resources.get("available_memory_mb", 0)
            if available_memory < self.resource_requirements.memory_mb:
                return False

        # Check disk
        if self.resource_requirements.disk_gb:
            available_disk = node_resources.get("available_disk_gb", 0)
            if available_disk < self.resource_requirements.disk_gb:
                return False

        # Check GPU
        if self.resource_requirements.gpu_count:
            available_gpu = node_resources.get("available_gpu_count", 0)
            if available_gpu < self.resource_requirements.gpu_count:
                return False

        return True

    def _check_affinity_rules(self, node: NodeInfo) -> bool:
        """Check if node satisfies affinity rules."""
        node_labels: dict[str, str] = getattr(node, "labels", {})

        for affinity in self.node_affinity:
            if affinity.type == NodeAffinityType.REQUIRED:
                if not self._evaluate_affinity_rule(affinity, node_labels):
                    return False

        return True

    def _evaluate_affinity_rule(
        self,
        affinity: NodeAffinity,
        node_labels: dict[str, str],
    ) -> bool:
        """Evaluate a single affinity rule against node labels."""

        if affinity.operator == "In":
            return node_labels.get(affinity.key) in affinity.values

        elif affinity.operator == "NotIn":
            return node_labels.get(affinity.key) not in affinity.values

        elif affinity.operator == "Exists":
            return affinity.key in node_labels

        elif affinity.operator == "DoesNotExist":
            return affinity.key not in node_labels

        else:
            logger.warning("Unknown affinity operator", operator=affinity.operator)
            return False


class PlacementEngine:
    """Engine for determining optimal container placement."""

    def __init__(self):
        """Initialize placement engine."""
        self.placement_history: list[dict[str, Any]] = []

    def select_nodes(
        self,
        available_nodes: list[NodeInfo],
        strategy: PlacementStrategy,
        count: int,
        node_selector: NodeSelector | None = None,
        existing_placements: dict[str, list[str]] | None = None,
    ) -> list[NodeInfo]:
        """Select nodes for container placement.

        Args:
            available_nodes: List of available nodes
            strategy: Placement strategy to use
            count: Number of nodes to select
            node_selector: Node selection criteria
            existing_placements: Current container placements by node

        Returns:
            List of selected nodes
        """

        # Filter nodes based on selector
        eligible_nodes: list[NodeInfo] = available_nodes
        if node_selector:
            eligible_nodes = [
                node for node in available_nodes if node_selector.matches(node)
            ]

        if not eligible_nodes:
            logger.warning("No eligible nodes found for placement")
            return []

        # Apply placement strategy
        if strategy == PlacementStrategy.SPREAD:
            return self._spread_placement(eligible_nodes, count, existing_placements)

        elif strategy == PlacementStrategy.PACK:
            return self._pack_placement(eligible_nodes, count, existing_placements)

        elif strategy == PlacementStrategy.RANDOM:
            return self._random_placement(eligible_nodes, count)

        elif strategy == PlacementStrategy.AFFINITY:
            return self._affinity_placement(eligible_nodes, count, node_selector)

        elif strategy == PlacementStrategy.RESOURCE:
            return self._resource_placement(eligible_nodes, count, node_selector)

        else:
            logger.warning(
                "Unknown placement strategy, using spread", strategy=strategy
            )
            return self._spread_placement(eligible_nodes, count, existing_placements)

    def _spread_placement(
        self,
        nodes: list[NodeInfo],
        count: int,
        existing_placements: dict[str, list[str]] | None = None,
    ) -> list[NodeInfo]:
        """Spread containers across different nodes."""

        # Sort nodes by current container count (ascending)
        if existing_placements:
            nodes_with_counts: list[tuple[NodeInfo, int]] = [
                (
                    node,
                    len(existing_placements.get(node.node_id, [])),
                )
                for node in nodes
            ]
            nodes_with_counts.sort(key=lambda x: x[1])
            sorted_nodes: list[NodeInfo] = [node for node, _ in nodes_with_counts]
        else:
            sorted_nodes = nodes

        # Select nodes in round-robin fashion
        selected: list[NodeInfo] = []
        node_index: int = 0

        for _ in range(min(count, len(sorted_nodes))):
            selected.append(sorted_nodes[node_index])
            node_index = (node_index + 1) % len(sorted_nodes)

        return selected

    def _pack_placement(
        self,
        nodes: list[NodeInfo],
        count: int,
        existing_placements: dict[str, list[str]] | None = None,
    ) -> list[NodeInfo]:
        """Pack containers onto same nodes."""

        # Sort nodes by current container count (descending)
        if existing_placements:
            nodes_with_counts: list[tuple[NodeInfo, int]] = [
                (
                    node,
                    len(existing_placements.get(node.node_id, [])),
                )
                for node in nodes
            ]
            nodes_with_counts.sort(key=lambda x: x[1], reverse=True)
            sorted_nodes = [node for node, _ in nodes_with_counts]
        else:
            sorted_nodes = nodes

        # Fill up nodes sequentially
        selected: list[NodeInfo] = []
        for node in sorted_nodes:
            if len(selected) >= count:
                break
            selected.append(node)

        return selected

    def _random_placement(self, nodes: list[NodeInfo], count: int) -> list[NodeInfo]:
        """Random container placement."""
        import random

        return random.choices(nodes, k=min(count, len(nodes)))

    def _affinity_placement(
        self,
        nodes: list[NodeInfo],
        count: int,
        node_selector: NodeSelector | None = None,
    ) -> list[NodeInfo]:
        """Placement based on node affinity preferences."""

        if not node_selector or not node_selector.node_affinity:
            return self._spread_placement(nodes, count)

        # Score nodes based on preferred affinity rules
        node_scores: list[tuple[NodeInfo, int]] = []

        for node in nodes:
            score: int = 0
            node_labels: dict[str, str] = getattr(node, "labels", {})

            for affinity in node_selector.node_affinity:
                if affinity.type == NodeAffinityType.PREFERRED:
                    if self._evaluate_affinity_rule(affinity, node_labels):
                        score += affinity.weight

            node_scores.append((node, score))

        # Sort by score (descending) and select top nodes
        node_scores.sort(key=lambda x: x[1], reverse=True)
        selected: list[NodeInfo] = [node for node, _ in node_scores[:count]]

        return selected

    def _resource_placement(
        self,
        nodes: list[NodeInfo],
        count: int,
        node_selector: NodeSelector | None = None,
    ) -> list[NodeInfo]:
        """Placement based on resource availability."""

        # Score nodes based on available resources
        node_scores: list[tuple[NodeInfo, int]] = []

        for node in nodes:
            resources: dict[str, Any] = getattr(node, "resources", {})

            # Calculate resource score
            score: int = 0
            score += resources.get("available_cpu_cores", 0) * 10
            score += resources.get("available_memory_mb", 0) / 1024  # Convert to GB
            score += resources.get("available_disk_gb", 0)
            score += resources.get("available_gpu_count", 0) * 100

            node_scores.append((node, score))

        # Sort by resource score (descending) and select top nodes
        def sort_key(x: tuple[NodeInfo, int]) -> int:
            return x[1]

        node_scores.sort(key=sort_key, reverse=True)
        selected: list[NodeInfo] = [
            node
            for node, _ in node_scores[:count]
        ]

        return selected

    def _evaluate_affinity_rule(
        self,
        affinity: NodeAffinity,
        node_labels: dict[str, str],
    ) -> bool:
        """Evaluate affinity rule (shared with NodeSelector)."""
        if affinity.operator == "In":
            return node_labels.get(affinity.key) in affinity.values
        elif affinity.operator == "NotIn":
            return node_labels.get(affinity.key) not in affinity.values
        elif affinity.operator == "Exists":
            return affinity.key in node_labels
        elif affinity.operator == "DoesNotExist":
            return affinity.key not in node_labels
        else:
            return False

    def record_placement(
        self,
        container_id: str,
        node_id: str,
        strategy: PlacementStrategy,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a placement decision for analysis."""
        placement_record: dict[str, Any] = {
            "container_id": container_id,
            "node_id": node_id,
            "strategy": strategy.value,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {},
        }

        self.placement_history.append(placement_record)

        # Keep only recent history (last 1000 placements)
        if len(self.placement_history) > 1000:
            self.placement_history = self.placement_history[-1000:]

    def get_placement_stats(self) -> dict[str, Any]:
        """Get placement statistics and analysis."""
        if not self.placement_history:
            return {"total_placements": 0}

        total: int = len(self.placement_history)

        # Strategy distribution
        strategy_counts: dict[str, int] = {}
        for record in self.placement_history:
            strategy = record["strategy"]
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        # Node distribution
        node_counts: dict[str, int] = {}
        for record in self.placement_history:
            node_id = record["node_id"]
            node_counts[node_id] = node_counts.get(node_id, 0) + 1

        return {
            "total_placements": total,
            "strategy_distribution": strategy_counts,
            "node_distribution": node_counts,
            "most_used_strategy": max(strategy_counts.items(), key=lambda x: x[1])[0]
            if strategy_counts
            else None,
            "most_used_node": max(node_counts.items(), key=lambda x: x[1])[0]
            if node_counts
            else None,
        }
