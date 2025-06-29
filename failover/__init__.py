"""
Constellation Failover Module

Provides container-level failover and recovery capabilities.
"""

from .manager import FailoverManager, FailoverRule, FailoverEvent, ContainerState

__all__ = [
    "FailoverManager",
    "FailoverRule", 
    "FailoverEvent",
    "ContainerState"
]
