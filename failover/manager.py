"""
Failover Manager for Constellation

Handles container-level failover, health monitoring, and automatic recovery.
"""

from __future__ import annotations

import asyncio

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


class FailoverEvent(str, Enum):
    """Failover event types."""

    CONTAINER_UNHEALTHY = "container_unhealthy"
    CONTAINER_FAILED = "container_failed"
    RESTART_ATTEMPTED = "restart_attempted"
    RESTART_FAILED = "restart_failed"
    FAILOVER_TRIGGERED = "failover_triggered"
    FAILOVER_COMPLETED = "failover_completed"
    RECOVERY_STARTED = "recovery_started"
    RECOVERY_COMPLETED = "recovery_completed"


@dataclass
class FailoverRule:
    """Defines failover behavior for a container or service."""

    # Health check configuration
    health_check_interval: timedelta = field(
        default_factory=lambda: timedelta(seconds=30)
    )
    health_check_timeout: timedelta = field(
        default_factory=lambda: timedelta(seconds=10)
    )
    unhealthy_threshold: int = 3  # Consecutive failures before considering unhealthy

    # Restart behavior
    max_restart_attempts: int = 3
    restart_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    restart_backoff_multiplier: float = 2.0
    max_restart_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))

    # Failover behavior
    enable_failover: bool = True
    failover_delay: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    fallback_containers: list[str] = field(default_factory=list)

    # Recovery behavior
    enable_recovery: bool = True
    recovery_check_interval: timedelta = field(
        default_factory=lambda: timedelta(minutes=1)
    )
    recovery_attempts: int = 3


@dataclass
class ContainerState:
    """Tracks the state of a container for failover purposes."""

    name: str
    node_id: str
    is_healthy: bool = True
    consecutive_failures: int = 0
    restart_count: int = 0
    last_health_check: datetime | None = None
    last_restart_attempt: datetime | None = None
    is_primary: bool = True
    failover_active: bool = False
    active_fallback: str | None = None

    def reset_failure_count(self) -> None:
        """Reset failure counters when container becomes healthy."""
        self.consecutive_failures = 0
        self.restart_count = 0

    def increment_failure(self) -> None:
        """Increment failure counter."""
        self.consecutive_failures += 1

    def increment_restart(self) -> None:
        """Increment restart counter."""
        self.restart_count += 1
        self.last_restart_attempt = datetime.utcnow()


class FailoverManager:
    """Manages container failover and recovery operations."""

    def __init__(self, docker_client, node_discovery_service):
        """
        Initialize failover manager.

        Args:
            docker_client: Docker client for container operations
            node_discovery_service: Service for node discovery and communication
        """
        self.docker_client = docker_client
        self.node_discovery = node_discovery_service

        # State tracking
        self.container_states: dict[str, ContainerState] = {}
        self.failover_rules: dict[str, FailoverRule] = {}
        self.event_handlers: dict[FailoverEvent, list[Callable]] = {}

        # Control flags
        self._running = False
        self._health_check_task: asyncio.Task | None = None
        self._recovery_task: asyncio.Task | None = None

        logger.info("Failover manager initialized")

    async def start(self) -> None:
        """Start the failover manager."""
        if self._running:
            logger.warning("Failover manager already running")
            return

        self._running = True

        # Start background tasks
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._recovery_task = asyncio.create_task(self._recovery_loop())

        logger.info("Failover manager started")

    async def stop(self) -> None:
        """Stop the failover manager."""
        self._running = False

        # Cancel background tasks
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass

        logger.info("Failover manager stopped")

    def register_container(
        self,
        container_name: str,
        node_id: str,
        failover_rule: FailoverRule | None = None,
    ) -> None:
        """
        Register a container for failover monitoring.

        Args:
            container_name: Name of the container
            node_id: ID of the node hosting the container
            failover_rule: Failover configuration (uses default if None)
        """
        if container_name not in self.container_states:
            self.container_states[container_name] = ContainerState(
                name=container_name, node_id=node_id
            )

        if failover_rule:
            self.failover_rules[container_name] = failover_rule
        elif container_name not in self.failover_rules:
            self.failover_rules[container_name] = FailoverRule()

        logger.info(
            "Registered container for failover", container=container_name, node=node_id
        )

    def unregister_container(self, container_name: str) -> None:
        """Unregister a container from failover monitoring."""
        self.container_states.pop(container_name, None)
        self.failover_rules.pop(container_name, None)
        logger.info("Unregistered container from failover", container=container_name)

    def add_event_handler(self, event: FailoverEvent, handler: Callable) -> None:
        """Add an event handler for failover events."""
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        self.event_handlers[event].append(handler)

    async def _emit_event(
        self, event: FailoverEvent, container_name: str, **kwargs
    ) -> None:
        """Emit a failover event to registered handlers."""
        if event in self.event_handlers:
            for handler in self.event_handlers[event]:
                try:
                    await handler(container_name, **kwargs)
                except Exception as e:
                    logger.error("Event handler failed", event=event, error=str(e))

    async def _health_check_loop(self) -> None:
        """Main health check loop."""
        while self._running:
            try:
                await self._check_all_containers()
                await asyncio.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error("Health check loop error", error=str(e))
                await asyncio.sleep(10)  # Wait longer on error

    async def _recovery_loop(self) -> None:
        """Recovery check loop for failed containers."""
        while self._running:
            try:
                await self._check_recovery_opportunities()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error("Recovery loop error", error=str(e))
                await asyncio.sleep(60)

    async def _check_all_containers(self) -> None:
        """Check health of all registered containers."""
        for container_name in list(self.container_states.keys()):
            try:
                await self._check_container_health(container_name)
            except Exception as e:
                logger.error(
                    "Container health check failed",
                    container=container_name,
                    error=str(e),
                )

    async def _check_container_health(self, container_name: str) -> None:
        """Check health of a specific container."""
        state = self.container_states.get(container_name)
        rule = self.failover_rules.get(container_name)

        if not state or not rule:
            return

        # Skip health check if not enough time has passed
        if (
            state.last_health_check
            and datetime.utcnow() - state.last_health_check < rule.health_check_interval
        ):
            return

        state.last_health_check = datetime.utcnow()

        try:
            # Get container from Docker
            container = self.docker_client.containers.get(container_name)

            # Check if container is running
            if container.status != "running":
                await self._handle_unhealthy_container(
                    container_name, "Container not running"
                )
                return

            # Check health status if available
            health = container.attrs.get("State", {}).get("Health", {})
            if health:
                health_status = health.get("Status", "unknown")
                if health_status == "unhealthy":
                    await self._handle_unhealthy_container(
                        container_name, "Health check failed"
                    )
                    return
                elif health_status == "healthy":
                    await self._handle_healthy_container(container_name)
                    return

            # If no health check, assume healthy if running
            await self._handle_healthy_container(container_name)

        except Exception as e:
            await self._handle_unhealthy_container(
                container_name, f"Health check error: {str(e)}"
            )

    async def _handle_healthy_container(self, container_name: str) -> None:
        """Handle a healthy container."""
        state = self.container_states[container_name]

        if not state.is_healthy:
            logger.info("Container recovered", container=container_name)
            state.is_healthy = True
            state.reset_failure_count()

            # If this was a fallback that's now healthy, consider recovery
            if state.failover_active and not state.is_primary:
                await self._consider_recovery(container_name)

    async def _handle_unhealthy_container(
        self, container_name: str, reason: str
    ) -> None:
        """Handle an unhealthy container."""
        state = self.container_states[container_name]
        rule = self.failover_rules[container_name]

        state.increment_failure()
        state.is_healthy = False

        logger.warning(
            "Container unhealthy",
            container=container_name,
            reason=reason,
            failures=state.consecutive_failures,
        )

        await self._emit_event(
            FailoverEvent.CONTAINER_UNHEALTHY, container_name, reason=reason
        )

        # Check if we've exceeded the unhealthy threshold
        if state.consecutive_failures >= rule.unhealthy_threshold:
            await self._handle_failed_container(container_name)

    async def _handle_failed_container(self, container_name: str) -> None:
        """Handle a failed container."""
        state = self.container_states[container_name]
        rule = self.failover_rules[container_name]

        logger.error(
            "Container failed",
            container=container_name,
            restart_count=state.restart_count,
        )

        await self._emit_event(FailoverEvent.CONTAINER_FAILED, container_name)

        # Try restart if we haven't exceeded max attempts
        if state.restart_count < rule.max_restart_attempts:
            await self._attempt_restart(container_name)
        elif rule.enable_failover and rule.fallback_containers:
            await self._trigger_failover(container_name)
        else:
            logger.error("No recovery options available", container=container_name)

    async def _attempt_restart(self, container_name: str) -> None:
        """Attempt to restart a failed container."""
        state = self.container_states[container_name]
        rule = self.failover_rules[container_name]

        # Calculate restart delay with exponential backoff
        delay = rule.restart_delay.total_seconds() * (
            rule.restart_backoff_multiplier**state.restart_count
        )
        delay = min(delay, rule.max_restart_delay.total_seconds())

        logger.info(
            "Attempting container restart",
            container=container_name,
            delay=delay,
            attempt=state.restart_count + 1,
        )

        await self._emit_event(
            FailoverEvent.RESTART_ATTEMPTED,
            container_name,
            attempt=state.restart_count + 1,
        )

        await asyncio.sleep(delay)

        try:
            container = self.docker_client.containers.get(container_name)
            container.restart()

            state.increment_restart()
            logger.info("Container restart initiated", container=container_name)

        except Exception as e:
            logger.error(
                "Container restart failed", container=container_name, error=str(e)
            )
            await self._emit_event(
                FailoverEvent.RESTART_FAILED, container_name, error=str(e)
            )

            # If restart failed, try failover
            if rule.enable_failover and rule.fallback_containers:
                await self._trigger_failover(container_name)

    async def _trigger_failover(self, container_name: str) -> None:
        """Trigger failover to a backup container."""
        state = self.container_states[container_name]
        rule = self.failover_rules[container_name]

        if not rule.fallback_containers:
            logger.error("No fallback containers available", container=container_name)
            return

        logger.info(
            "Triggering failover",
            container=container_name,
            fallbacks=rule.fallback_containers,
        )

        await self._emit_event(FailoverEvent.FAILOVER_TRIGGERED, container_name)

        # Try each fallback container in order
        for fallback_name in rule.fallback_containers:
            try:
                await self._start_fallback_container(container_name, fallback_name)
                state.failover_active = True
                state.active_fallback = fallback_name

                logger.info(
                    "Failover completed", primary=container_name, fallback=fallback_name
                )
                await self._emit_event(
                    FailoverEvent.FAILOVER_COMPLETED,
                    container_name,
                    fallback=fallback_name,
                )
                return

            except Exception as e:
                logger.error(
                    "Fallback container failed to start",
                    fallback=fallback_name,
                    error=str(e),
                )

        logger.error("All fallback containers failed", container=container_name)

    async def _start_fallback_container(
        self, primary_name: str, fallback_name: str
    ) -> None:
        """Start a fallback container."""
        # This would typically involve:
        # 1. Getting the container configuration
        # 2. Starting the container on an available node
        # 3. Updating network routing/load balancer
        # 4. Registering the fallback for monitoring

        # For now, we'll simulate this
        logger.info(
            "Starting fallback container", primary=primary_name, fallback=fallback_name
        )

        # Register the fallback container for monitoring
        # Note: In a real implementation, we'd get the actual node_id
        self.register_container(fallback_name, "fallback-node")

    async def _check_recovery_opportunities(self) -> None:
        """Check if any failed primary containers can be recovered."""
        for container_name, state in self.container_states.items():
            if state.failover_active and state.is_primary:
                await self._consider_recovery(container_name)

    async def _consider_recovery(self, container_name: str) -> None:
        """Consider recovering a failed primary container."""
        state = self.container_states[container_name]
        rule = self.failover_rules[container_name]

        if not rule.enable_recovery or not state.failover_active:
            return

        try:
            # Check if primary container is now healthy
            container = self.docker_client.containers.get(container_name)
            if container.status == "running":
                health = container.attrs.get("State", {}).get("Health", {})
                if not health or health.get("Status") == "healthy":
                    await self._recover_primary_container(container_name)

        except Exception as e:
            logger.debug(
                "Primary container not ready for recovery",
                container=container_name,
                error=str(e),
            )

    async def _recover_primary_container(self, container_name: str) -> None:
        """Recover the primary container and stop fallback."""
        state = self.container_states[container_name]

        logger.info(
            "Recovering primary container",
            container=container_name,
            fallback=state.active_fallback,
        )

        await self._emit_event(FailoverEvent.RECOVERY_STARTED, container_name)

        try:
            # Stop the fallback container
            if state.active_fallback:
                await self._stop_fallback_container(state.active_fallback)

            # Update state
            state.failover_active = False
            state.active_fallback = None
            state.reset_failure_count()

            logger.info(
                "Primary container recovery completed", container=container_name
            )
            await self._emit_event(FailoverEvent.RECOVERY_COMPLETED, container_name)

        except Exception as e:
            logger.error("Recovery failed", container=container_name, error=str(e))

    async def _stop_fallback_container(self, fallback_name: str) -> None:
        """Stop a fallback container."""
        logger.info("Stopping fallback container", fallback=fallback_name)

        try:
            container = self.docker_client.containers.get(fallback_name)
            container.stop()

            # Unregister from monitoring
            self.unregister_container(fallback_name)

        except Exception as e:
            logger.error(
                "Failed to stop fallback container",
                fallback=fallback_name,
                error=str(e),
            )

    def get_container_status(self, container_name: str) -> dict[str, Any] | None:
        """Get the current status of a container."""
        state = self.container_states.get(container_name)
        if not state:
            return None

        return {
            "name": state.name,
            "node_id": state.node_id,
            "is_healthy": state.is_healthy,
            "consecutive_failures": state.consecutive_failures,
            "restart_count": state.restart_count,
            "last_health_check": state.last_health_check,
            "is_primary": state.is_primary,
            "failover_active": state.failover_active,
            "active_fallback": state.active_fallback,
        }

    def get_all_container_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all monitored containers."""
        return {
            name: self.get_container_status(name)
            for name in self.container_states.keys()
        }
