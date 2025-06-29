"""
Core container and service configuration classes for Constellation.

Provides base classes for defining containerized services with networking,
health checks, and failover capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import structlog

from pydantic import BaseModel, Field, validator

from constellation.network.ip_allocation import (
    NetworkConfig,
    NetworkMode,
    create_dhcp_allocation,
    create_env_allocation,
)

logger = structlog.get_logger(__name__)


class ContainerStatus(str, Enum):
    """Container status enumeration."""

    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    RESTARTING = "restarting"


class HealthCheckStatus(str, Enum):
    """Health check status enumeration."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    UNKNOWN = "unknown"


class RestartPolicy(str, Enum):
    """Container restart policy."""

    NO = "no"
    ALWAYS = "always"
    ON_FAILURE = "on-failure"
    UNLESS_STOPPED = "unless-stopped"


@dataclass
class HealthCheck:
    """Health check configuration for containers."""

    test: str | list[str]
    interval: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    retries: int = 3
    start_period: timedelta = field(default_factory=lambda: timedelta(seconds=0))

    def to_docker_config(self) -> dict[str, Any]:
        """Convert to Docker health check configuration."""
        test_cmd = (
            self.test if isinstance(self.test, list) else ["CMD-SHELL", self.test]
        )

        return {
            "test": test_cmd,
            "interval": int(self.interval.total_seconds() * 1000000000),  # nanoseconds
            "timeout": int(self.timeout.total_seconds() * 1000000000),
            "retries": self.retries,
            "start_period": int(self.start_period.total_seconds() * 1000000000),
        }


@dataclass
class VolumeMount:
    """Volume mount configuration."""

    source: str
    target: str
    type: str = "bind"  # bind, volume, tmpfs
    read_only: bool = False
    consistency: str | None = None  # consistent, cached, delegated


@dataclass
class PortMapping:
    """Port mapping configuration."""

    container_port: int
    host_port: int | None = None
    protocol: str = "tcp"
    host_ip: str = "0.0.0.0"


class ContainerConfig(BaseModel):
    """Base configuration for a container."""

    # Basic container configuration
    name: str = Field(..., description="Container name")
    image: str = Field(..., description="Docker image")
    tag: str = Field(default="latest", description="Image tag")

    # Networking
    networks: list[NetworkConfig] = Field(
        default_factory=list, description="Network configurations"
    )
    ports: list[PortMapping] = Field(default_factory=list, description="Port mappings")
    hostname: str | None = Field(None, description="Container hostname")

    # Environment and configuration
    environment: dict[str, str] = Field(
        default_factory=dict, description="Environment variables"
    )
    command: str | list[str] | None = Field(None, description="Container command")
    entrypoint: str | list[str] | None = Field(None, description="Container entrypoint")
    working_dir: str | None = Field(None, description="Working directory")
    user: str | None = Field(None, description="User to run as")

    # Storage
    volumes: list[VolumeMount] = Field(
        default_factory=list, description="Volume mounts"
    )

    # Resource limits
    cpu_limit: float | None = Field(None, description="CPU limit (cores)")
    memory_limit: str | None = Field(
        None, description="Memory limit (e.g., '512m', '1g')"
    )

    # Health and lifecycle
    health_check: HealthCheck | None = Field(
        None, description="Health check configuration"
    )
    restart_policy: RestartPolicy = Field(
        default=RestartPolicy.UNLESS_STOPPED, description="Restart policy"
    )

    # Dependencies
    depends_on: list[str] = Field(
        default_factory=list, description="Container dependencies"
    )

    # Labels and metadata
    labels: dict[str, str] = Field(default_factory=dict, description="Container labels")

    # Failover configuration
    fallback_containers: list[str] = Field(
        default_factory=list, description="Fallback container names"
    )
    max_restart_attempts: int = Field(
        default=3, description="Maximum restart attempts before failover"
    )

    class Config:
        arbitrary_types_allowed = True

    @validator("networks", pre=True)
    def ensure_default_network(cls, v):
        """Ensure at least one network configuration exists."""
        if not v:
            # Default to DHCP allocation on bridge network
            return [
                NetworkConfig(
                    name="default",
                    mode=NetworkMode.BRIDGE,
                    ip_allocation=create_dhcp_allocation(),
                    vpn_container=None,
                    vpn_type=None,
                )
            ]
        return v

    def get_full_image_name(self) -> str:
        """Get the full Docker image name with tag."""
        return f"{self.image}:{self.tag}"

    def get_primary_network(self) -> NetworkConfig:
        """Get the primary network configuration."""
        return (
            self.networks[0]
            if self.networks
            else NetworkConfig(
                name="default",
                ip_allocation=create_dhcp_allocation(),
                vpn_container=None,
                vpn_type=None,
            )
        )

    def has_vpn_network(self) -> bool:
        """Check if container uses VPN networking."""
        return any(
            net.mode in [NetworkMode.SERVICE, NetworkMode.CONTAINER]
            and net.vpn_container
            for net in self.networks
        )


@dataclass
class ContainerInstance:
    """Represents a running container instance."""

    id: str
    config: ContainerConfig
    node_id: str
    status: ContainerStatus = ContainerStatus.PENDING
    health_status: HealthCheckStatus = HealthCheckStatus.UNKNOWN
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    restart_count: int = 0
    last_health_check: datetime | None = None
    allocated_ip: str | None = None

    def is_healthy(self) -> bool:
        """Check if container is healthy."""
        return (
            self.status == ContainerStatus.RUNNING
            and self.health_status == HealthCheckStatus.HEALTHY
        )

    def needs_restart(self) -> bool:
        """Check if container needs restart."""
        return (
            self.status in [ContainerStatus.FAILED, ContainerStatus.STOPPED]
            and self.restart_count < self.config.max_restart_attempts
        )

    def needs_failover(self) -> bool:
        """Check if container needs failover to backup."""
        return (
            self.restart_count >= self.config.max_restart_attempts
            and len(self.config.fallback_containers) > 0
        )


class ServiceConfig(BaseModel):
    """Configuration for a service (group of containers)."""

    name: str = Field(..., description="Service name")
    containers: list[ContainerConfig] = Field(
        ..., description="Container configurations"
    )

    # Service-level configuration
    replicas: int = Field(default=1, description="Number of replicas")
    placement_strategy: str = Field(
        default="spread", description="Container placement strategy"
    )

    # Load balancing and networking
    load_balancer: dict[str, Any] | None = Field(
        None, description="Load balancer configuration"
    )
    service_discovery: bool = Field(
        default=True, description="Enable service discovery"
    )

    # High availability
    min_healthy_replicas: int = Field(default=1, description="Minimum healthy replicas")
    max_unavailable: int = Field(
        default=0, description="Maximum unavailable replicas during updates"
    )

    # Update strategy
    update_strategy: str = Field(default="rolling", description="Update strategy")
    update_parallelism: int = Field(
        default=1, description="Number of containers to update in parallel"
    )

    def get_primary_container(self) -> ContainerConfig:
        """Get the primary container configuration."""
        return self.containers[0]

    def get_fallback_containers(self) -> list[ContainerConfig]:
        """Get fallback container configurations."""
        return self.containers[1:] if len(self.containers) > 1 else []


class VPNContainerConfig(ContainerConfig):
    """Specialized configuration for VPN containers."""

    vpn_type: str = Field(
        ..., description="VPN type (warp, warp2, tailscale, nordlynx)"
    )
    vpn_config_path: str | None = Field(
        None, description="Path to VPN configuration file"
    )
    dns_servers: list[str] = Field(
        default_factory=list, description="Custom DNS servers"
    )

    # VPN-specific networking
    firewall_enabled: bool = Field(default=True, description="Enable VPN firewall")
    kill_switch: bool = Field(default=True, description="Enable kill switch")

    # Gluetun-specific configuration
    gluetun_provider: str | None = Field(None, description="Gluetun VPN provider")
    gluetun_server_countries: list[str] = Field(
        default_factory=list, description="Server countries"
    )

    def __init__(self, **data):
        """Initialize VPN container with defaults."""
        # Set default network configuration for VPN containers
        if "networks" not in data or not data["networks"]:
            data["networks"] = [
                NetworkConfig(
                    name="publicnet",
                    ip_allocation=create_env_allocation(
                        f"${{{self._get_env_var_name(data.get('name', 'vpn'))}:-{self._get_default_ip()}}}"
                    ),
                    vpn_container=None,
                    vpn_type=data.get("vpn_type", "warp"),
                )
            ]

        super().__init__(**data)

    def _get_env_var_name(self, name: str) -> str:
        """Generate environment variable name for VPN container."""
        return f"{name.upper().replace('-', '_')}_IPV4_ADDRESS"

    def _get_default_ip(self) -> str:
        """Get default IP for VPN container."""
        # Default IP ranges for different VPN types
        defaults = {
            "warp": "10.76.128.97",
            "warp2": "10.76.128.98",
            "tailscale": "100.64.0.1",
            "nordlynx": "10.76.128.99",
        }
        return defaults.get(self.vpn_type, "10.76.128.100")


# Factory functions for common container configurations
def create_basic_container(name: str, image: str, **kwargs) -> ContainerConfig:
    """Create a basic container configuration with DHCP networking."""
    return ContainerConfig(
        name=name,
        image=image,
        networks=[
            NetworkConfig(
                name="default",
                ip_allocation=create_dhcp_allocation(),
                vpn_container=None,
                vpn_type=None,
            )
        ],
        **kwargs,
    )


def create_vpn_routed_container(
    name: str,
    image: str,
    vpn_container: str,
    **kwargs,
) -> ContainerConfig:
    """Create a container that routes through a VPN container."""
    return ContainerConfig(
        name=name,
        image=image,
        networks=[
            NetworkConfig(
                name="vpn",
                mode=NetworkMode.SERVICE,
                vpn_container=vpn_container,
                ip_allocation=create_dhcp_allocation(),
                vpn_type=None,
            )
        ],
        depends_on=[vpn_container],
        **kwargs,
    )


def create_warp_container(name: str = "warp", **kwargs) -> VPNContainerConfig:
    """Create a Cloudflare WARP VPN container configuration."""
    return VPNContainerConfig(
        name=name,
        image="caomingjun/warp",
        tag="latest",
        vpn_type="warp",
        **kwargs,
    )


def create_tailscale_container(name: str = "tailscale", **kwargs) -> VPNContainerConfig:
    """Create a Tailscale VPN container configuration."""
    return VPNContainerConfig(
        name=name,
        image="tailscale/tailscale",
        tag="latest",
        vpn_type="tailscale",
        **kwargs,
    )
