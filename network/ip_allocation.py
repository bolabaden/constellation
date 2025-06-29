"""
IP Allocation System for Constellation

Provides flexible IP allocation strategies for containers and services.
"""
from __future__ import annotations

import os
import re

from dataclasses import dataclass
from enum import Enum
from ipaddress import AddressValueError, IPv4Address, IPv4Network
from typing import Any

import structlog

from pydantic import BaseModel, Field, validator

logger = structlog.get_logger(__name__)


class IPAllocationStrategy(str, Enum):
    """IP allocation strategies for containers."""

    DHCP = "dhcp"
    STATIC = "static"
    ENV_VAR = "env_var"
    SUBNET_POOL = "subnet_pool"
    VPN_GATEWAY = "vpn_gateway"


class NetworkMode(str, Enum):
    """Network mode options for containers."""

    BRIDGE = "bridge"
    HOST = "host"
    NONE = "none"
    SERVICE = "service"  # network_mode: service:<container_name>
    CONTAINER = "container"  # network_mode: container:<container_name>


@dataclass
class IPAllocation:
    """Represents an IP allocation for a container or service."""

    strategy: IPAllocationStrategy
    value: str | None = None
    env_var: str | None = None
    default_value: str | None = None
    subnet: str | None = None
    gateway: str | None = None

    def __post_init__(self):
        """Validate allocation configuration."""
        if self.strategy == IPAllocationStrategy.STATIC and not self.value:
            raise ValueError("Static IP allocation requires a value")
        if self.strategy == IPAllocationStrategy.ENV_VAR and not self.env_var:
            raise ValueError("Environment variable allocation requires env_var")


class NetworkConfig(BaseModel):
    """Network configuration for a container or service."""

    name: str = Field(..., description="Network name")
    mode: NetworkMode = Field(default=NetworkMode.BRIDGE, description="Network mode")
    ip_allocation: IPAllocation = Field(default_factory=lambda: IPAllocation(IPAllocationStrategy.DHCP))
    extra_hosts: dict[str, str] = Field(default_factory=dict, description="Additional host mappings")
    aliases: list[str] = Field(default_factory=list, description="Network aliases")

    # VPN-specific configuration
    vpn_container: str | None = Field(None, description="VPN container name for service mode")
    vpn_type: str | None = Field(None, description="VPN type (warp, warp2, tailscale)")

    class Config:
        arbitrary_types_allowed: bool = True

    @validator("ip_allocation", pre=True)
    def validate_ip_allocation(cls, v: dict | IPAllocation) -> IPAllocation:
        """Validate IP allocation configuration."""
        if isinstance(v, dict):
            return IPAllocation(**v)
        return v


class IPAllocator:
    """Handles IP allocation for containers and services."""

    def __init__(self, subnet_pools: dict[str, str] | None = None):
        """
        Initialize IP allocator.

        Args:
            subnet_pools: Dictionary of subnet name to CIDR mapping
        """
        self.subnet_pools = subnet_pools or {}
        self.allocated_ips: dict[str, str] = {}
        self.env_cache: dict[str, str] = {}

    def allocate_ip(self, container_name: str, allocation: IPAllocation) -> str | None:
        """
        Allocate an IP address based on the allocation strategy.

        Args:
            container_name: Name of the container
            allocation: IP allocation configuration

        Returns:
            Allocated IP address or None for DHCP
        """
        logger.info("Allocating IP", container=container_name, strategy=allocation.strategy)

        if allocation.strategy == IPAllocationStrategy.DHCP:
            return None  # Let Docker handle DHCP allocation

        elif allocation.strategy == IPAllocationStrategy.STATIC:
            if not allocation.value:
                raise ValueError("Static IP allocation requires a value")
            ip = self._validate_ip(allocation.value)
            if ip:
                self.allocated_ips[container_name] = ip
                return ip

        elif allocation.strategy == IPAllocationStrategy.ENV_VAR:
            if not allocation.env_var:
                raise ValueError("Environment variable allocation requires env_var")
            return self._resolve_env_var(allocation.env_var, allocation.default_value)

        elif allocation.strategy == IPAllocationStrategy.SUBNET_POOL:
            if not allocation.subnet:
                raise ValueError("Subnet pool allocation requires a subnet")
            return self._allocate_from_pool(container_name, allocation.subnet)

        elif allocation.strategy == IPAllocationStrategy.VPN_GATEWAY:
            if not allocation.gateway:
                raise ValueError("VPN gateway allocation requires a gateway")
            return self._allocate_vpn_gateway(container_name, allocation)

        logger.warning("Unknown allocation strategy", strategy=allocation.strategy)
        return None

    def _validate_ip(self, ip_str: str) -> str | None:
        """Validate IP address format."""
        try:
            IPv4Address(ip_str)
            return ip_str
        except AddressValueError:
            logger.error("Invalid IP address format", ip=ip_str)
            return None

    def _resolve_env_var(self, env_var: str, default: str | None = None) -> str | None:
        """
        Resolve environment variable with Docker Compose syntax support.

        Supports patterns like: ${WARP_IPV4_ADDRESS:-10.76.128.97}
        """
        if env_var in self.env_cache:
            return self.env_cache[env_var]

        # Handle Docker Compose environment variable syntax
        env_pattern = r"\$\{([^}:]+)(?::(-?)([^}]*))?\}"
        match = re.match(env_pattern, env_var)

        if match:
            var_name = match.group(1)
            default_val = match.group(3) if match.group(3) else default
            value = os.getenv(var_name, default_val)
        else:
            value = os.getenv(env_var, default)

        if value:
            validated_ip = self._validate_ip(value)
            if validated_ip:
                self.env_cache[env_var] = validated_ip
                return validated_ip

        logger.warning("Could not resolve environment variable", env_var=env_var)
        return default

    def _allocate_from_pool(self, container_name: str, subnet_name: str | None) -> str | None:
        """Allocate IP from a subnet pool."""
        if not subnet_name or subnet_name not in self.subnet_pools:
            logger.error("Subnet pool not found", subnet=subnet_name)
            return None

        try:
            network = IPv4Network(self.subnet_pools[subnet_name])
            # Simple allocation: find first available IP
            for ip in network.hosts():
                ip_str = str(ip)
                if ip_str not in self.allocated_ips.values():
                    self.allocated_ips[container_name] = ip_str
                    return ip_str
        except Exception as e:
            logger.error("Failed to allocate from pool", subnet=subnet_name, error=str(e))

        return None

    def _allocate_vpn_gateway(self, container_name: str, allocation: IPAllocation) -> str | None:
        """Allocate VPN gateway IP."""
        # For VPN gateways, we typically use the gateway IP of the subnet
        if allocation.gateway:
            return self._validate_ip(allocation.gateway)
        return None

    def deallocate_ip(self, container_name: str) -> None:
        """Deallocate IP for a container."""
        if container_name in self.allocated_ips:
            del self.allocated_ips[container_name]
            logger.info("Deallocated IP", container=container_name)


class VPNNetworkManager:
    """Manages VPN network configurations and host mappings."""

    def __init__(self):
        """Initialize VPN network manager."""
        self.vpn_containers: dict[str, dict[str, Any]] = {}
        self.host_mappings: dict[str, str] = {}

    def register_vpn_container(
        self,
        name: str,
        vpn_type: str,
        ip_address: str,
        extra_hosts: dict[str, str] | None = None,
    ) -> None:
        """
        Register a VPN container.

        Args:
            name: Container name
            vpn_type: Type of VPN (warp, warp2, tailscale)
            ip_address: VPN container IP
            extra_hosts: Additional host mappings
        """
        self.vpn_containers[name] = {
            "type": vpn_type,
            "ip": ip_address,
            "extra_hosts": extra_hosts or {},
        }

        # Update host mappings
        self.host_mappings[name] = ip_address
        if extra_hosts:
            self.host_mappings.update(extra_hosts)

        logger.info("Registered VPN container", name=name, type=vpn_type, ip=ip_address)

    def get_gluetun_hosts_workaround(self) -> list[str]:
        """
        Generate the gluetun-hosts-workaround configuration.

        Returns:
            List of host:ip mappings for Docker extra_hosts
        """
        hosts = ["host.docker.internal:host-gateway"]

        for hostname, ip in self.host_mappings.items():
            hosts.append(f"{hostname}:{ip}")

        return hosts

    def resolve_network_mode(self, config: NetworkConfig) -> str:
        """
        Resolve network mode configuration.

        Args:
            config: Network configuration

        Returns:
            Docker network mode string
        """
        if config.mode == NetworkMode.SERVICE and config.vpn_container:
            return f"service:{config.vpn_container}"
        elif config.mode == NetworkMode.CONTAINER and config.vpn_container:
            return f"container:{config.vpn_container}"
        else:
            return config.mode.value

    def get_vpn_dependencies(self, container_name: str) -> list[str]:
        """
        Get VPN container dependencies for a service.

        Args:
            container_name: Name of the container

        Returns:
            List of VPN container dependencies
        """
        dependencies = []

        # Check if this container uses a VPN network mode
        for vpn_name in self.vpn_containers:
            if container_name.startswith(vpn_name) or vpn_name in container_name:
                dependencies.append(vpn_name)

        return dependencies


# Factory function for creating common IP allocation configurations
def create_dhcp_allocation() -> IPAllocation:
    """Create DHCP IP allocation."""
    return IPAllocation(IPAllocationStrategy.DHCP)


def create_static_allocation(ip: str) -> IPAllocation:
    """Create static IP allocation."""
    return IPAllocation(IPAllocationStrategy.STATIC, value=ip)


def create_env_allocation(env_var: str, default: str | None = None) -> IPAllocation:
    """Create environment variable IP allocation."""
    return IPAllocation(IPAllocationStrategy.ENV_VAR, env_var=env_var, default_value=default)


def create_warp_allocation(warp_type: str = "warp") -> IPAllocation:
    """Create Cloudflare WARP IP allocation."""
    return (
        create_env_allocation("${WARP2_IPV4_ADDRESS:-10.76.128.98}")
        if warp_type == "warp2"
        else create_env_allocation("${WARP_IPV4_ADDRESS:-10.76.128.97}")
    )