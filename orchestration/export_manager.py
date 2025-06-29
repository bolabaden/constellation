"""Export Manager for Constellation.

This module provides functionality to export Constellation container specifications
to various orchestration platforms: Helm charts, Nomad jobs, and Docker Compose.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import yaml

from constellation.core.container import HealthCheck, RestartPolicy, VolumeMount

if TYPE_CHECKING:
    from pathlib import Path

    from constellation.core.container import ContainerConfig
    from constellation.core.service import ConstellationService


@dataclass
class ExportConfig:
    """Configuration for export operations."""

    output_dir: Path
    namespace: str = "constellation"
    chart_name: str = "constellation-stack"
    version: str = "1.0.0"
    include_secrets: bool = False
    include_volumes: bool = True
    include_networks: bool = True
    include_healthchecks: bool = True


class BaseExporter(ABC):
    """Base class for all exporters."""

    def __init__(self, config: ExportConfig):
        self.config: ExportConfig = config
        self.output_dir: Path = config.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def export(
        self,
        containers: list[ContainerConfig],
        services: list[ConstellationService] | None = None,
    ) -> Path:
        """Export containers to the target format."""
        pass

    def _sanitize_name(self, name: str) -> str:
        """Sanitize names for different platforms."""
        return name.lower().replace("_", "-").replace(" ", "-")


class DockerComposeExporter(BaseExporter):
    """Export Constellation containers to Docker Compose format with comprehensive feature support."""

    def export(
        self,
        containers: list[ContainerConfig],
        services: list[ConstellationService] | None = None,
    ) -> Path:
        """Export to Docker Compose format."""
        compose_file: Path = self.output_dir / "docker-compose.yml"

        compose_spec: dict[str, Any] = self._create_compose_spec(containers)

        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(
                compose_spec,
                f,
                default_flow_style=False,
                sort_keys=False,
            )

        return compose_file

    def _create_compose_spec(
        self,
        containers: list[ContainerConfig],
    ) -> dict[str, Any]:
        """Create comprehensive Docker Compose specification."""
        compose: dict[str, Any] = {
            "version": "3.8",
            "services": {},
        }

        # Collect networks, volumes, secrets, and configs used by containers
        used_networks: set[str] = set()
        used_volumes: set[str] = set()
        used_secrets: set[str] = set()
        used_configs: set[str] = set()

        # Add services
        for container in containers:
            # Use container name directly as service name for round-trip compatibility
            service_name: str = container.name
            service: dict[str, Any] = {
                "image": f"{container.image}:{container.tag}",
                "restart": self._map_restart_policy(container.restart_policy),
            }

            # Add container_name to specify the actual container name
            service["container_name"] = container.name

            # Handle networks - map container networks to Docker Compose networks
            if container.networks:
                # Add networks to the service
                service_networks: list[str] = []
                for network in container.networks:
                    # Map default network to constellation namespace
                    network_name = network.name
                    if network_name == "default":
                        network_name = self.config.namespace
                    service_networks.append(network_name)
                    used_networks.add(network_name)

                if service_networks:
                    service["networks"] = service_networks
            else:
                # If no networks specified, use the default constellation network
                default_network = self.config.namespace
                service["networks"] = [default_network]
                used_networks.add(default_network)

            # Add hostname
            if container.hostname and container.hostname.strip():
                service["hostname"] = container.hostname

            # Add ports with comprehensive format support
            if container.ports:
                service["ports"] = []
                for port in container.ports:
                    port_spec = self._export_port_specification(port)
                    service["ports"].append(port_spec)

            # Add environment
            if container.environment:
                service["environment"] = {
                    key: value
                    for key, value in container.environment.items()
                    if value is not None
                }

            # Add volumes with comprehensive support
            if container.volumes:
                service["volumes"] = []
                for mount in container.volumes:
                    volume_spec = self._format_volume_for_compose(mount)
                    service["volumes"].append(volume_spec)

                    # Track volumes for volume section
                    if (
                        mount.type == "volume"
                        and mount.source
                        and not mount.source.startswith("/")
                    ):
                        used_volumes.add(mount.source)

            # Add command and entrypoint with proper list formatting
            if container.command and (
                not isinstance(container.command, str) or container.command.strip()
            ):
                if isinstance(container.command, str):
                    # Split string command into list for better YAML representation
                    service["command"] = container.command.split()
                else:
                    service["command"] = container.command

            if container.entrypoint and (
                not isinstance(container.entrypoint, str)
                or container.entrypoint.strip()
            ):
                if isinstance(container.entrypoint, str):
                    service["entrypoint"] = container.entrypoint.split()
                else:
                    service["entrypoint"] = container.entrypoint

            if container.working_dir and container.working_dir.strip():
                service["working_dir"] = container.working_dir

            if container.user and container.user.strip():
                service["user"] = container.user

            # Add dependencies
            if container.depends_on:
                service["depends_on"] = container.depends_on

            # Add labels
            if container.labels:
                service["labels"] = container.labels

            # Add resource limits
            if container.cpu_limit or container.memory_limit:
                service["deploy"] = {"resources": {"limits": {}}}
                if container.cpu_limit:
                    service["deploy"]["resources"]["limits"]["cpus"] = str(
                        container.cpu_limit
                    )
                if container.memory_limit:
                    service["deploy"]["resources"]["limits"]["memory"] = (
                        container.memory_limit
                    )

            # Add health check with comprehensive support
            if container.health_check and self.config.include_healthchecks:
                healthcheck = self._export_health_check(container.health_check)
                if healthcheck:
                    service["healthcheck"] = healthcheck

            compose["services"][service_name] = service

        # Add networks section
        if used_networks and self.config.include_networks:
            compose["networks"] = {}
            for network_name in used_networks:
                compose["networks"][network_name] = {
                    "driver": "bridge",
                    "name": f"{self.config.namespace}_{network_name}",
                }

        # Add volumes section
        if used_volumes and self.config.include_volumes:
            compose["volumes"] = {}
            for volume_name in used_volumes:
                compose["volumes"][volume_name] = {
                    "driver": "local",
                    "name": f"{self.config.namespace}_{volume_name}",
                }

        # Add secrets section (placeholder for future enhancement)
        if used_secrets and self.config.include_secrets:
            compose["secrets"] = {}
            for secret_name in used_secrets:
                compose["secrets"][secret_name] = {
                    "external": True,
                    "name": f"{self.config.namespace}_{secret_name}",
                }

        # Add configs section (placeholder for future enhancement)
        if used_configs:
            compose["configs"] = {}
            for config_name in used_configs:
                compose["configs"][config_name] = {
                    "external": True,
                    "name": f"{self.config.namespace}_{config_name}",
                }

        return compose

    def _export_port_specification(self, port: Any) -> str | dict[str, Any]:
        """Export port specification with comprehensive format support."""
        if hasattr(port, "host_port") and hasattr(port, "container_port"):
            # PortMapping object
            if port.host_port is None:
                # Container port only
                if port.protocol != "tcp":
                    return f"{port.container_port}/{port.protocol}"
                return str(port.container_port)
            elif port.host_ip and port.host_ip != "0.0.0.0":
                # Host IP:host port:container port format
                if port.protocol != "tcp":
                    return f"{port.host_ip}:{port.host_port}:{port.container_port}/{port.protocol}"
                return f"{port.host_ip}:{port.host_port}:{port.container_port}"
            else:
                # Host port:container port format
                if port.protocol != "tcp":
                    return f"{port.host_port}:{port.container_port}/{port.protocol}"
                return f"{port.host_port}:{port.container_port}"
        else:
            # Fallback for other port formats
            return str(port)

    def _format_volume_for_compose(self, volume: VolumeMount) -> str:
        """Format a volume mount for Docker Compose."""
        if volume.type == "bind":
            # Bind mount: /host/path:/container/path[:ro]
            volume_str = f"{volume.source}:{volume.target}"
            if volume.read_only:
                volume_str += ":ro"
            # Don't add :rw for read-write as it's the default
            return volume_str
        else:
            # Named volume: volume_name:/container/path[:ro]
            volume_str = f"{volume.source}:{volume.target}"
            if volume.read_only:
                volume_str += ":ro"
            # Don't add :rw for read-write as it's the default
            return volume_str

    def _map_restart_policy(self, policy: RestartPolicy) -> str:
        """Map RestartPolicy to Docker Compose restart value."""
        policy_map: dict[RestartPolicy, str] = {
            RestartPolicy.ALWAYS: "always",
            RestartPolicy.UNLESS_STOPPED: "unless-stopped",
            RestartPolicy.ON_FAILURE: "on-failure",
            RestartPolicy.NO: "no",
        }
        return policy_map.get(policy, "unless-stopped")

    def _export_health_check(self, health_check: HealthCheck) -> dict[str, Any] | None:
        """Export health check configuration with comprehensive support."""
        if not health_check:
            return None

        healthcheck: dict[str, Any] = {}

        # Handle test command - ensure proper format for Docker Compose
        if health_check.test:
            if isinstance(health_check.test, str):
                # String format - wrap in CMD-SHELL
                healthcheck["test"] = ["CMD-SHELL", health_check.test]
            elif isinstance(health_check.test, list):
                # List format - add CMD prefix if not present
                if health_check.test and health_check.test[0] not in [
                    "CMD",
                    "CMD-SHELL",
                ]:
                    healthcheck["test"] = ["CMD"] + health_check.test
                else:
                    healthcheck["test"] = health_check.test
            else:
                return None

        # Convert timedelta objects to duration strings
        if health_check.interval:
            healthcheck["interval"] = self._timedelta_to_duration_string(
                health_check.interval
            )

        if health_check.timeout:
            healthcheck["timeout"] = self._timedelta_to_duration_string(
                health_check.timeout
            )

        if health_check.start_period:
            healthcheck["start_period"] = self._timedelta_to_duration_string(
                health_check.start_period
            )

        if health_check.retries:
            healthcheck["retries"] = health_check.retries

        return healthcheck if healthcheck else None

    def _timedelta_to_duration_string(self, td: timedelta) -> str:
        """Convert timedelta to Docker Compose duration string format (e.g., '30s', '1m30s', '1h')."""
        if not td:
            return "0s"

        total_seconds: int = int(td.total_seconds())

        if total_seconds == 0:
            return "0s"

        # Calculate hours, minutes, and seconds
        hours: int = total_seconds // 3600
        minutes: int = (total_seconds % 3600) // 60
        seconds: int = total_seconds % 60

        # Build duration string
        parts: list[str] = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:  # Always include seconds if no other parts
            parts.append(f"{seconds}s")

        return "".join(parts)


class KubernetesExporter(BaseExporter):
    """Export Constellation containers to Kubernetes manifests."""

    def export(
        self,
        containers: list[ContainerConfig],
        services: list[ConstellationService] | None = None,
    ) -> Path:
        """Export to Kubernetes manifests."""
        # This is a placeholder implementation
        # In a real implementation, this would generate proper Kubernetes YAML
        k8s_file: Path = self.output_dir / "kubernetes.yaml"

        manifests: list[dict[str, Any]] = []
        for container in containers:
            # Create a basic Deployment manifest
            deployment: dict[str, Any] = {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": self._sanitize_name(container.name),
                    "namespace": self.config.namespace,
                },
                "spec": {
                    "replicas": 1,
                    "selector": {
                        "matchLabels": {
                            "app": self._sanitize_name(container.name),
                        }
                    },
                    "template": {
                        "metadata": {
                            "labels": {
                                "app": self._sanitize_name(container.name),
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": self._sanitize_name(container.name),
                                    "image": f"{container.image}:{container.tag}",
                                    "env": [
                                        {"name": k, "value": v}
                                        for k, v in container.environment.items()
                                    ],
                                }
                            ]
                        },
                    },
                },
            }
            manifests.append(deployment)

        with k8s_file.open("w", encoding="utf-8") as f:
            yaml.dump_all(manifests, f, default_flow_style=False)

        return k8s_file


class NomadExporter(BaseExporter):
    """Export Constellation containers to Nomad job specifications."""

    def export(
        self,
        containers: list[ContainerConfig],
        services: list[ConstellationService] | None = None,
    ) -> Path:
        """Export to Nomad job specification."""
        # This is a placeholder implementation
        # In a real implementation, this would generate proper Nomad HCL
        nomad_file: Path = self.output_dir / "nomad.hcl"

        job_spec: dict[str, Any] = {
            "job": {
                self.config.chart_name: {
                    "datacenters": ["dc1"],
                    "type": "service",
                    "group": {
                        "app": {
                            "count": 1,
                            "task": {},
                        }
                    },
                }
            }
        }

        # Add tasks for each container
        for container in containers:
            task_spec: dict[str, Any] = {
                "driver": "docker",
                "config": {
                    "image": f"{container.image}:{container.tag}",
                    "port_map": {
                        f"port_{port.container_port}": port.container_port
                        for port in container.ports
                    },
                },
                "env": container.environment,
                "resources": {
                    "cpu": int(container.cpu_limit * 1000)
                    if container.cpu_limit
                    else 500,
                    "memory": 256,  # Default memory
                    "network": {
                        "port": [
                            {f"port_{port.container_port}": {"static": port.host_port}}
                            for port in container.ports
                            if port.host_port
                        ]
                    },
                },
            }

            job_spec["job"][self.config.chart_name]["group"]["app"]["task"][container.name] = task_spec

        with nomad_file.open("w", encoding="utf-8") as f:
            yaml.dump(job_spec, f, default_flow_style=False)

        return nomad_file


class ExportManager:
    """High-level export manager for various container formats."""

    def __init__(self, config: ExportConfig):
        self.config: ExportConfig = config
        self.exporters: dict[str, BaseExporter] = {
            "compose": DockerComposeExporter(config),
            "kubernetes": KubernetesExporter(config),
            "nomad": NomadExporter(config),
        }

    def export(
        self,
        containers: list[ContainerConfig],
        format: str,
        services: list[ConstellationService] | None = None,
    ) -> Path:
        """Export containers to the specified format."""
        if format not in self.exporters:
            raise ValueError(f"Unsupported export format: {format}")

        exporter: BaseExporter = self.exporters[format]
        return exporter.export(containers, services)

    def export_all(
        self,
        containers: list[ContainerConfig],
        services: list[ConstellationService] | None = None,
    ) -> dict[str, Path]:
        """Export containers to all supported formats."""
        results: dict[str, Path] = {}
        for format_name, exporter in self.exporters.items():
            results[format_name] = exporter.export(containers, services)
        return results

    def get_supported_formats(self) -> list[str]:
        """Get list of supported export formats."""
        return list(self.exporters.keys())


def export_from_constellation_config(
    config_path: Path,
    output_dir: Path,
    formats: list[str] | None = None,
    **kwargs: Any,
) -> dict[str, Path]:
    """Export from a Constellation configuration file."""
    # This would load a constellation config and export
    # For now, this is a placeholder
    export_config = ExportConfig(output_dir=output_dir, **kwargs)
    manager = ExportManager(export_config)

    # Mock containers for demonstration
    containers: list[ContainerConfig] = []

    if formats:
        results: dict[str, Path] = {}
        for fmt in formats:
            results[fmt] = manager.export(containers, fmt)
        return results
    else:
        return manager.export_all(containers)


def validate_export_config(config: ExportConfig) -> list[str]:
    """Validate export configuration and return any issues."""
    issues: list[str] = []

    if not config.output_dir:
        issues.append("Output directory is required")
    elif not config.output_dir.exists():
        try:
            config.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create output directory: {e}")

    if not config.namespace or not config.namespace.strip():
        issues.append("Namespace is required")

    if not config.chart_name or not config.chart_name.strip():
        issues.append("Chart name is required")

    return issues
