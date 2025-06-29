"""Import Manager for Constellation.

This module provides functionality to import container specifications from
Docker Compose files and convert them to Constellation container configurations.
"""

from __future__ import annotations

import os
import re
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from constellation.core.container import (
    ContainerConfig,
    HealthCheck,
    PortMapping,
    RestartPolicy,
    VolumeMount,
)

if TYPE_CHECKING:
    pass


class DockerComposeImporter:
    """Import from Docker Compose files."""

    def import_from_file(self, file: Path) -> list[ContainerConfig]:
        """Import containers from Docker Compose file."""
        if not file.exists():
            raise FileNotFoundError(f"Docker Compose file not found: {file}")

        try:
            with file.open("r", encoding="utf-8") as f:
                compose_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in Docker Compose file: {e.__class__.__name__}: {e}")  # noqa: B904
        except Exception as e:
            raise ValueError(f"Error reading Docker Compose file: {e.__class__.__name__}: {e}")  # noqa: B904

        if not isinstance(compose_data, dict):
            raise ValueError("Docker Compose file must contain a dictionary")

        containers: list[ContainerConfig] = []
        services: dict[str, Any] = compose_data.get("services", {})
        networks: dict[str, Any] = compose_data.get("networks", {})
        volumes: dict[str, Any] = compose_data.get("volumes", {})
        secrets: dict[str, Any] = compose_data.get("secrets", {})
        configs: dict[str, Any] = compose_data.get("configs", {})

        for service_name, service_config in services.items():
            container = self._convert_service_to_container(
                service_name, service_config, networks, volumes, secrets, configs
            )
            containers.append(container)

        return containers

    def _convert_service_to_container(
        self,
        name: str,
        service: dict[str, Any],
        networks: dict[str, Any],
        volumes: dict[str, Any],
        secrets: dict[str, Any],
        configs: dict[str, Any],
    ) -> ContainerConfig:
        """Convert a Docker Compose service to ContainerConfig."""
        # Parse image and tag - handle SHA256 digests
        image_full: str = service.get("image", f"{name}:latest")
        if "@sha256:" in image_full:
            # Handle SHA256 digest format: image@sha256:hash
            if ":" in image_full and not image_full.startswith("sha256:"):
                image, tag = image_full.rsplit(":", 1)
                if "@sha256:" in tag:
                    image, tag = image_full.split("@", 1)
            else:
                image, tag = image_full, "latest"
        elif ":" in image_full and not image_full.startswith("sha256:"):
            image, tag = image_full.rsplit(":", 1)
        else:
            image, tag = image_full, "latest"

        # Parse ports with comprehensive support
        ports: list[PortMapping] = []
        for port_spec in service.get("ports", []):
            parsed_ports: list[PortMapping] = self._parse_port_specification(port_spec)
            ports.extend(parsed_ports)

        # Parse volumes with comprehensive support
        volume_mounts: list[VolumeMount] = []
        for volume_spec in service.get("volumes", []):
            mount: VolumeMount | None = self._parse_volume_specification(volume_spec, volumes)
            if mount:
                volume_mounts.append(mount)

        # Parse environment variables with comprehensive support
        environment: dict[str, str] = {}
        env_config: dict[str, Any] | list[str] = service.get("environment", {})
        if isinstance(env_config, list):
            for env_var in env_config:
                if "=" in env_var:
                    key, value = env_var.split("=", 1)
                    environment[key] = value
                else:
                    # Environment variable without value - inherit from host
                    environment[env_var] = os.getenv(env_var, "")
        elif isinstance(env_config, dict):
            environment = {k: str(v) if v is not None else "" for k, v in env_config.items()}

        # Parse restart policy
        restart_policy: RestartPolicy = self._parse_restart_policy(service.get("restart", "unless-stopped"))

        # Parse health check
        health_check: HealthCheck | None = self._parse_health_check(service.get("healthcheck"))

        # Parse command and entrypoint
        command: str | None = service.get("command")
        if isinstance(command, list):
            command = " ".join(command)

        entrypoint: str | None = service.get("entrypoint")
        if isinstance(entrypoint, list):
            entrypoint = " ".join(entrypoint)

        # Parse resource limits
        deploy_config: dict[str, Any] = service.get("deploy", {})
        resources: dict[str, Any] = deploy_config.get("resources", {})
        limits: dict[str, Any] = resources.get("limits", {})

        cpu_limit = None
        memory_limit = None

        if "cpus" in limits:
            cpu_limit = float(limits["cpus"])

        if "memory" in limits:
            memory_limit = limits["memory"]

        # Parse labels - handle both list and dict formats
        labels: dict[str, str] = {}
        labels_config = service.get("labels", {})
        if isinstance(labels_config, list):
            # Convert list format to dict
            for label in labels_config:
                if "=" in label:
                    key, value = label.split("=", 1)
                    labels[key] = value
                else:
                    labels[label] = ""
        elif isinstance(labels_config, dict):
            labels = {k: str(v) for k, v in labels_config.items()}

        return ContainerConfig(
            name=service.get("container_name", name),
            image=image,
            tag=tag,
            hostname=service.get("hostname"),
            ports=ports,
            environment=environment,
            command=command,
            entrypoint=entrypoint,
            working_dir=service.get("working_dir"),
            user=service.get("user"),
            volumes=volume_mounts,
            restart_policy=restart_policy,
            depends_on=service.get("depends_on", []),
            labels=labels,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            health_check=health_check,
        )

    def _parse_port_specification(self, port_spec: Any) -> list[PortMapping]:
        """Parse Docker Compose port specification with comprehensive support."""
        ports: list[PortMapping] = []

        if isinstance(port_spec, str):
            # Handle string format: "host:container", "host_ip:host:container", or "container"
            host_ip: str = "0.0.0.0"
            host_port: int | None = None
            container_port: int | str = port_spec
            protocol: str = "tcp"

            # Handle protocol specification first
            if "/" in port_spec:
                port_part, protocol = port_spec.rsplit("/", 1)
            else:
                port_part = port_spec

            # Parse port mapping
            if ":" in port_part:
                parts: list[str] = port_part.split(":")
                if len(parts) == 2:
                    # host_port:container_port
                    host_port, container_port = parts
                elif len(parts) == 3:
                    # host_ip:host_port:container_port
                    host_ip, host_port, container_port = parts
            else:
                # Just container port
                container_port = port_part
                host_port = None

            ports.append(
                PortMapping(
                    host_port=int(host_port) if host_port else None,
                    container_port=int(container_port),
                    protocol=protocol,
                    host_ip=host_ip,
                )
            )

        elif isinstance(port_spec, dict):
            # Handle long format with comprehensive support
            ports.append(
                PortMapping(
                    host_port=port_spec.get("published"),
                    container_port=port_spec["target"],
                    protocol=port_spec.get("protocol", "tcp"),
                    host_ip=port_spec.get("host_ip", "0.0.0.0"),
                )
            )
        elif isinstance(port_spec, int):
            # Handle bare port number
            ports.append(
                PortMapping(
                    container_port=port_spec,
                    protocol="tcp",
                    host_ip="0.0.0.0",
                )
            )

        return ports

    def _parse_volume_specification(
        self,
        volume_spec: str | dict[str, Any],
        volumes: dict[str, Any],
    ) -> VolumeMount | None:
        """Parse Docker Compose volume specification with comprehensive support."""
        if isinstance(volume_spec, str):
            # Handle string format: "source:target" or "source:target:mode"
            parts: list[str] = volume_spec.split(":")
            if len(parts) < 2:
                # Invalid volume specification
                return None

            source: str = parts[0]
            target: str = parts[1]
            read_only: bool = False

            # Check for read-only mode
            if len(parts) > 2:
                mode: str = parts[2]
                read_only = "ro" in mode

            # Determine volume type - bind vs named volume
            volume_type: str = "bind" if source.startswith("/") or source.startswith(".") else "volume"

            return VolumeMount(
                source=source,
                target=target,
                type=volume_type,
                read_only=read_only,
            )

        elif isinstance(volume_spec, dict):
            # Handle long format with comprehensive support
            return VolumeMount(
                source=volume_spec.get("source", ""),
                target=volume_spec["target"],
                type=volume_spec.get("type", "volume"),
                read_only=volume_spec.get("read_only", False),
                consistency=volume_spec.get("consistency"),
            )

        return None

    def _parse_restart_policy(
        self,
        restart_config: str | None,
    ) -> RestartPolicy:
        """Parse Docker Compose restart policy."""
        if restart_config == "always":
            return RestartPolicy.ALWAYS
        elif restart_config == "on-failure":
            return RestartPolicy.ON_FAILURE
        elif restart_config == "no":
            return RestartPolicy.NO
        else:
            return RestartPolicy.UNLESS_STOPPED

    def _parse_health_check(self, healthcheck: dict[str, Any] | None) -> HealthCheck | None:
        """Parse Docker Compose health check configuration with comprehensive support."""
        if not healthcheck:
            return None

        # Check for disabled health check
        if healthcheck.get("disable", False):
            return None

        test: list[str] | str | None = healthcheck.get("test", [])
        if isinstance(test, str):
            # For string format, keep as string
            test_cmd: str | list[str] = test
        elif isinstance(test, list) and len(test) > 0:
            # If test starts with CMD or CMD-SHELL, remove the prefix for our format
            if test[0] in ["CMD", "CMD-SHELL"]:
                test_cmd = test[1:]  # Remove the CMD/CMD-SHELL prefix
            else:
                # Otherwise, assume it's a command list
                test_cmd: str | list[str] = test
        else:
            return None

        interval: timedelta = self._parse_duration(healthcheck.get("interval", "30s"))
        timeout: timedelta = self._parse_duration(healthcheck.get("timeout", "30s"))
        start_period: timedelta = self._parse_duration(healthcheck.get("start_period", "0s"))
        retries: int = healthcheck.get("retries", 3)

        return HealthCheck(
            test=test_cmd,
            interval=interval,
            timeout=timeout,
            retries=retries,
            start_period=start_period,
        )

    def _parse_duration(self, duration_str: str) -> timedelta:
        """Parse Docker Compose duration string (e.g., '30s', '1m30s', '1h') with comprehensive support."""
        if isinstance(duration_str, (int, float)):
            return timedelta(seconds=duration_str)

        duration_str = str(duration_str).strip()
        if not duration_str:
            return timedelta(seconds=0)

        # Regular expression to parse duration components
        pattern = r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s?)?$"
        match: re.Match[str] | None = re.match(pattern, duration_str)

        if not match:
            # Fallback: try to parse as plain number (seconds)
            try:
                return timedelta(seconds=float(duration_str))
            except ValueError:
                return timedelta(seconds=30)  # Default fallback

        hours, minutes, seconds = match.groups()
        total_seconds: int = 0

        if hours:
            total_seconds += int(hours) * 3600
        if minutes:
            total_seconds += int(minutes) * 60
        if seconds:
            total_seconds += float(seconds)

        return timedelta(seconds=total_seconds)


class ImportManager:
    """High-level import manager for various container formats."""

    def __init__(self):
        self.compose_importer = DockerComposeImporter()

    def import_from_compose(self, file_path: Path) -> list[ContainerConfig]:
        """Import containers from Docker Compose file."""
        return self.compose_importer.import_from_file(file_path)

    def validate_compose_file(self, file_path: Path) -> bool:
        """Validate that a Docker Compose file is valid and can be imported."""
        try:
            self.import_from_compose(file_path)
            return True
        except Exception:
            return False

    def get_compose_services(self, file_path: Path) -> list[str]:
        """Get list of service names from a Docker Compose file."""
        try:
            with file_path.open("r", encoding="utf-8") as f:
                compose_data: dict[str, Any] = yaml.safe_load(f)
            return list(compose_data.get("services", {}).keys())
        except Exception:
            return []
