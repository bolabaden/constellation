"""Tests for Docker Compose import/export functionality."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from constellation.core.container import (
    ContainerConfig,
    HealthCheck,
    PortMapping,
    RestartPolicy,
    VolumeMount,
)
from constellation.orchestration.export_manager import ExportConfig, ExportManager
from constellation.orchestration.import_manager import ImportManager


class TestDockerComposeImport:
    """Test Docker Compose import functionality."""

    def test_basic_service_import(self, tmp_path: Path):
        """Test importing a basic Docker Compose service."""
        compose_content = {
            "version": "3.8",
            "services": {
                "web": {
                    "image": "nginx:latest",
                    "ports": ["80:80"],
                    "environment": {"ENV_VAR": "value"},
                    "restart": "unless-stopped",
                }
            },
        }

        compose_file = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()
        containers = import_manager.import_from_compose(compose_file)

        assert len(containers) == 1
        container = containers[0]
        assert container.name == "web"
        assert container.image == "nginx"
        assert container.tag == "latest"
        assert len(container.ports) == 1
        assert container.ports[0].host_port == 80
        assert container.ports[0].container_port == 80
        assert container.environment["ENV_VAR"] == "value"
        assert container.restart_policy == RestartPolicy.UNLESS_STOPPED

    def test_complex_service_import(self, tmp_path: Path):
        """Test importing a complex Docker Compose service with all features."""
        compose_content = {
            "version": "3.8",
            "services": {
                "app": {
                    "image": "myapp:v1.2.3",
                    "container_name": "my-app-container",
                    "hostname": "app-host",
                    "ports": [
                        "8080:80",
                        "9090:9090/udp",
                        {"target": 3000, "published": 3000, "protocol": "tcp"},
                    ],
                    "environment": {
                        "DATABASE_URL": "postgres://localhost/db",
                        "DEBUG": "true",
                    },
                    "volumes": [
                        "/host/data:/app/data",
                        "named-volume:/app/storage",
                        "/host/config:/app/config:ro",
                    ],
                    "command": ["python", "app.py"],
                    "entrypoint": ["/entrypoint.sh"],
                    "working_dir": "/app",
                    "user": "1000:1000",
                    "depends_on": ["db", "redis"],
                    "restart": "always",
                    "labels": {
                        "app.name": "myapp",
                        "app.version": "1.2.3",
                    },
                    "healthcheck": {
                        "test": ["CMD", "curl", "-f", "http://localhost/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                        "start_period": "60s",
                    },
                    "deploy": {
                        "resources": {
                            "limits": {
                                "cpus": "0.5",
                                "memory": "512M",
                            }
                        }
                    },
                }
            },
            "volumes": {
                "named-volume": {},
            },
        }

        compose_file: Path = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(compose_file)

        assert len(containers) == 1
        container = containers[0]

        # Basic properties
        assert container.name == "my-app-container"
        assert container.image == "myapp"
        assert container.tag == "v1.2.3"
        assert container.hostname == "app-host"

        # Ports
        assert len(container.ports) == 3
        port_map: dict[int, PortMapping] = {p.container_port: p for p in container.ports}
        assert port_map[80].host_port == 8080
        assert port_map[80].protocol == "tcp"
        assert port_map[9090].host_port == 9090
        assert port_map[9090].protocol == "udp"
        assert port_map[3000].host_port == 3000
        assert port_map[3000].protocol == "tcp"

        # Environment
        assert container.environment["DATABASE_URL"] == "postgres://localhost/db"
        assert container.environment["DEBUG"] == "true"

        # Volumes
        assert len(container.volumes) == 3
        volume_map: dict[str, VolumeMount] = {v.target: v for v in container.volumes}
        assert volume_map["/app/data"].source == "/host/data"
        assert volume_map["/app/data"].type == "bind"
        assert not volume_map["/app/data"].read_only
        assert volume_map["/app/storage"].source == "named-volume"
        assert volume_map["/app/storage"].type == "volume"
        assert volume_map["/app/config"].read_only

        # Command and entrypoint
        assert container.command == "python app.py"
        assert container.entrypoint == "/entrypoint.sh"

        # Other properties
        assert container.working_dir == "/app"
        assert container.user == "1000:1000"
        assert container.depends_on == ["db", "redis"]
        assert container.restart_policy == RestartPolicy.ALWAYS

        # Labels
        assert container.labels["app.name"] == "myapp"
        assert container.labels["app.version"] == "1.2.3"

        # Resource limits
        assert container.cpu_limit == 0.5
        assert container.memory_limit == "512M"

        # Health check
        assert container.health_check is not None
        assert container.health_check.test == ["curl", "-f", "http://localhost/health"]
        assert container.health_check.retries == 3

    def test_port_parsing_variations(self, tmp_path: Path):
        """Test various port specification formats."""
        compose_content = {
            "version": "3.8",
            "services": {
                "web": {
                    "image": "nginx:latest",
                    "ports": [
                        "80",  # Container port only
                        "8080:8080",  # Host:container (different ports to avoid conflict)
                        "127.0.0.1:9090:90",  # Host IP:host port:container port
                        "443:443/tcp",  # With protocol
                        "53:53/udp",  # UDP protocol
                        {
                            "target": 3000,
                            "published": 3000,
                            "protocol": "tcp",
                        },  # Long format
                    ],
                }
            },
        }

        compose_file: Path = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(compose_file)

        container = containers[0]
        assert len(container.ports) == 6

        # Check specific port configurations
        port_map: dict[int, PortMapping] = {p.container_port: p for p in container.ports}

        # Container port only
        assert port_map[80].host_port is None
        assert port_map[80].protocol == "tcp"

        # Host:container
        assert port_map[8080].host_port == 8080
        assert port_map[8080].protocol == "tcp"

        # Host IP:host port:container port
        assert port_map[90].host_port == 9090
        assert port_map[90].host_ip == "127.0.0.1"

        # With protocols
        assert port_map[443].protocol == "tcp"
        assert port_map[53].protocol == "udp"

        # Long format
        assert port_map[3000].host_port == 3000
        assert port_map[3000].protocol == "tcp"

    def test_environment_parsing_variations(self, tmp_path: Path):
        """Test various environment variable formats."""
        compose_content = {
            "version": "3.8",
            "services": {
                "app": {
                    "image": "app:latest",
                    "environment": [
                        "VAR1=value1",
                        "VAR2=value with spaces",
                        "VAR3=",  # Empty value
                        "VAR4",  # No value (should get from env)
                    ],
                },
                "app2": {
                    "image": "app:latest",
                    "environment": {
                        "DICT_VAR1": "dict_value1",
                        "DICT_VAR2": 42,
                        "DICT_VAR3": None,
                    },
                },
            },
        }

        compose_file: Path = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(compose_file)

        assert len(containers) == 2

        # List format
        app1 = next(c for c in containers if c.name == "app")
        assert app1.environment["VAR1"] == "value1"
        assert app1.environment["VAR2"] == "value with spaces"
        assert app1.environment["VAR3"] == ""
        assert "VAR4" in app1.environment  # Should be empty string from os.getenv

        # Dict format
        app2 = next(c for c in containers if c.name == "app2")
        assert app2.environment["DICT_VAR1"] == "dict_value1"
        assert app2.environment["DICT_VAR2"] == "42"
        assert app2.environment["DICT_VAR3"] == ""

    def test_healthcheck_parsing(self, tmp_path: Path):
        """Test health check parsing with various formats."""
        compose_content = {
            "version": "3.8",
            "services": {
                "app1": {
                    "image": "app:latest",
                    "healthcheck": {
                        "test": ["CMD", "curl", "-f", "http://localhost/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                        "start_period": "1m",
                    },
                },
                "app2": {
                    "image": "app:latest",
                    "healthcheck": {
                        "test": "curl -f http://localhost/health",
                        "interval": "1m30s",
                    },
                },
                "app3": {
                    "image": "app:latest",
                    "healthcheck": {"disable": True},
                },
            },
        }

        compose_file: Path = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(compose_file)

        app1 = next(c for c in containers if c.name == "app1")
        assert app1.health_check is not None
        assert app1.health_check.test == ["curl", "-f", "http://localhost/health"]
        assert app1.health_check.retries == 3

        app2 = next(c for c in containers if c.name == "app2")
        assert app2.health_check is not None
        assert app2.health_check.test == "curl -f http://localhost/health"

        app3 = next(c for c in containers if c.name == "app3")
        assert app3.health_check is None

    def test_invalid_file_handling(self, tmp_path: Path):
        """Test handling of invalid files and error cases."""
        import_manager = ImportManager()

        # Non-existent file
        with pytest.raises(FileNotFoundError):
            import_manager.import_from_compose(tmp_path / "nonexistent.yml")

        # Invalid YAML
        invalid_yaml_file = tmp_path / "invalid.yml"
        with invalid_yaml_file.open("w", encoding="utf-8") as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(ValueError, match="Invalid YAML"):
            import_manager.import_from_compose(invalid_yaml_file)

        # Not a dictionary
        invalid_content_file = tmp_path / "invalid_content.yml"
        with invalid_content_file.open("w", encoding="utf-8") as f:
            yaml.dump(["not", "a", "dict"], f)

        with pytest.raises(ValueError, match="must contain a dictionary"):
            import_manager.import_from_compose(invalid_content_file)

    def test_validation_methods(self, tmp_path: Path):
        """Test validation and utility methods."""
        compose_content = {
            "version": "3.8",
            "services": {
                "web": {"image": "nginx:latest"},
                "db": {"image": "postgres:13"},
            },
        }

        compose_file: Path = tmp_path / "docker-compose.yml"
        with compose_file.open("w", encoding="utf-8") as f:
            yaml.dump(compose_content, f)

        import_manager = ImportManager()

        # Test validation
        assert import_manager.validate_compose_file(compose_file)

        # Test service listing
        services: list[str] = import_manager.get_compose_services(compose_file)
        assert set(services) == {"web", "db"}

        # Test validation with invalid file
        invalid_file = tmp_path / "invalid.yml"
        with invalid_file.open("w", encoding="utf-8") as f:
            f.write("invalid yaml [")

        assert not import_manager.validate_compose_file(invalid_file)
        assert import_manager.get_compose_services(invalid_file) == []


class TestDockerComposeExport:
    """Test Docker Compose export functionality."""

    def test_basic_container_export(self, tmp_path: Path):
        """Test exporting a basic container configuration."""
        container = ContainerConfig(
            name="test-container",
            image="nginx",
            tag="latest",
            ports=[PortMapping(host_port=8080, container_port=80, protocol="tcp")],
            environment={"ENV_VAR": "value"},
            restart_policy=RestartPolicy.UNLESS_STOPPED,
        )

        export_config = ExportConfig(output_dir=tmp_path)
        export_manager = ExportManager(export_config)

        result_file: Path = export_manager.export([container], "compose")

        assert result_file.exists()
        with result_file.open("r", encoding="utf-8") as f:
            exported_compose = yaml.safe_load(f)

        assert exported_compose["version"] == "3.8"
        assert "test-container" in exported_compose["services"]

        service = exported_compose["services"]["test-container"]
        assert service["image"] == "nginx:latest"
        assert service["container_name"] == "test-container"
        assert service["ports"] == ["8080:80"]
        assert service["environment"]["ENV_VAR"] == "value"
        assert service["restart"] == "unless-stopped"

    def test_complex_container_export(self, tmp_path: Path):
        """Test exporting a complex container with all features."""
        from datetime import timedelta

        health_check = HealthCheck(
            test=["curl", "-f", "http://localhost/health"],
            interval=timedelta(seconds=30),
            timeout=timedelta(seconds=10),
            retries=3,
            start_period=timedelta(minutes=1),
        )

        container = ContainerConfig(
            name="complex-app",
            image="myapp",
            tag="v1.0.0",
            hostname="app-host",
            ports=[
                PortMapping(host_port=8080, container_port=80, protocol="tcp"),
                PortMapping(host_port=9090, container_port=9090, protocol="udp"),
                PortMapping(container_port=3000, protocol="tcp"),  # No host port
            ],
            environment={
                "DATABASE_URL": "postgres://localhost/db",
                "DEBUG": "true",
            },
            volumes=[
                VolumeMount(source="/host/data", target="/app/data", type="bind"),
                VolumeMount(source="named-vol", target="/app/storage", type="volume"),
                VolumeMount(
                    source="/host/config",
                    target="/app/config",
                    type="bind",
                    read_only=True,
                ),
            ],
            command="python app.py",
            entrypoint="/entrypoint.sh",
            working_dir="/app",
            user="1000:1000",
            depends_on=["db", "redis"],
            restart_policy=RestartPolicy.ALWAYS,
            labels={
                "app.name": "myapp",
                "app.version": "1.0.0",
            },
            cpu_limit=0.5,
            memory_limit="512M",
            health_check=health_check,
        )

        export_config = ExportConfig(output_dir=tmp_path)
        export_manager = ExportManager(export_config)

        result_file: Path = export_manager.export([container], "compose")

        with result_file.open("r", encoding="utf-8") as f:
            exported_compose = yaml.safe_load(f)

        service = exported_compose["services"]["complex-app"]

        # Basic properties
        assert service["image"] == "myapp:v1.0.0"
        assert service["container_name"] == "complex-app"
        assert service["hostname"] == "app-host"

        # Ports
        expected_ports: list[str] = ["8080:80", "9090:9090/udp", "3000"]
        assert service["ports"] == expected_ports

        # Environment
        assert service["environment"]["DATABASE_URL"] == "postgres://localhost/db"
        assert service["environment"]["DEBUG"] == "true"

        # Volumes
        expected_volumes: list[str] = [
            "/host/data:/app/data",
            "named-vol:/app/storage",
            "/host/config:/app/config:ro",
        ]
        assert service["volumes"] == expected_volumes

        # Command and entrypoint (should be split into lists)
        assert service["command"] == ["python", "app.py"]
        assert service["entrypoint"] == ["/entrypoint.sh"]

        # Other properties
        assert service["working_dir"] == "/app"
        assert service["user"] == "1000:1000"
        assert service["depends_on"] == ["db", "redis"]
        assert service["restart"] == "always"

        # Labels
        assert service["labels"]["app.name"] == "myapp"
        assert service["labels"]["app.version"] == "1.0.0"

        # Resource limits
        assert service["deploy"]["resources"]["limits"]["cpus"] == "0.5"
        assert service["deploy"]["resources"]["limits"]["memory"] == "512M"

        # Health check
        healthcheck: dict[str, Any] = service["healthcheck"]
        assert healthcheck["test"] == ["CMD", "curl", "-f", "http://localhost/health"]
        assert healthcheck["interval"] == "30s"
        assert healthcheck["timeout"] == "10s"
        assert healthcheck["retries"] == 3
        assert healthcheck["start_period"] == "1m"

        # Networks and volumes sections
        assert "networks" in exported_compose
        assert "constellation" in exported_compose["networks"]
        assert "volumes" in exported_compose
        assert "named-vol" in exported_compose["volumes"]

    def test_port_export_variations(self, tmp_path: Path):
        """Test exporting various port configurations."""
        container = ContainerConfig(
            name="port-test",
            image="nginx",
            tag="latest",
            hostname="test-host",
            command=["nginx", "-g", "daemon off;"],
            entrypoint=["/bin/sh", "-c"],
            working_dir="/app",
            user="1000:1000",
            cpu_limit=0.5,
            memory_limit="512M",
            health_check=HealthCheck(
                test=["CMD", "curl", "-f", "http://localhost/health"],
                interval=timedelta(seconds=30),
                timeout=timedelta(seconds=10),
                retries=3,
            ),
            ports=[
                PortMapping(container_port=80, protocol="tcp"),  # No host port
                PortMapping(host_port=8080, container_port=80, protocol="tcp"),
                PortMapping(host_port=9090, container_port=9090, protocol="udp"),
                PortMapping(
                    host_port=3000,
                    container_port=3000,
                    protocol="tcp",
                    host_ip="127.0.0.1",
                ),
            ],
        )

        export_config = ExportConfig(output_dir=tmp_path)
        export_manager = ExportManager(export_config)

        result_file: Path = export_manager.export([container], "compose")

        with result_file.open("r", encoding="utf-8") as f:
            exported_compose = yaml.safe_load(f)

        ports: list[str] = exported_compose["services"]["port-test"]["ports"]
        expected_ports: list[str] = ["80", "8080:80", "9090:9090/udp", "127.0.0.1:3000:3000"]
        assert ports == expected_ports

    def test_export_multiple_formats(self, tmp_path: Path):
        """Test exporting to multiple formats."""
        container = ContainerConfig(
            name="test-app",
            image="nginx",
            tag="latest",
            hostname="test-host",
            command=["nginx", "-g", "daemon off;"],
            entrypoint=["/bin/sh", "-c"],
            working_dir="/app",
            user="1000:1000",
            cpu_limit=0.5,
            memory_limit="512M",
            health_check=HealthCheck(
                test=["CMD", "curl", "-f", "http://localhost/health"],
                interval=timedelta(seconds=30),
                timeout=timedelta(seconds=10),
                retries=3,
            ),
        )

        export_config = ExportConfig(output_dir=tmp_path)
        export_manager = ExportManager(export_config)

        # Test getting supported formats
        formats: list[str] = export_manager.get_supported_formats()
        assert "compose" in formats
        assert "kubernetes" in formats
        assert "nomad" in formats

        # Test export all
        results = export_manager.export_all([container])

        assert "compose" in results
        assert results["compose"].exists()
        assert results["compose"].name == "docker-compose.yml"

        # Other formats should exist but be placeholders for now
        assert "kubernetes" in results
        assert "nomad" in results

    def test_export_config_validation(self):
        """Test export configuration validation."""
        from constellation.orchestration.export_manager import validate_export_config

        # Valid config
        valid_config = ExportConfig(
            output_dir=Path("/tmp"),
            namespace="test",
            chart_name="test-chart",
        )
        errors: list[str] = validate_export_config(valid_config)
        assert len(errors) == 0

        # Invalid config
        invalid_config = ExportConfig(
            output_dir=None,  # type: ignore
            namespace="",
            chart_name="",
        )
        errors: list[str] = validate_export_config(invalid_config)
        assert len(errors) == 3
        assert "Output directory is required" in errors
        assert "Namespace is required" in errors
        assert "Chart name is required" in errors


class TestRoundTripImportExport:
    """Test round-trip import/export functionality."""

    def test_basic_round_trip(self, tmp_path: Path):
        """Test importing a Docker Compose file and exporting it back."""
        original_compose: dict[str, Any] = {
            "version": "3.8",
            "services": {
                "web": {
                    "image": "nginx:latest",
                    "ports": ["80:80"],
                    "environment": {"ENV_VAR": "value"},
                    "restart": "unless-stopped",
                },
                "db": {
                    "image": "postgres:13",
                    "environment": {"POSTGRES_DB": "mydb"},
                    "volumes": ["db-data:/var/lib/postgresql/data"],
                },
            },
            "volumes": {"db-data": {}},
        }

        # Write original file
        original_file: Path = tmp_path / "original.yml"
        with original_file.open("w", encoding="utf-8") as f:
            yaml.dump(original_compose, f)

        # Import
        import_manager: ImportManager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(original_file)

        # Export
        export_config: ExportConfig = ExportConfig(output_dir=tmp_path)
        export_manager: ExportManager = ExportManager(export_config)
        exported_file: Path = export_manager.export(containers, "compose")

        # Read exported file
        with exported_file.open("r", encoding="utf-8") as f:
            exported_compose: dict[str, Any] = yaml.safe_load(f)

        # Verify key elements are preserved
        assert len(exported_compose["services"]) == 2
        assert "web" in exported_compose["services"]
        assert "db" in exported_compose["services"]

        web_service: dict[str, Any] = exported_compose["services"]["web"]
        assert web_service["image"] == "nginx:latest"
        assert web_service["ports"] == ["80:80"]
        assert web_service["environment"]["ENV_VAR"] == "value"

        db_service: dict[str, Any] = exported_compose["services"]["db"]
        assert db_service["image"] == "postgres:13"
        assert db_service["environment"]["POSTGRES_DB"] == "mydb"

        # Volumes should be preserved
        assert "volumes" in exported_compose
        assert "db-data" in exported_compose["volumes"]

    def test_complex_round_trip(self, tmp_path: Path):
        """Test round-trip with complex Docker Compose features."""
        original_compose: dict[str, Any] = {
            "version": "3.8",
            "services": {
                "app": {
                    "image": "myapp:v1.0.0",
                    "container_name": "my-app",
                    "hostname": "app-host",
                    "ports": [
                        "8080:80",
                        {"target": 3000, "published": 3000, "protocol": "tcp"},
                    ],
                    "environment": {
                        "DATABASE_URL": "postgres://localhost/db",
                        "DEBUG": "true",
                    },
                    "volumes": [
                        "/host/data:/app/data",
                        "app-storage:/app/storage",
                        "/host/config:/app/config:ro",
                    ],
                    "command": ["python", "app.py"],
                    "entrypoint": ["/entrypoint.sh"],
                    "working_dir": "/app",
                    "user": "1000:1000",
                    "depends_on": ["db"],
                    "restart": "always",
                    "labels": {
                        "app.name": "myapp",
                        "app.version": "1.0.0",
                    },
                    "healthcheck": {
                        "test": ["CMD", "curl", "-f", "http://localhost/health"],
                        "interval": "30s",
                        "timeout": "10s",
                        "retries": 3,
                    },
                    "deploy": {
                        "resources": {
                            "limits": {
                                "cpus": "0.5",
                                "memory": "512M",
                            }
                        }
                    },
                }
            },
            "volumes": {"app-storage": {}},
        }

        # Write, import, export
        original_file: Path = tmp_path / "complex.yml"
        with original_file.open("w", encoding="utf-8") as f:
            yaml.dump(original_compose, f)

        import_manager: ImportManager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(original_file)

        export_config: ExportConfig = ExportConfig(output_dir=tmp_path)
        export_manager: ExportManager = ExportManager(export_config)
        exported_file: Path = export_manager.export(containers, "compose")

        # Verify round-trip preservation
        with exported_file.open("r", encoding="utf-8") as f:
            exported_compose: dict[str, Any] = yaml.safe_load(f)

        # The service name becomes the container name after round-trip
        app_service: dict[str, Any] = exported_compose["services"]["my-app"]

        # Check that complex features are preserved
        assert app_service["image"] == "myapp:v1.0.0", "Image 'myapp:v1.0.0' not preserved"
        assert app_service["container_name"] == "my-app", "Container name 'my-app' not preserved"
        assert app_service["hostname"] == "app-host", "Hostname 'app-host' not preserved"
        assert "8080:80" in app_service["ports"], "Port mapping '8080:80' not preserved"
        assert "3000:3000" in app_service["ports"], "Port mapping '3000:3000' not preserved"
        assert app_service["environment"]["DATABASE_URL"] == "postgres://localhost/db", "Environment variable 'DATABASE_URL' not preserved"
        assert "/host/data:/app/data" in app_service["volumes"], "Volume mount '/host/data:/app/data' not found"
        assert "app-storage:/app/storage" in app_service["volumes"], "Volume mount 'app-storage:/app/storage' not found"
        assert "/host/config:/app/config:ro" in app_service["volumes"], "Volume mount '/host/config:/app/config:ro' not found"
        assert app_service["command"] == ["python", "app.py"], "Command 'python app.py' not preserved"
        assert app_service["entrypoint"] == ["/entrypoint.sh"], "Entrypoint '/entrypoint.sh' not preserved"
        assert app_service["working_dir"] == "/app", "Working directory '/app' not preserved"
        assert app_service["user"] == "1000:1000", "User '1000:1000' not preserved"
        assert app_service["depends_on"] == ["db"], "Depends-on 'db' not preserved"
        assert app_service["restart"] == "always", "Restart policy 'always' not preserved"
        assert app_service["labels"]["app.name"] == "myapp", "Label 'app.name' not preserved"
        assert "healthcheck" in app_service, "Healthcheck section not preserved"
        assert "deploy" in app_service, "Deploy section not preserved"

        # Verify volumes section
        assert "volumes" in exported_compose
        assert "app-storage" in exported_compose["volumes"]

    def test_data_preservation_accuracy(self, tmp_path: Path):
        """Test that specific data types and edge cases are preserved accurately."""
        original_compose: dict[str, Any] = {
            "version": "3.8",
            "services": {
                "edge-case-app": {
                    "image": "app:latest",
                    "environment": {
                        "STRING_VAR": "string_value",
                        "NUMBER_VAR": 42,
                        "BOOL_VAR": True,
                        "NULL_VAR": None,
                        "EMPTY_VAR": "",
                    },
                    "labels": [
                        "label1=value1",
                        "label2=value2",
                        "label_without_value",
                    ],
                    "ports": [
                        80,  # Bare integer
                        "443:443",  # String
                    ],
                }
            },
        }

        original_file: Path = tmp_path / "edge-cases.yml"
        with original_file.open("w", encoding="utf-8") as f:
            yaml.dump(original_compose, f)

        # Import and export
        import_manager: ImportManager = ImportManager()
        containers: list[ContainerConfig] = import_manager.import_from_compose(original_file)

        export_config: ExportConfig = ExportConfig(output_dir=tmp_path)
        export_manager: ExportManager = ExportManager(export_config)
        _exported_file: Path = export_manager.export(containers, "compose")

        # Verify data preservation
        container: ContainerConfig = containers[0]

        # Environment variables should be strings
        assert container.environment["STRING_VAR"] == "string_value", "STRING_VAR not preserved"
        assert container.environment["NUMBER_VAR"] == "42", "NUMBER_VAR not preserved"
        assert container.environment["BOOL_VAR"] == "True", "BOOL_VAR not preserved"
        assert container.environment["NULL_VAR"] == "", "NULL_VAR not preserved"
        assert container.environment["EMPTY_VAR"] == "", "EMPTY_VAR not preserved"

        # Labels should be converted from list to dict
        assert container.labels["label1"] == "value1", "label1 not preserved"
        assert container.labels["label2"] == "value2", "label2 not preserved"
        assert container.labels["label_without_value"] == "", "label_without_value not preserved"

        # Ports should be properly parsed
        port_map: dict[int, PortMapping] = {p.container_port: p for p in container.ports}
        assert 80 in port_map, "80 port not preserved"
        assert port_map[80].host_port is None, "80 port host port not preserved"
        assert 443 in port_map, "443 port not preserved"
        assert port_map[443].host_port == 443, "443 port host port not preserved"
