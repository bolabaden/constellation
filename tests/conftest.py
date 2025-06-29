"""Pytest configuration and fixtures for Constellation tests."""
from __future__ import annotations

import asyncio
from typing import Any, Generator
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_docker_client() -> Mock:
    """Mock Docker client for testing."""
    mock: Mock = Mock()
    mock.containers = Mock()
    mock.networks = Mock()
    mock.volumes = Mock()
    mock.images = Mock()
    return mock


@pytest.fixture
async def mock_tailscale_client() -> AsyncMock:
    """Mock Tailscale client for testing."""
    mock: AsyncMock = AsyncMock()
    mock.status = AsyncMock()
    mock.devices = AsyncMock()
    return mock


@pytest.fixture
def sample_docker_compose_config() -> dict[str, Any]:
    """Sample docker-compose configuration for testing."""
    return {
        "services": {
            "web": {
                "image": "nginx:latest",
                "ports": ["80:80"],
                "networks": ["publicnet"],
            },
            "gluetun-vpn": {
                "image": "qmcgaw/gluetun",
                "cap_add": ["NET_ADMIN"],
                "environment": {
                    "VPN_SERVICE_PROVIDER": "nordvpn",
                },
                "networks": ["publicnet"],
            },
            "app": {
                "image": "myapp:latest",
                "network_mode": "service:gluetun-vpn",
                "depends_on": ["gluetun-vpn"],
            },
        },
        "networks": {
            "publicnet": {
                "driver": "bridge",
                "ipam": {
                    "config": [{"subnet": "172.20.0.0/16"}],
                },
            },
        },
    }


@pytest.fixture
def sample_constellation_config() -> dict[str, Any]:
    """Sample Constellation configuration for testing."""
    return {
        "node": {
            "name": "test-node",
            "port": 5443,
            "discovery_interval": 30,
        },
        "vpn": {
            "providers": ["gluetun-vpn", "warp-1", "warp-2"],
            "health_check_interval": 10,
            "failover_threshold": 3,
        },
        "orchestration": {
            "max_containers_per_node": 50,
            "resource_threshold": 0.8,
        },
    }
