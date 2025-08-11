# Constellation - Distributed Container Orchestration System

Constellation is a distributed container orchestration system designed to provide high availability and automatic failover for containerized applications across multiple nodes.

I created this to get an idea about how industry standard orchestrators make decisions, and to test the real-world consequences of various ideas and strategies that I brainstormed but have not seen in modern practice.

This project is loosely based on nomad/kubernetes/swarm. None of this should be used in production at this time.

## Features

- **Distributed Architecture**: Peer-to-peer node discovery using Tailscale
- **High Availability**: Container-level failover with automatic recovery
- **VPN-Aware Routing**: Special handling for VPN containers and network dependencies
- **Multi-Node Distribution**: Automatic workload distribution across available nodes
- **Real-time Monitoring**: Health monitoring with Prometheus metrics
- **Zero-Downtime Updates**: Rolling updates with automatic rollback

## Architecture

Constellation consists of several core components:

- **Node Discovery Service**: Automatic peer discovery via Tailscale network
- **Container Management**: Docker integration with lifecycle management
- **Distributed State Management**: Consensus-based state synchronization
- **Network Management**: VPN-aware routing and failover
- **Failover Manager**: Intelligent container migration and recovery
- **REST API**: FastAPI-based management interface
- **WebSocket Service**: Real-time updates and notifications

## Requirements

- Python 3.11+
- Docker Engine
- Tailscale (for node discovery)
- Linux (recommended)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd constellation
```

2. Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Install in development mode:

```bash
pip install -e .
```

## Quick Start

1. Start the Constellation node:

```bash
constellation --config config/constellation.yml
```

2. Deploy your first service:

```bash
constellation deploy --file docker-compose.yml
```

## Configuration

Configuration is managed via YAML files. See `config/constellation.yml.example` for a complete configuration reference.

## Development

### Setup Development Environment

```bash
# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Run tests
pytest

# Run linting
black src/ tests/
isort src/ tests/
flake8 src/ tests/
mypy src/
```

### Project Structure

```shell
src/constellation/
├── core/           # Core orchestration logic
├── api/            # REST API endpoints
├── network/        # Network management
├── discovery/      # Node discovery service
├── failover/       # Failover management
├── monitoring/     # Health monitoring
└── config/         # Configuration management
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Status

This project is currently in alpha development. APIs and features may change.
