# Claude Code Guide for Callosum

## Project Overview

Callosum is an asyncio-based RPC library for Python that supports multiple transport backends (ZeroMQ, Redis, Thrift).

## Development Environment

This project uses **uv** as the package manager.

### Setup

```bash
uv sync --extra zeromq --extra redis --extra thrift
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_rpc.py -v

# Run specific test
uv run pytest tests/test_rpc.py::test_external_context_injection -v
```

### Type Checking

```bash
uv run mypy src/callosum
```

### Linting

```bash
uv run ruff check src/
uv run ruff format src/
```

## Project Structure

```
src/callosum/
├── lower/           # Transport layer (zeromq, redis)
│   ├── __init__.py  # BaseTransport, AbstractBinder, AbstractConnector
│   ├── zeromq.py    # ZeroMQ transport implementation
│   └── redis.py     # Redis transport implementation
├── rpc/             # RPC layer
│   ├── channel.py   # Peer class (main RPC interface)
│   ├── message.py   # RPC message types
│   └── exceptions.py
├── auth.py          # Authentication (CURVE for ZeroMQ)
├── ordering.py      # Async schedulers
└── serial.py        # Serialization utilities
```

## Key Classes

- `Peer` (rpc/channel.py): Main RPC interface for both client and server
- `ZeroMQBaseTransport` (lower/zeromq.py): ZeroMQ transport with context management
- `ZeroMQRPCTransport`: RPC-specific transport using ROUTER/DEALER sockets

## Optional Dependencies

Defined in `pyproject.toml` under `[project.optional-dependencies]`:
- `zeromq`: pyzmq for ZeroMQ transport
- `redis`: redis-py for Redis transport
- `thrift`: thriftpy2 for Thrift serialization
- `snappy`: python-snappy for compression
