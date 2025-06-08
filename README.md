Callosum
========

An RPC Transport Library

It provides an asynchronous multi-channel order-preserving message and data
streaming transport for *upper* RPC layers (e.g., Apache Thrift) by wrapping
*lower* transport implementations (e.g., ZeroMQ).

It aims to follow the latest coding style and conventions in Python asyncio.

*Corpus callosum* is a bundle of neuron fibers that connects two cerebral
hemispheres of a human brain.

Prerequisite
------------

Python 3.11 or higher.

Features
--------

* RPC
  - Native timeout and cancellation support
  - Explicit server-to-client error propagation including stringified tracebacks
  - Order preserving based on user-defined keys while keeping executions asynchronous
* Streaming
  - Broadcast & shared pipelines
* Optional client authentication and encrypted communication
  - Currently supported for only ZeroMQ with its CURVE library
* Optional message compression using [snappy](https://pypi.org/project/python-snappy/)
* Replacible and combinable lower/upper layers (ZeroMQ/Redis + JSON/msgpack/Thrift)

Planned features
----------------

* Managed streaming (with acks)
* Tunneling to bundle other channels and generic network traffic in a single connection
* Bidirectional RPC
* Chunked transfer of large messages

Installation
------------

To install the core:

```console
$ pip install -U pip setuptools
$ pip install callosum
```

You may add extra dependencies like:

```console
$ pip install 'callosum[zeromq,redis,thrift,snappy]'
```

Examples
--------

Please check out [the examples directory.](https://github.com/lablup/callosum/tree/master/examples)

Development
-----------

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and development workflow.

### Prerequisites

Install uv:

```console
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Setup

Clone the repository and set up the development environment:

```console
$ git clone https://github.com/lablup/callosum.git
$ cd callosum
$ uv sync --dev
```

This will create a virtual environment and install the package in editable mode along with all development dependencies.

### Development Commands

Run tests:
```console
$ uv run pytest
```

Run pre-commit hooks for linting:
```console
$ pre-commit run --all-files
```

Build the package:
```console
$ uv build
```

### Running Examples

You can run examples using uv with additional dependencies:

```console
$ uv sync --extra zeromq --extra redis --extra thrift --extra snappy
$ uv run python examples/simple-server.py
```
