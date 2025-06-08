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

### News Fragment Types

Use these suffixes to put news fragments in the `changes/` directory for each PR, whereas the name should be the PR number (e.g., `1234.fix.dm`):

- `.breaking.md` - Breaking changes
- `.feature.md` - New features
- `.fix.md` - Bug fixes
- `.deprecation.md` - Deprecation notices
- `.doc.md` - Documentation changes
- `.misc.md` - Miscellaneous changes

### Running Examples

You can run examples using uv with additional dependencies:

```console
$ uv sync --extra zeromq --extra redis --extra thrift --extra snappy
$ uv run python examples/simple-server.py
```

Making a Release
----------------

This project uses [towncrier](https://towncrier.readthedocs.io/) for changelog management and [setuptools-scm](https://setuptools-scm.readthedocs.io/) for automatic versioning based on git tags.

### Release Process

1. **Ensure all changes are documented**: Add change fragments to the `changes/` directory:
   ```console
   # For a bug fix
   $ echo "Fix description of the bug fix ([#123](https://github.com/lablup/callosum/issues/123))" > changes/123.fix.md

   # For a new feature
   $ echo "Description of the new feature ([#124](https://github.com/lablup/callosum/issues/124))" > changes/124.feature.md
   ```

2. **Generate the changelog**: Create a draft changelog to review:
   ```console
   $ uv run towncrier build --draft --version 1.0.4
   ```

3. **Build and finalize the changelog**: When ready to release:
   ```console
   $ uv run towncrier build --version 1.0.4
   ```
   This updates `CHANGES.md` and removes the change fragments.

4. **Commit the changelog**:
   ```console
   $ git add CHANGES.md changes/
   $ git commit -m "release: 1.0.4"
   ```

5. **Create and push a annotated, signed git tag**:
   ```console
   $ git tag -a -s 1.0.4
   $ git push origin main --tags
   ```

6. **Automated release**: The GitHub Actions workflow will automatically:
   - Run all tests and checks
   - Build source and wheel distributions using `uv build`
   - Extract release notes from the changelog
   - Create a GitHub release with the built artifacts
   - Publish to PyPI using trusted publishing

### Release Workflow

The release is triggered automatically when a git tag is pushed. The workflow:

1. **Triggers on**: Git tag push (e.g., `git push origin --tags`)
2. **Builds**: Uses `uv build` to create distributions
3. **Publishes**:
   - GitHub release with changelog and artifacts
   - PyPI release using trusted publishing (no manual tokens needed)
4. **Determines release type**: Automatically detects pre-releases (`rc`, `a` (alpha), `b` (beta), `dev` suffixes)
