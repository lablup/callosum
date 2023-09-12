import re
from pathlib import Path
from typing import List

from setuptools import find_packages, setup

root = Path(__file__).parent


def read_src_version():
    p = root / "src" / "callosum" / "__init__.py"
    src = p.read_text()
    m = re.search(r'^__version__\s*=\s*"([^"]+)"', src, re.M)
    assert m is not None
    return m.group(1)


install_requires = [
    "aiotools>=1.7.0",
    "attrs>=21.3.0",
    "python-dateutil>=2.8.2",
    "msgpack>=1.0.4",
    "temporenc>=0.1",
    "yarl>=1.8.2",
]

build_requires = [
    "wheel>=0.38.4",
    "twine>=4.0.1",
    "towncrier>=22.8.0",
]

test_requires = [
    "Click>=8.0",
    "pytest~=7.2",
    "pytest-asyncio>=0.20.2",
    "pytest-cov>=4.0",
    "pytest-mock>=3.10.0",
    "codecov>=2.1",
]

dev_requires: List[str] = [
    # 'pytest-sugar',
    "pre-commit",
]

lint_requires = [
    "black~=23.9.1",
    "ruff>=0.0.287",
    "ruff-lsp",
]

typecheck_requires = [
    "mypy~=1.5.1",
    "types-python-dateutil",
]

docs_requires = [
    "sphinx",
    "sphinx-autodoc-typehints",
]

thrift_requires = [
    "thriftpy2>=0.4.16",
]

zmq_requires = [
    "pyzmq>=23.0.0",
]

redis_requires = ["redis>=4.6.0"]

snappy_requires = [
    "python-snappy>=0.6.1",
]

setup(
    name="callosum",
    version=read_src_version(),
    description="Callosum RPC Library",
    long_description="\n\n".join(
        [(root / "README.md").read_text(), (root / "CHANGES.md").read_text()]
    ),
    long_description_content_type="text/markdown",
    url="https://github.com/lablup/callosum",
    author="Lablup Inc.",
    author_email="joongi@lablup.com",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: AsyncIO",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Communications",
        "Topic :: Internet",
    ],
    package_dir={
        "": "src",
    },
    packages=find_packages("src"),
    package_data={
        "callosum": ["py.typed"],
    },
    include_package_data=True,
    python_requires=">=3.11",
    setup_requires=["setuptools>=61.0"],
    install_requires=install_requires,
    extras_require={
        "build": build_requires,
        "test": test_requires,
        "dev": dev_requires,
        "lint": lint_requires,
        "typecheck": typecheck_requires,
        "docs": docs_requires,
        "thrift": thrift_requires,
        "zeromq": zmq_requires,
        "redis": redis_requires,
        "snappy": snappy_requires,
    },
)
