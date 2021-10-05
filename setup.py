from setuptools import setup, find_packages
from typing import List
from pathlib import Path
import re

root = Path(__file__).parent


def read_src_version():
    p = (root / 'src' / 'callosum' / '__init__.py')
    src = p.read_text()
    m = re.search(r"^__version__\s*=\s*'([^']+)'", src, re.M)
    return m.group(1)


install_requires = [
    'aiotools>=0.9.1',
    'async_timeout>=3.0.1',
    'attrs>=21.2.0',
    'python-dateutil>=2.8.1',
    'msgpack>=1.0.0',
    'temporenc>=0.1',
]

build_requires = [
    'wheel>=0.33.6',
    'twine>=3.1.0',
    'towncrier>=19.2.0',
]

test_requires = [
    'Click>=8.0',
    'pytest~=6.2',
    'pytest-asyncio>=0.15.0',
    'pytest-cov',
    'pytest-mock',
    'codecov',
]

dev_requires: List[str] = [
    # 'pytest-sugar',
]

lint_requires = [
    'flake8>=3.9',
]

typecheck_requires = [
    'mypy>=0.910',
    'types-python-dateutil',
]

docs_requires = [
    'sphinx',
    'sphinx-autodoc-typehints',
]

thrift_requires = [
    'thriftpy2>=0.4.9',
]

zmq_requires = [
    'pyzmq>=19.0.0',
]

redis_requires = [
    'aioredis>=1.3.0,<2.0',
]

snappy_requires = [
    'python-snappy>=0.5.4',
]

setup(
    name='callosum',
    version=read_src_version(),
    description='Callosum RPC Library',
    long_description='\n\n'.join([(root / 'README.md').read_text(),
                                  (root / 'CHANGES.md').read_text()]),
    long_description_content_type='text/markdown',
    url='https://github.com/lablup/callosum',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license="MIT",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: AsyncIO',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Communications',
        'Topic :: Internet',
    ],
    package_dir={
        '': 'src',
    },
    packages=find_packages('src'),
    package_data={
        'callosum': ['py.typed'],
    },
    include_package_data=True,
    python_requires='>=3.8',
    setup_requires=['setuptools>=45.2.0'],
    install_requires=install_requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'lint': lint_requires,
        'typecheck': typecheck_requires,
        'docs': docs_requires,
        'thrift': thrift_requires,
        'zeromq': zmq_requires,
        'redis': redis_requires,
        'snappy': snappy_requires,
    },
)
