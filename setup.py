from setuptools import setup, find_packages
from pathlib import Path
import re

root = Path(__file__).parent


def read_src_version():
    p = (root / 'src' / 'callosum' / '__init__.py')
    src = p.read_text()
    m = re.search(r"^__version__\s*=\s*'([^']+)'", src, re.M)
    return m.group(1)


install_requires = [
    'aiojobs>=0.2.2',
    'aiotools>=0.8.5',
    'async_timeout~=3.0.1',
    'attrs>=19.3.0',
    'python-dateutil>=2.8.1',
    'msgpack~=0.6.2',
    'yarl>=1.3.0',
]

build_requires = [
    'wheel>=0.33.6',
    'twine>=3.1.0',
]

test_requires = [
    'pytest>=5.2',
    'pytest-asyncio>=0.10',
    'pytest-cov',
    'pytest-mock',
    'mypy>=0.750',
    'flake8>=3.7.9',
    'codecov',
]

dev_requires = [
    'pytest-sugar',
]

docs_requires = [
    'sphinx',
    'sphinx-autodoc-typehints',
]

thrift_requires = [
    'aiothrift',
]

zmq_requires = [
    'pyzmq>=18.1.1',
]

redis_requires = [
    'aioredis>=1.3.0',
]

compress_requires = [
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
        'Development Status :: 3 - Alpha',
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
    setup_requires=['setuptools>=42.0.1'],
    install_requires=install_requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'docs': docs_requires,
        'thrift': thrift_requires,
        'zeromq': zmq_requires,
        'redis': redis_requires,
        'compress': compress_requires,
    },
)
