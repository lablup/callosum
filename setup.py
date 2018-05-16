from setuptools import setup
from pathlib import Path
import re

root = Path(__file__).parent


def read_src_version():
    p = (root / 'callosum' / '__init__.py')
    src = p.read_text()
    m = re.search(r"^__version__\s*=\s*'([^']+)'", src, re.M)
    return m.group(1)


install_requires = [
    'aiojobs>=0.2.1',
    'async_timeout',
    'dataclasses; python_version<"3.7"',
    'msgpack>=0.5.0',
    'pyzmq>=17.0.0',
    'python-snappy>=0.5.1',
]

build_requires = [
    'wheel>=0.31.0',
    'twine>=1.11.0',
]

test_requires = [
    'pytest>=3.5',
    'pytest-asyncio',
    'pytest-cov',
    'pytest-mock',
    'flake8',
    'codecov',
]

dev_requires = build_requires + test_requires + [
    'pytest-sugar',
]

ci_requires = build_requires + test_requires + []

docs_requires = [
    'sphinx',
    'sphinx-autodoc-typehints',
]

thrift_requires = [
    'aiothrift',
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
        'Development Status :: 2 - Pre-Alpha',
        'Framework :: AsyncIO',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Communications',
        'Topic :: Internet',
    ],
    packages=[
        'callosum',
        'callosum.rpc',
    ],
    python_requires='>=3.6',
    setup_requires=['setuptools>=38.6.0'],
    install_requires=install_requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'ci': ci_requires,
        'docs': docs_requires,
        'thrift': thrift_requires,
    },
)
