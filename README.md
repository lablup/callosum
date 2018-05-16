Callosum
========

An RPC Transport Library

It provides an asynchronous multi-channel order-preserving message and data
streaming transport for *upper* RPC layers (e.g., Apache Thrift) by wrapping
*lower* transport implementations (e.g., ZeroMQ).

*Corpus callosum* is a bundle of neuron fibers that connects two cerebral
hemispheres of a human brain.

Features
--------

* Designed for Python asyncio and bases on ZeroMQ
* Client authentication and encrypted communication
* Persistent multiple messaging channels where each channel is order-preserved
* Intrinsic support for error propagation
* Supports large-size data streaming via automatic chunking
* Replacible lower/upper layers

Installation
------------

TODO
