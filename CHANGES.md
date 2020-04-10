Changes
=======

v0.9.4 (2020-04-10)
-------------------

#### Fixes
* Fix a race condition due to overlapping RPC message sequence IDs by separating server/client message sequence IDs [(#12)](https://github.com/lablup/callosum/issues/12)

#### Miscellaneous
* Adopt towncrier for changelog management [(#11)](https://github.com/lablup/callosum/issues/11)

v0.9.3 (2020-03-20)
-------------------

* FIX: Mitigate bogus KeyError when cleaning up task futures in RPC servers that has caused event
  loop spinning.

v0.9.2 (2020-02-28)
-------------------

* MAINTENANCE: Update dependencies and only specify the minimum versions since
  Callosum is a library.

v0.9.1 (2020-01-05)
-------------------

* FIX: wrong typing of `RPCMessage.body` field

* IMPROVE: Add `debug_rpc` option to `rpc.Peer` for logging exceptions in RPC
  scheduler and user-defined handlers explicitly.

* Update dependencies and remove unused ones.

v0.9.0 (2019-12-06)
-------------------

* First public release with a working RPC based on ZeroMQ DEALER/ROUTER sockets.

2018-05-02
----------

* Started the project.
