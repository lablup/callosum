# Changes

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

<!-- towncrier release notes start -->

## v1.0.1 (2023-09-18)

### Fixes
* Prevent leaking secret keys in the logs and allow infinite timeouts on connection pings ([#24](https://github.com/lablup/callosum/issues/24))


## v1.0.0 (2023-09-14)

### Breaking change
* Now it requires Python 3.11 to work!

### Features
* Implement the full version of secure, encrypted RPC communication based on ZeroMQ's ZAP protocol using CURVE keypairs ([#21](https://github.com/lablup/callosum/issues/21))


## v0.9.10 (2022-02-17)

### Fixes
* Fix pyzmq attribute error on Ubuntu 20.04 at aarch64, which is built using older libzeromq without some socket monitoring event constants, by loading the constant declarations dynamically ([#20](https://github.com/lablup/callosum/issues/20))


## v0.9.9 (2021-10-05)

### Fixes
* Add explicit `task_done()` call to the output queue of `rpc.Peer` instances to avoid potential missing wakeup on joined coroutines ([#16](https://github.com/lablup/callosum/issues/16))


## v0.9.8 (2021-10-05)

### Features
* lower.zeromq: Add a transport option to attach monitors for logging underlying socket events ([#17](https://github.com/lablup/callosum/issues/17))

### Fixes
* Improve `zsock_opts` handling when explicitly specified by the library users, as previously it caused argument errors in binders and connectors ([#18](https://github.com/lablup/callosum/issues/18))


## v0.9.7 (2020-12-22)

### Features
* Provide `repr()` of exceptions in RPC user/internal errors for better application-level error logging ([#15](https://github.com/lablup/callosum/issues/15))


## v0.9.6 (2020-06-05)

### Features
* `upper.rpc`: Support server-side cancellation and propagation to clients by adding the `CANCELLED` RPC message type ([#14](https://github.com/lablup/callosum/issues/14))

### Deprecations
* Drop use of aiojobs in favor of native semaphores for limiting the task concurrency and less clutters on job scheduling semantics ([#14](https://github.com/lablup/callosum/issues/14))

### Fixes
* Stability updates for the RPC layer: ([#14](https://github.com/lablup/callosum/issues/14))
  - Fix wrong message sequence calcuation with `SEQ_BITS` and clarify the roles of `cleanup()` and `cancel()` methods in the schedulers.
  - Now we use the exit-ordered scheduler by default.


## v0.9.5 (2020-05-12)

### Fixes
* lower.zeromq: Use destroy() for zmq context termination to improve stability and shutdown open socekts cleanly ([#13](https://github.com/lablup/callosum/issues/13))


## v0.9.4 (2020-04-10)

### Fixes
* Fix a race condition due to overlapping RPC message sequence IDs by separating server/client message sequence IDs [(#12)](https://github.com/lablup/callosum/issues/12)

### Miscellaneous
* Adopt towncrier for changelog management [(#11)](https://github.com/lablup/callosum/issues/11)


## v0.9.3 (2020-03-20)

* FIX: Mitigate bogus KeyError when cleaning up task futures in RPC servers that has caused event
  loop spinning.


## v0.9.2 (2020-02-28)

* MAINTENANCE: Update dependencies and only specify the minimum versions since
  Callosum is a library.


## v0.9.1 (2020-01-05)

* FIX: wrong typing of `RPCMessage.body` field

* IMPROVE: Add `debug_rpc` option to `rpc.Peer` for logging exceptions in RPC
  scheduler and user-defined handlers explicitly.

* Update dependencies and remove unused ones.


## v0.9.0 (2019-12-06)

* First public release with a working RPC based on ZeroMQ DEALER/ROUTER sockets.


## 2018-05-02

* Started the project.
