#! /bin/bash
cd examples

set -e
export TERM=${TERM:-xterm}
export REDIS_HOST=${REDIS_HOST:-127.0.0.1}
export REDIS_PORT=${REDIS_PORT:-6379}

do_test() {
  echo "$(tput bold)Testing pairs: $(tput setaf 3)$1$(tput setaf 4) <==> $(tput setaf 3)$2$(tput sgr0)"
  trap "pkill -TERM -P $$" ERR RETURN
  python "$1" &
  timeout 60 python "$2"
}

do_test simple-server.py simple-client.py
# TODO: fix up Redis RPC
# do_test simple-server-redis.py simple-client-redis.py
do_test simple-server-thrift.py simple-client-thrift.py
do_test ordering-by-exit-server.py ordering-client.py
do_test ordering-by-key-server.py ordering-client.py
do_test simple-consumer-redis.py simple-publisher-redis.py

exit $?
