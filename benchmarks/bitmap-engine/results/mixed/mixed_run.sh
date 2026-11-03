#!/bin/bash
# search under update load: ours vs ES (default / async translog / async+refresh 5s / split index)
cd "$(dirname "$0")"
E=./bitmap_engine/target/release/es; P=$(pgrep -x prod); J=$(cat /opt/esbench/elasticsearch-9.2.0/es.pid)
es_set() { curl -s -XPUT localhost:9200/$1/_settings -H 'Content-Type: application/json' -d "$2" > /dev/null; }
run() { # sys spid label
  local sys=$1 spid=$2 label=$3
  SPID=$spid taskset -c 0 $E mixed $sys amazon/all.tsv 0 30 "$label | no updates"
  SPID=$spid taskset -c 0 $E mixed $sys amazon/all.tsv 10000 60 "$label | 10k updates/s"
  SPID=$spid taskset -c 0 $E mixed $sys amazon/all.tsv 50000 60 "$label | 50k updates/s"
  QCLIENTS=0 SPID=$spid taskset -c 0 $E mixed $sys amazon/all.tsv 10000 30 "$label | 10k updates/s, no queries"
  QCLIENTS=0 SPID=$spid taskset -c 0 $E mixed $sys amazon/all.tsv 50000 30 "$label | 50k updates/s, no queries"
}
run ours $P "ours (WAL fsync per bulk)"
es_set real '{"index":{"refresh_interval":"1s","translog.durability":"request"}}'
run es $J "ES A default (refresh 1s, translog fsync per request)"
es_set real '{"index":{"translog.durability":"async","translog.sync_interval":"5s"}}'
run es $J "ES B async translog (5s), refresh 1s"
es_set real '{"index":{"refresh_interval":"5s"}}'
run es $J "ES C async translog, refresh 5s"
es_set real '{"index":{"refresh_interval":"1s","translog.durability":"request"}}'
es_set vol '{"index":{"refresh_interval":"1s","translog.durability":"request"}}'
run split $J "ES D split index (stock/price in vol, mget), defaults"
taskset -c 0 $E mixedcheck amazon/all.tsv
