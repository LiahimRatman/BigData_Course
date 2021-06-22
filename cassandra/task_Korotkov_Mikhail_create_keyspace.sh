#!/usr/bin/env bash

REPLICATIONFACTOR=2
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ${2} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $REPLICATIONFACTOR}" ${1}
