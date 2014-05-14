// install Cassandra, e.g. 'brew install cassandra'
// then execute this script: 'cqlsh -f setup.cql'
// you can also run it to reset (truncate) the data table

CREATE KEYSPACE test
WITH replication = {
    'class': 'SimpleStrategy', 
    'replication_factor': 1 
};

use test;

// hash_key is the partition key
// message_id is the cluster key, most recent first within partition
CREATE TABLE data (
  hash_key text,
  message_id timeuuid,
  body blob,
  metadata text,
  PRIMARY KEY (hash_key, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

TRUNCATE data;
