-- docker/cassandra/init.cql
CREATE KEYSPACE IF NOT EXISTS netflow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- TABLE DES FLOWS (historique)
CREATE TABLE IF NOT EXISTS netflow.flows (
    src_ip_int INT,
    timestamp TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    duration_ms BIGINT,
    label INT,
    PRIMARY KEY (src_ip_int, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': '100'};

-- TABLE DES PRÉDICTIONS (temps réel)
CREATE TABLE IF NOT EXISTS netflow.predictions (
    src_ip_int INT,
    timestamp TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    prediction INT,
    PRIMARY KEY (src_ip_int, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'HOURS'};