-- docker/cassandra/init.cql
CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS netflow.flows (
    id UUID PRIMARY KEY,
    src_ip_int INT,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    duration_ms BIGINT,
    label INT
);

CREATE TABLE IF NOT EXISTS netflow.predictions (
    id UUID PRIMARY KEY,
    src_ip_int INT,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    prediction INT,
    timestamp TIMESTAMP
);