CREATE KEYSPACE IF NOT EXISTS security
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS security.logs (
    id UUID PRIMARY KEY,
    src_ip TEXT,
    dst_ip TEXT,
    src_port INT,
    dst_port INT,
    protocol INT,
    bytes_in BIGINT,
    bytes_out BIGINT,
    pkt_in BIGINT,
    pkt_out BIGINT,
    timestamp TIMESTAMP,
    label INT
);

CREATE TABLE IF NOT EXISTS security.predictions (
    id UUID PRIMARY KEY,
    src_ip TEXT,
    prediction INT,
    probability DOUBLE,
    timestamp TIMESTAMP
);
