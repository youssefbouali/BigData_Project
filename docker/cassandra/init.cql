-- =============================================
-- FINAL & OPTIMIZED CASSANDRA SCHEMA
-- Compatible with your CSV + clean_and_load.py + Streaming
-- =============================================

-- 1. Keyspace
CREATE KEYSPACE IF NOT EXISTS netflow
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

-- 2. Main table for historical & batch flows
CREATE TABLE IF NOT EXISTS netflow.flows (
    flow_id BIGINT,                    -- Unique flow identifier
    src_ip_int INT,                    -- Source IP as integer
    dst_ip_int INT,                    -- Destination IP as integer
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    in_pkts BIGINT,
    out_pkts BIGINT,
    duration_ms BIGINT,
    tcp_flags INT,
    flow_start_time TIMESTAMP,         -- Used for time-based queries
    label INT,                         -- 0 = Benign, 1 = Attack
    PRIMARY KEY (src_ip_int, flow_start_time, flow_id)
)
WITH CLUSTERING ORDER BY (flow_start_time DESC, flow_id ASC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'DAYS'
}
AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'};

-- 3. Table for real-time anomaly predictions (used by Spark Streaming → Cassandra)
CREATE TABLE IF NOT EXISTS netflow.predictions (
    src_ip_int INT,
    flow_start_time TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    duration_ms BIGINT,
    prediction_score DOUBLE,           -- Anomaly score (0–1 or raw score)
    is_anomaly INT,                    -- 1 = Anomaly detected, 0 = Normal
    model_version TEXT,                -- Optional: track which model made the prediction
    PRIMARY KEY (src_ip_int, flow_start_time)
)
WITH CLUSTERING ORDER BY (flow_start_time DESC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'HOURS'
};

-- 4. Optional: Materialized View for Top Attackers (very useful for Power BI)
CREATE MATERIALIZED VIEW IF NOT EXISTS netflow.top_attackers AS
SELECT src_ip_int, flow_start_time, is_anomaly
FROM netflow.predictions
WHERE src_ip_int IS NOT NULL AND flow_start_time IS NOT NULL AND is_anomaly = 1
PRIMARY KEY (src_ip_int, flow_start_time)
WITH CLUSTERING ORDER BY (flow_start_time DESC);