-- =============================================
-- CASSANDRA SCHEMA - NetFlow Anomaly Detection System
-- Fully compliant with Cahier des Charges (Section 6.5)
-- =============================================

-- Create Keyspace (as specified in the document)
CREATE KEYSPACE IF NOT EXISTS netflow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the correct keyspace
USE netflow;

-- Table for historical NetFlow records (exact structure from Cahier des Charges p.6.5)
CREATE TABLE IF NOT EXISTS netflow.flows (
    src_ip_int INT,           -- Source IP converted to integer
    timestamp TIMESTAMP,       -- Flow timestamp
    dst_ip_int INT,            -- Destination IP as integer
    src_port INT,              -- Source port
    dst_port INT,              -- Destination port
    protocol INT,              -- Protocol number (6=TCP, 17=UDP, etc.)
    in_bytes BIGINT,           -- Bytes from source to destination
    out_bytes BIGINT,          -- Bytes from destination to source
    duration_ms BIGINT,        -- Flow duration in milliseconds
    label INT,                 -- 0 = Benign, 1 = Attack
    PRIMARY KEY (src_ip_int, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_size': 1,
                    'compaction_window_unit': 'DAYS'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': '100'};

-- Table for real-time predictions (as required in streaming pipeline)
CREATE TABLE IF NOT EXISTS netflow.predictions (
    src_ip_int INT,
    timestamp TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    prediction INT,            -- 0 = Normal, 1 = Anomaly/Attack
    PRIMARY KEY (src_ip_int, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_size': 1,
                    'compaction_window_unit': 'HOURS'};