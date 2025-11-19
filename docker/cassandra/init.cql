-- =============================================
-- Schema Cassandra optimisé pour NF-CSE-CIC-IDS2018-v2
-- Compatible avec plus de 80 colonnes du dataset original
-- =============================================

CREATE KEYSPACE IF NOT EXISTS netflow
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

USE netflow;

-- Table principale : stockage des flux historiques + batch
CREATE TABLE IF NOT EXISTS netflow.flows (
    flow_id BIGINT,                                      -- Identifiant unique du flux

    -- Adresses IP (texte + entier pour partitionnement rapide)
    src_ip TEXT,
    dst_ip TEXT,
    src_ip_int INT,
    dst_ip_int INT,

    src_port INT,                                        -- Port source
    dst_port INT,                                        -- Port destination
    protocol INT,                                        -- Protocole (6=TCP, 17=UDP, etc.)
    l7_proto DOUBLE,                                     -- Protocole applicatif détecté

    in_bytes BIGINT,                                     -- Bytes entrants
    out_bytes BIGINT,                                    -- Bytes sortants
    in_pkts BIGINT,                                      -- Paquets entrants
    out_pkts BIGINT,                                     -- Paquets sortants

    duration_ms BIGINT,                                  -- Durée du flux en ms
    tcp_flags INT,                                       -- Flags TCP combinés
    client_tcp_flags INT,                                -- Flags TCP du client
    server_tcp_flags INT,                                -- Flags TCP du serveur

    flow_start_time TIMESTAMP,                           -- Horodatage (utilisé pour partitionnement temporel)

    -- Quelques caractéristiques importantes du dataset original
    src_to_dst_avg_throughput DOUBLE,
    dst_to_src_avg_throughput DOUBLE,
    num_pkts_up_to_128_bytes BIGINT,
    num_pkts_128_to_256_bytes BIGINT,
    num_pkts_256_to_512_bytes BIGINT,
    num_pkts_512_to_1024_bytes BIGINT,
    num_pkts_1024_to_1514_bytes BIGINT,

    label INT,                                           -- 0 = Benign, 1 = Attaque
    attack_type TEXT,                                    -- Type d'attaque (DDoS, Bruteforce, Benign...)

    PRIMARY KEY ((src_ip_int), flow_start_time, flow_id)
)
WITH CLUSTERING ORDER BY (flow_start_time DESC, flow_id ASC)
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
};



CREATE TABLE IF NOT EXISTS netflow.predictions (
    src_ip_int INT,
    ingestion_time TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    duration_ms BIGINT,
    is_anomaly INT,
    anomaly_score DOUBLE,
    PRIMARY KEY (src_ip_int, ingestion_time)
) WITH CLUSTERING ORDER BY (ingestion_time DESC);

-- Index pour accélérer les requêtes par type d'attaque
CREATE INDEX IF NOT EXISTS idx_label ON netflow.flows (label);
CREATE INDEX IF NOT EXISTS idx_attack_type ON netflow.flows (attack_type);