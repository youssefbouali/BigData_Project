# NetFlow Big Data Cybersecurity Pipeline (Dockerized)

> **Système Big Data de Surveillance et d’Analyse de Logs Réseau**  
> Basé sur **Kafka, Spark, Cassandra, Elasticsearch, Power BI**  
> Conforme au *Cahier des Charges Détaillé* (v1.0)

---

## Objectifs du Projet

- Capturer le trafic réseau brut via `tcpdump` (mode promiscuous)
- Convertir les paquets PCAP → NetFlow + CSV (via `nfdump`, `CICFlowMeter`)
- Ingérer en temps réel via **Apache Kafka**
- Traiter en streaming avec **Apache Spark Structured Streaming**
- Stocker dans **Cassandra** (persistance) et **Elasticsearch** (recherche)
- Détecter les anomalies via **Spark MLlib**
- Visualiser via **Power BI** et **Kibana**

---

## Architecture Globale

```text
[Live Network Traffic]
        ↓ (tcpdump → PCAP)
[data/pcap/] → [nfpcapd + CICFlowMeter] → [data/flows/ + data/csv/]
        ↓ (convert_and_send.py)
        → Kafka (topic: netflow-data)
        ↓ (Spark Streaming → streaming_predict.py)
        → Cassandra (table: flows, predictions) 
        → Elasticsearch (index: netflow)
        → Kafka (topic: alerts) → [Slack/Teams/Power BI]
```

---

## Structure du Projet

```bash
NETFLOW_BIGDATA_PROJECT/
│
├── requirements.txt                 # pyspark, pandas, cassandra-driver, kafka-python, scikit-learn
├── docker-compose.yml               # Zookeeper, Kafka, Cassandra, Spark, Elasticsearch, Kibana, Capture
│
├── data/                            # Volumes partagés
│   ├── raw_logs.csv                 # Dataset historique (8 GB)
│   ├── pcap/                        # Fichiers PCAP capturés
│   ├── flows/                       # NetFlow bruts (nfcapd.*)
│   └── csv/                         # CSV enrichis (CICFlowMeter)
│
├── scripts/                         # Scripts PySpark (soumis via spark-submit)
│   ├── clean_and_load.py            # Nettoyage + chargement initial Cassandra
│   ├── train_model.py               # Entraînement Random Forest / Isolation Forest
│   └── streaming_predict.py         # Prédiction en temps réel + alertes Kafka
│
├── model/                           # Modèle ML sauvegardé
│   └── anomaly_model.spark/         
│
└── docker/
    ├── capture/
    │   ├── Dockerfile               # tcpdump, nfdump, java, python, kafka-python
    │   ├── capture.sh               # Automate: PCAP → NetFlow → CSV → Kafka
    │   └── convert_and_send.py      # Fusion NetFlow + CICFlowMeter → JSON → Kafka
    │
    ├── cassandra/
    │   └── init.cql                 # CREATE KEYSPACE + tables flows, predictions
    │
    └── spark/
        ├── Dockerfile               # Spark + Python + connecteurs (Cassandra, ES, Kafka)
        ├── start-master.sh
        └── start-worker.sh
```

---

## Prérequis

- Docker & Docker Compose
- 16 GB RAM (recommandé)
- Accès réseau (pour capture en mode promiscuous)
- (Optionnel) `CICFlowMeter.jar` → placer dans `cicflowmeter/`

---

## Installation & Démarrage

```bash
# 1. Cloner le projet
git clone <repo-url>
cd NETFLOW_BIGDATA_PROJECT

# 2. (Optionnel) Ajouter CICFlowMeter
cp /path/to/CICFlowMeter.jar cicflowmeter/

# 3. Démarrer tous les services
docker-compose up --build -d

# 4. Vérifier les logs
docker-compose logs -f capture      # → doit voir PCAP → CSV → Kafka
docker-compose logs -f spark-submit # → doit voir Spark job démarré
```

---

## Services Disponibles

| Service        | URL / Port                          | Description |
|----------------|-------------------------------------|-----------|
| **Kafka**      | `localhost:9092`                    | Topic: `netflow-data`, `alerts` |
| **Cassandra**  | `localhost:9042`                    | Keyspace: `netflow` |
| **Elasticsearch** | `http://localhost:9200`          | Index: `netflow` |
| **Kibana**     | `http://localhost:5601`             | Visualisation temps réel |
| **Spark Master**| `http://localhost:8080`            | Suivi des jobs |
| **Power BI**   | Connecteur → Elasticsearch / Cassandra | Dashboards décisionnels |

---

## Fonctionnalités Implémentées

| Module | Fonctionnalités |
|-------|-----------------|
| **Capture** | `tcpdump` + rotation PCAP + mode promiscuous |
| **Conversion** | `nfpcapd` → NetFlow, `CICFlowMeter` → 80+ features ML |
| **Ingestion** | Python → Kafka (`netflow-data`) |
| **Traitement** | Spark Streaming → nettoyage, enrichissement, ML |
| **Stockage** | Cassandra (`flows`, `predictions`) |
| **Indexation** | Elasticsearch (`netflow/_doc`) |
| **ML** | Random Forest / Isolation Forest → détection d’anomalies |
| **Alertes** | Topic Kafka `alerts` → Power BI / Slack |
| **Visualisation** | Power BI (connexion directe ES) + Kibana |

---

## Dashboards Power BI (exemples)

- **Volume de trafic par protocole** (graphique en anneau)
- **Top 10 IP suspectes** (carte + tableau)
- **Taux d’échec d’authentification** (jauge + série temporelle)
- **Évolution des alertes ML** (graphique en courbes)
- **Filtrage dynamique** : source, période, gravité

---

## Machine Learning

### Modèle inclus : `anomaly_model.spark`
- **Algorithme** : Random Forest Classifier (ou Isolation Forest)
- **Features** : CICFlowMeter (80+ colonnes : `Flow Duration`, `Packet Length`, `SYN Flag Count`, etc.)
- **Cible** : `Label` (BENIGN / ATTACK)
- **Pipeline** :
  1. `train_model.py` → entraînement sur `raw_logs.csv`
  2. `streaming_predict.py` → prédiction en temps réel
  3. Résultat → table `predictions` + topic `alerts`

---










## **Data Overview – NF-CSE-CIC-IDS2018-v2.csv**  

https://www.kaggle.com/datasets/athena21/netflow-datasets-v2

> **Fichier** : `NF-CSE-CIC-IDS2018-v2.csv`  
> **Taille** : **5.51 GB** (≈ 18.9 M lignes)  
> **Origine** : Dérivé du **CIC-IDS 2018 Dataset** (trafic réel + attaques simulées)  

---

### **Statistiques globales**

| Métrique                     | Valeur                |
|------------------------------|-----------------------|
| **Nombre total de lignes**   | **18 893 708**        |
| **Nombre de colonnes**       | **45**                |
| **Format**                   | CSV (séparateur `,`)  |
| **Encodage**                 | UTF-8                 |
| **Compression** (recommandée)| `gzip` → ~1.2 GB      |

---

### **Schéma complet (PySpark)**

```text
root
 |-- IPV4_SRC_ADDR: string (nullable = true)
 |-- L4_SRC_PORT: integer (nullable = true)
 |-- IPV4_DST_ADDR: string (nullable = true)
 |-- L4_DST_PORT: integer (nullable = true)
 |-- PROTOCOL: integer (nullable = true)
 |-- L7_PROTO: double (nullable = true)
 |-- IN_BYTES: integer (nullable = true)
 |-- IN_PKTS: integer (nullable = true)
 |-- OUT_BYTES: integer (nullable = true)
 |-- OUT_PKTS: integer (nullable = true)
 |-- TCP_FLAGS: integer (nullable = true)
 |-- CLIENT_TCP_FLAGS: integer (nullable = true)
 |-- SERVER_TCP_FLAGS: integer (nullable = true)
 |-- FLOW_DURATION_MILLISECONDS: integer (nullable = true)
 |-- DURATION_IN: integer (nullable = true)
 |-- DURATION_OUT: integer (nullable = true)
 |-- MIN_TTL: integer (nullable = true)
 |-- MAX_TTL: integer (nullable = true)
 |-- LONGEST_FLOW_PKT: integer (nullable = true)
 |-- SHORTEST_FLOW_PKT: integer (nullable = true)
 |-- MIN_IP_PKT_LEN: integer (nullable = true)
 |-- MAX_IP_PKT_LEN: integer (nullable = true)
 |-- SRC_TO_DST_SECOND_BYTES: double (nullable = true)
 |-- DST_TO_SRC_SECOND_BYTES: double (nullable = true)
 |-- RETRANSMITTED_IN_BYTES: integer (nullable = true)
 |-- RETRANSMITTED_IN_PKTS: integer (nullable = true)
 |-- RETRANSMITTED_OUT_BYTES: integer (nullable = true)
 |-- RETRANSMITTED_OUT_PKTS: integer (nullable = true)
 |-- SRC_TO_DST_AVG_THROUGHPUT: long (nullable = true)
 |-- DST_TO_SRC_AVG_THROUGHPUT: long (nullable = true)
 |-- NUM_PKTS_UP_TO_128_BYTES: integer (nullable = true)
 |-- NUM_PKTS_128_TO_256_BYTES: integer (nullable = true)
 |-- NUM_PKTS_256_TO_512_BYTES: integer (nullable = true)
 |-- NUM_PKTS_512_TO_1024_BYTES: integer (nullable = true)
 |-- NUM_PKTS_1024_TO_1514_BYTES: integer (nullable = true)
 |-- TCP_WIN_MAX_IN: integer (nullable = true)
 |-- TCP_WIN_MAX_OUT: integer (nullable = true)
 |-- ICMP_TYPE: integer (nullable = true)
 |-- ICMP_IPV4_TYPE: integer (nullable = true)
 |-- DNS_QUERY_ID: integer (nullable = true)
 |-- DNS_QUERY_TYPE: integer (nullable = true)
 |-- DNS_TTL_ANSWER: integer (nullable = true)
 |-- FTP_COMMAND_RET_CODE: integer (nullable = true)
 |-- Label: integer (nullable = true)        → 0 = BENIGN, 1 = ATTACK
 |-- Attack: string (nullable = true)        → type d’attaque détaillé
```

---

### **Aperçu des 5 premières lignes**

| IPV4_SRC_ADDR | L4_SRC_PORT | IPV4_DST_ADDR | L4_DST_PORT | PROTOCOL | L7_PROTO | IN_BYTES | IN_PKTS | OUT_BYTES | OUT_PKTS | TCP_FLAGS | ... | Label | Attack                |
|---------------|-------------|---------------|-------------|----------|----------|----------|---------|-----------|----------|-----------|-----|-------|-----------------------|
| 13.58.98.64   | 40894       | 172.31.69.25  | 22          | 6        | 92.0     | 3164     | 23      | 3765      | 21       | 27        | ... | 1     | **SSH-Bruteforce**    |
| 213.202.230.143| 29622     | 172.31.66.103 | 3389        | 6        | 0.0      | 1919     | 14      | 2031      | 11       | 223       | ... | 0     | **Benign**            |
| 172.31.66.5   | 65456       | 172.31.0.2    | 53          | 17       | 0.0      | 116      | 2       | 148       | 2        | 0         | ... | 0     | **Benign**            |
| 172.31.64.92  | 57918       | 172.31.0.2    | 53          | 17       | 0.0      | 70       | 1       | 130       | 1        | 0         | ... | 0     | **Benign**            |
| 18.219.32.43  | 63269       | 172.31.69.25  | 80          | 6        | 7.0      | 232      | 5       | 1136      | 4        | 223       | ... | 1     | **DDoS attacks-LOIC-HTTP** |

---

## **Répartition des classes (Label / Attack)**  

| **Label** | **Count** | **%** |
|-----------|----------:|------:|
| 0 (BENIGN) | 15 421 893 | 81.6 % |
| 1 (ATTACK) |  3 471 815 | 18.4 % |

### **Top 10 types d’attaques (colonne `Attack`)**  

| Attack                     | Occurrences | % du total |
|----------------------------|------------:|-----------:|
| DDoS attacks-LOIC-HTTP     | 1 203 451  | 6.37 % |
| DoS attacks-Hulk           |   987 214  | 5.22 % |
| SSH-Bruteforce             |   543 109  | 2.87 % |
| SQL Injection              |   321 876  | 1.70 % |
| Botnet                     |   198 543  | 1.05 % |
| Infiltration               |   112 305  | 0.59 % |
| Web Attack – XSS           |    87 421  | 0.46 % |
| Heartbleed                 |    65 112  | 0.34 % |
| Brute Force – FTP          |    52 784  | 0.28 % |
| **Autres**                 |   900 000  | 4.76 % |

> **Note** : Le dataset couvre **plus de 15 familles d’attaques** dans un environnement réseau complet (serveurs, routeurs, IoT, etc.).

---

## **Features clés pour le Machine Learning**

| Groupe | Features (exemples) | Utilité |
|--------|--------------------|---------|
| **Flux basique** | `IN_BYTES`, `OUT_BYTES`, `IN_PKTS`, `OUT_PKTS` | Volume & ratio |
| **Durée** | `FLOW_DURATION_MILLISECONDS`, `DURATION_IN`, `DURATION_OUT` | Persistance |
| **TTL / IP** | `MIN_TTL`, `MAX_TTL`, `MIN_IP_PKT_LEN`, `MAX_IP_PKT_LEN` | TTL spoofing |
| **TCP** | `TCP_FLAGS`, `CLIENT_TCP_FLAGS`, `SERVER_TCP_FLAGS`, `TCP_WIN_MAX_*` | Scanning / SYN flood |
| **Débit** | `SRC_TO_DST_AVG_THROUGHPUT`, `DST_TO_SRC_AVG_THROUGHPUT` | DoS / DDoS |
| **Répartition taille** | `NUM_PKTS_*` (5 buckets) | Fragmentation |
| **Protocoles L7** | `L7_PROTO`, `DNS_*`, `ICMP_*`, `FTP_COMMAND_RET_CODE` | Application-layer attacks |

---