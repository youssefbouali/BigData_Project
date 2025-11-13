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