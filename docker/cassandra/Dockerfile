# docker/cassandra/Dockerfile
FROM cassandra:4.1

# === MODIFICATIONS EN ROOT ===
ARG UID=1000
ARG GID=1000

# 1. Modifie cassandra.yaml en root
RUN sed -i 's/listen_address: localhost/listen_address: 0.0.0.0/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/rpc_address: localhost/rpc_address: 0.0.0.0/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/# broadcast_address: .*/broadcast_address: cassandra/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/# broadcast_rpc_address: .*/broadcast_rpc_address: cassandra/' /etc/cassandra/cassandra.yaml && \
    sed -i '/seed_provider/,/seeds:/ s/seeds: \"127.0.0.1\"/seeds: \"cassandra\"/' /etc/cassandra/cassandra.yaml

# 2. Crée utilisateur
RUN groupadd -g ${GID} cassgroup && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash cassuser && \
    mkdir -p /home/cassuser/.cassandra && \
    chown -R cassuser:cassgroup /home/cassuser/.cassandra

# 3. Droits sur les répertoires critiques
RUN chown -R cassuser:cassgroup /var/lib/cassandra /var/log/cassandra

# === PASSAGE EN NON-ROOT ===
USER cassuser
WORKDIR /home/cassuser