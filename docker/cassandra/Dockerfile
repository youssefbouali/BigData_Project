FROM cassandra:4.1

# === MODIFICATIONS EN ROOT ===
ARG UID=1000
ARG GID=1000

# CORRIGÉ : "" au lieu de 0.0.0.0
RUN sed -i 's/listen_address: localhost/listen_address: ""/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/rpc_address: localhost/rpc_address: ""/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/# broadcast_address: .*/broadcast_address: cassandra/' /etc/cassandra/cassandra.yaml && \
    sed -i 's/# broadcast_rpc_address: .*/broadcast_rpc_address: cassandra/' /etc/cassandra/cassandra.yaml && \
    sed -i '/seed_provider/,/seeds:/ s/seeds: \"127.0.0.1\"/seeds: \"cassandra\"/' /etc/cassandra/cassandra.yaml

# Crée utilisateur
RUN groupadd -g ${GID} cassgroup && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash cassuser && \
    mkdir -p /home/cassuser/.cassandra && \
    chown -R cassuser:cassgroup /home/cassuser/.cassandra

# Droits sur les répertoires
RUN chown -R cassuser:cassgroup /var/lib/cassandra /var/log/cassandra

# Passage en non-root
USER cassuser
WORKDIR /home/cassuser