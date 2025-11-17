FROM cassandra:4.1

# Créer utilisateur + groupe avec ton UID/GID local
ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} cassgroup && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash cassuser && \
    mkdir -p /home/cassuser/.cassandra && \
    chown -R cassuser:cassgroup /home/cassuser/.cassandra

# Changer les droits sur les répertoires critiques
RUN chown -R cassuser:cassgroup /var/lib/cassandra /var/log/cassandra

# Passer en utilisateur non-root
USER cassuser

# Répertoire de travail
WORKDIR /home/cassuser