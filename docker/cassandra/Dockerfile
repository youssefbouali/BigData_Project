# docker/cassandra/Dockerfile
FROM cassandra:4.1

# Crée un utilisateur avec UID/GID 1000 (ton utilisateur local)
RUN groupadd -g 1000 cassgroup && \
    useradd -u 1000 -g cassgroup -m -s /bin/bash cassuser

# Change le propriétaire du répertoire de données
RUN chown -R cassuser:cassgroup /var/lib/cassandra /var/log/cassandra

USER cassuser