FROM cassandra:4.1

ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} cassgroup && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash cassuser && \
    mkdir -p /home/cassuser/.cassandra && \
    chown -R cassuser:cassgroup /home/cassuser/.cassandra

RUN chown -R cassuser:cassgroup /var/lib/cassandra /var/log/cassandra

USER cassuser
WORKDIR /home/cassuser