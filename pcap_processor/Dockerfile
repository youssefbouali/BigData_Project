
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    tcpdump \
    libpcap-dev \
    build-essential \
    git \
    openjdk-11-jre-headless \
    wget \
    python3 \
    python3-pip \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*
# Install nfdump (nfcapd)
RUN wget https://github.com/phaag/nfdump/releases/download/v1.8.2/nfdump-1.8.2.tar.gz && \\
    tar xzf nfdump-1.8.2.tar.gz && cd nfdump-1.8.2 && \
    ./configure && make && make install || true

WORKDIR /pcap
COPY process_pcap.sh /usr/local/bin/process_pcap.sh
RUN chmod +x /usr/local/bin/process_pcap.sh
VOLUME ["/pcap", "/output", "/cicflowmeter"]
ENTRYPOINT ["/usr/local/bin/process_pcap.sh"]
