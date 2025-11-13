FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    tcpdump \
    nfdump \
    default-jre \
    python3-pip \
    inotify-tools \
    wget \
    && rm -rf /var/lib/apt/lists/*

# install CICFlowMeter (optional - will use if available)
# Try multiple possible URLs since GitHub raw links can be unreliable
RUN wget -q --timeout=10 --tries=1 https://github.com/ahlashkari/CICFlowMeter/releases/download/v4.0/CICFlowMeter-4.0.jar -O /CICFlowMeter.jar || \
    wget -q --timeout=10 --tries=1 https://github.com/ahlashkari/CICFlowMeter/raw/master/CICFlowMeter.jar -O /CICFlowMeter.jar || \
    wget -q --timeout=10 --tries=1 https://github.com/ahlashkari/CICFlowMeter/raw/master/Release/CICFlowMeter.jar -O /CICFlowMeter.jar || \
    echo "CICFlowMeter.jar not available - will continue without it" || true

# install Python
RUN pip3 install --break-system-packages kafka-python pandas watchdog

# copy scripts
COPY capture.sh /capture.sh
COPY convert_and_send.py /convert_and_send.py
RUN chmod +x /capture.sh /convert_and_send.py

CMD ["/capture.sh"]