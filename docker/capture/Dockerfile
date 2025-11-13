FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    tcpdump \
    nfdump \
    openjdk-11-jre \
    python3-pip \
    inotify-tools \
    wget \
    && rm -rf /var/lib/apt/lists/*

# install CICFlowMeter
RUN wget -q https://github.com/ahlashkari/CICFlowMeter/raw/master/CICFlowMeter-0.1.jar -O /CICFlowMeter.jar

# install Python
RUN pip3 install kafka-python pandas watchdog

# copy scripts
COPY capture.sh /capture.sh
COPY convert_and_send.py /convert_and_send.py
RUN chmod +x /capture.sh /convert_and_send.py

CMD ["/capture.sh"]