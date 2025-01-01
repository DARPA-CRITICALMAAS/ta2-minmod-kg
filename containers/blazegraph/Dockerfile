FROM eclipse-temurin:21-jre

RUN apt update && apt install -y python3 python3-pip

ARG UID=1000
ARG GID=1000
ARG MEMORY=12G

# custom code for eclipse-temurin:21-jdk to free up 1000:1000
RUN userdel ubuntu
RUN groupadd -f -g $GID criticalmaas && useradd -ms /bin/bash criticalmaas -u $UID -g $GID


USER criticalmaas
WORKDIR /home/criticalmaas
ENV JVM_ARGS="-Xmx$MEMORY"

RUN mkdir -p /home/criticalmaas/config && \
    mkdir -p /home/criticalmaas/database

RUN wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar && \
    wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/bigdata.jar

ADD --chown=criticalmaas:criticalmaas blazegraph.properties /home/criticalmaas/config/blazegraph.properties
ADD --chown=criticalmaas:criticalmaas data_loader.py /home/criticalmaas/data_loader.py
ADD --chown=criticalmaas:criticalmaas start.sh /home/criticalmaas/start.sh

RUN chmod +x /home/criticalmaas/start.sh

CMD ["/home/criticalmaas/start.sh"]