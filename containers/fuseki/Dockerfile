FROM eclipse-temurin:21-jre

ARG UID=1000
ARG GID=1000

RUN groupadd -g $GID criticalmaas || useradd -ms /bin/bash criticalmaas -u $UID -g $GID

USER criticalmaas
WORKDIR /home/criticalmaas
ENV JENA_HOME=/home/criticalmaas/jena
ENV FUSEKI_HOME=/home/criticalmaas/fuseki

RUN cd /tmp && \
    wget https://dlcdn.apache.org/jena/binaries/apache-jena-5.0.0.tar.gz && \
    wget https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-5.0.0.tar.gz && \
    tar -xzf apache-jena-5.0.0.tar.gz && \
    rm apache-jena-5.0.0.tar.gz && \
    mv apache-jena-5.0.0 $JENA_HOME && \
    tar -xzf apache-jena-fuseki-5.0.0.tar.gz && \
    rm apache-jena-fuseki-5.0.0.tar.gz && \
    mv apache-jena-fuseki-5.0.0 $FUSEKI_HOME

ADD config.ttl ${FUSEKI_HOME}/config.ttl

ENV PATH=$JENA_HOME/bin:$FUSEKI_HOME/:$PATH