FROM eclipse-temurin:21-jdk

ARG UID=1000
ARG GID=1000
ARG MEMORY=12G

# custom code for eclipse-temurin:21-jdk to free up 1000:1000
RUN userdel ubuntu
RUN groupadd -f -g $GID criticalmaas && useradd -ms /bin/bash criticalmaas -u $UID -g $GID

USER criticalmaas
WORKDIR /home/criticalmaas
ENV JENA_HOME=/home/criticalmaas/jena
ENV FUSEKI_HOME=/home/criticalmaas/fuseki

RUN cd /tmp && \
    wget https://archive.apache.org/dist/jena/binaries/apache-jena-5.2.0.tar.gz && \
    wget https://archive.apache.org/dist/jena/binaries/apache-jena-fuseki-5.2.0.tar.gz && \
    tar -xzf apache-jena-5.2.0.tar.gz && \
    rm apache-jena-5.2.0.tar.gz && \
    mv apache-jena-5.2.0 $JENA_HOME && \
    tar -xzf apache-jena-fuseki-5.2.0.tar.gz && \
    rm apache-jena-fuseki-5.2.0.tar.gz && \
    mv apache-jena-fuseki-5.2.0 $FUSEKI_HOME

ADD --chown=criticalmaas:criticalmaas config.ttl ${FUSEKI_HOME}/config.ttl
ADD --chown=criticalmaas:criticalmaas test_config.ttl ${FUSEKI_HOME}/test_config.ttl

RUN mkdir /home/criticalmaas/databases

ENV PATH=$JENA_HOME/bin:$FUSEKI_HOME/:${FUSEKI_HOME}/mytdb2:$PATH
ENV JVM_ARGS="-Xmx$MEMORY"

COPY --chown=criticalmaas:criticalmaas mytdb2 ${FUSEKI_HOME}/mytdb2

RUN cd ${FUSEKI_HOME}/mytdb2 && chmod +x mytdbloader && bash compile.sh