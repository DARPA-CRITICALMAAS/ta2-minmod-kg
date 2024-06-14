FROM eclipse-temurin:21-jre

RUN cd tmp && \
    wget https://dlcdn.apache.org/jena/binaries/apache-jena-5.0.0.tar.gz && \
    wget https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-5.0.0.tar.gz && \
    tar -xzf apache-jena-5.0.0.tar.gz && \
    rm apache-jena-5.0.0.tar.gz && \
    mv apache-jena-5.0.0 /opt/jena && \
    tar -xzf apache-jena-fuseki-5.0.0.tar.gz && \
    rm apache-jena-fuseki-5.0.0.tar.gz && \
    mv apache-jena-fuseki-5.0.0 /opt/fuseki

COPY --chmod=+x dataloader.sh /opt/
COPY config.ttl /opt/fuseki/config.ttl
# COPY shiro.ini /opt/fuseki/shiro.ini
# COPY users /opt/fuseki/users
# COPY users /users