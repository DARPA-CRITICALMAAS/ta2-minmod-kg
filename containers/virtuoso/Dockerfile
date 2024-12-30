FROM openlink/virtuoso-opensource-7:7.2.14

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=US/Pacific

RUN apt update && apt install -y python3 python3-pip default-jre

RUN mkdir -p /criticalmaas/build && \
    mkdir -p /criticalmaas/dataloader && \
    pip3 install psutil JayDeBeApi

# Copy the default virtuoso.ini file
ADD virtuoso.ini.template /criticalmaas/build/
ADD make_config.py /criticalmaas/build/
ADD data_loader.py /criticalmaas/build/
ADD setup.sql /initdb.d/

# Make the config file
RUN python3 /criticalmaas/build/make_config.py

ENV DBA_PASSWORD=dba
ENV CLASSPATH="$CLASSPATH:/opt/virtuoso-opensource/lib/jdbc-4.3/virtjdbc4_3.jar"
ENV VIRTUOSO_INI_FILE=/criticalmaas/build/virtuoso.ini
