FROM postgres:17-bookworm

ARG UID=1000
ARG GID=1000

RUN groupadd -f -g $GID minmod && useradd -ms /bin/bash minmod -u $UID -g $GID
RUN mkdir /database && chown -R minmod:minmod /database

ENV POSTGRES_USER=minmod
ENV POSTGRES_PASSWORD=criticalmaas2025
ENV POSTGRES_DB=minmod
ENV PGDATA=/database

USER minmod