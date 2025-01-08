FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y \
    gettext

COPY init_db.sql /sql/
COPY init.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init.sh