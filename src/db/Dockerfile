FROM postgres:14.15
WORKDIR /usr/app/src
COPY ./db_init.sql /docker-entrypoint-initdb.d
#RUN chmod +x /docker-entrypoint-initdb.d/init.sh
