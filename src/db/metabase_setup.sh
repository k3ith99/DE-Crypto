#!/bin/bash
#reason for reptition is metabase first creates example dashboards when metabase application db is initialised which is a must step.
#After initial connection and setup, we recreate metabase db and this time with our own metabase dashboards
docker exec -i postgres1 psql -U postgres -c "CREATE DATABASE metabase"; 
sleep 5;
echo "stopping metabase"
docker stop metabase
echo "dropping metabase"
docker exec -i postgres1 psql -U postgres -c "DROP DATABASE METABASE WITH (FORCE)";
echo "creating metabase"
docker exec -i postgres1 psql -U postgres -c "CREATE DATABASE metabase"; 
echo "updating metabase"
docker exec -i postgres1 sh -c "psql -U postgres -d metabase -f metabase_backup.sql"

docker start metabase