# DE-Crypto


## Project goal
Build a cryptocurrency dashboard to view price by pulling data from binance and use this as an opportunity to upskill and implement best practices.

## Installation & Usage
1. Ensure pipenv and docker are installed
2. Clone repo
3. Install dependencies using pipenv install and enter using pipenv shell]
4. Create main.env files in shared folder and db folder
5. Enter src/db and run docker-compose up to start minio,postgres and metabase
6. Apply permissions chmod + metabase_setup.sh and run metabase_setup.sh to prepare dashboards
7. cd into src/pipelines and run docker-compose -f docker-compose.ingest.yaml up --build -d to run ingestion from binance
8. Run docker-compose -f docker-compose.transform.yaml up --build -d to run transformation pipeline
9. Head to http://localhost:3000/admin/databases and enter the ip address of postgres1 container and save. You can obtain this using docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres1  
10. Once that is completed, view dashboards at http://localhost:3000/
## Design & Implementation
[Schema](https://dbdiagram.io/d/DE-Crypto-6762e4b084c741072719d5dd)



### Technologies
- Minio
- Binance API
- Postgres
- Metabase
- Python
- Spark
- Docker
- Pgadmin


## Bugs
* Correct Python interpreter does not load when pipenv shell is ran(May need to remove environment,exit shell and run pipenv commands again for script to run
- Need to use config file 


## Fixed Bugs


## Improvements & Future Features
- Implement daily cron job for new data
- Use airflow for orchestration and scheduling
- Include more metrics such as moving averages
- Include hourly data






Data engineering project featuring cryptocurrencies

## Project goal
To build an end to end ETL pipeline that ingests cryptocurrency data from binance and build dashboards of different metrics for learning purposes
## Installation & Guide

## Design & Implementation testing

### Schema
![DE-crypto-dbd](https://github.com/user-attachments/assets/70273554-28e1-4aec-b287-b2da7e524e50)


## Tech stack
 - Minio
 - Docker
 - Spark
 - Metabase
 - Binance API
 - Postgresdb
## Bugs


## Improvements/Future features
- Include calculated metrics such as beta, moving averages and STD
- Use sql for transformations instead of spark
- Apply financial models such as CAPM
- Apply AI & ML models
- Use Airflow for orchestration
- Run daily cron jobs
=======
## steps to set up postgres db locally (macos)
1. Brew install postgresql
2. Brew services restart postgresql
3. createuser -s postgres
4. brew services restart postgresql
Then install pgadmin4 and manually create a server there

