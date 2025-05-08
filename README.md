# DE-Crypto


## Project goal


## Installation & Usage

## Design & Implementation
[Schema](https://dbdiagram.io/d/DE-Crypto-6762e4b084c741072719d5dd)



### Technologies


## Bugs
* Correct Python interpreter does not load when pipenv shell is ran(May need to remove environment,exit shell and run pipenv commands again for script to run)


## Fixed Bugs


## Improvements & Future Features






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

