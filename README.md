# DE-Crypto


## Project goal
To build an end to end ETL pipeline that ingests cryptocurrency data from binance and visualises different metrics for learning and experimentation

## Installation & Usage
### Prerequisites
Ensure pipenv and docker are installed

### Set up instructions
1. Clone repo
2. Install dependencies using pipenv install and enter using pipenv shell
3. Create main.env files in shared folder and db folder
5. cd src/db and run docker-compose up to start infrastructure (minio,postgres and metabase)
After startup, access services at:
* MinIO: http://localhost:9000
  * Username: root
  * Password: minioadmin
* PgAdmin: http://localhost:5050
  * Username: postgres
  * Password: postgres
7. Apply permissions and setup dashboard using:
```
chmod + metabase_setup.sh
./metabase_setup.sh
```
7. Run ingestion pipeline:
```
  cd ./../pipelines
  docker-compose -f docker-compose.ingest.yaml up --build -d
```
8. Run transformation pipeline:
``` docker-compose -f docker-compose.transform.yaml up --build -d ```
10. Head to http://localhost:3000 and log in with:
   * Username:crypto@outlook.com
   * Password:King2030
11. Head to http://localhost:3000/admin/databases and enter the ip address of postgres1 container and save. You can obtain this using:
``` docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' postgres1 ```
12. Once that is completed, view dashboards at http://localhost:3000/
13. Start airflow with ``` Astro dev start ```
## Design & Implementation
### Schema
![image](https://github.com/user-attachments/assets/077b4c48-89f3-42ea-9787-53dc895f116b)


### Technologies
- Minio as object store for raw data
- Binance API as the data source
- Postgres - as a data warehouse
- Metabase for visualisation dashboards
- Spark for transformation
- Docker for containerisation and orchestration
- Pgadmin to interact with postgresdb


## Bugs
* Correct Python interpreter does not load when pipenv shell is ran(May need to remove environment,exit shell and run pipenv commands again for script to run
- Need to use config file 

## Tech stack
 - Minio
 - Docker
 - Spark
 - Metabase
 - Binance API
 - Postgresdb
 - Airflow
## Improvements/Future features
- Include calculated metrics such as beta, moving averages and STD
- Use sql for transformations instead of spark
- Apply financial models such as CAPM
- Apply AI & ML models
- Include hourly data
- Apply ETL testing


