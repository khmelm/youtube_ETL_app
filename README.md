# Youtube ETL Tool
![Python](https://img.shields.io/badge/Python-3.x-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.x-green) ![PostgresSQL](https://img.shields.io/badge/Postgres-14.x-orange) 


## Краткое описание
Airflow DAG, который собирает данные о выходе новых видео на YouTube по заданным тематикам  и сохраняет данные в базе данных.

## Что сделано
1) SQL скрипт по созданию структуры базы данных
2) Набор SQL скриптов, используемых для трансформации данных и их переноса из staging в data mart
3) .py файл с Airflow DAG
