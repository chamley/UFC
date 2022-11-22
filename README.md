<h3>Code Status:</h3>


[![Build Status](https://app.travis-ci.com/chamley/UFC.svg?branch=main)](https://app.travis-ci.com/chamley/UFC)
![Github last-commit](https://img.shields.io/github/last-commit/chamley/UFC)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


Hi. This is my personal project where I hope to aggregate all available data in the world about mma in the UFC league.

<b>Ingestion System and Transformation Layer:</b> Orchestrated Lambdas.

<b>Scalibility:</b> Lambdas allow for scale up as well as scale down.

<b>Data Architecture:</b> Two-Tier architecture leveraging a data lake and a data warehouse. 

<b>Orchestration:</b> Airflow with certain handoffs to dbt cloud.

<h3>Stack</h3>

💻 Data Engineering Tools: Airflow,  Docker, DBT, 

☁️ Cloud: AWS (Lambda, S3, Redshift, ECR, MWAA)

✅ CI/Testing: DBT tests, Travis CI, Pytest

📊 Visualization: Tableau

📚 Libraries: pandas, boto3, awswrangler, beautifulsoup, psycop2g

🌈 Languages: Python/SQL (Redshift's version of Postgres)

🧰 Workflow Tools: Black (linter), VSCode, Datagrip


![data architecture](misc/architecture.jpeg)
![Example Dashboard 1](misc/d1.png)
![Example Dashboard 2](misc/d2.png)

ToDo:
- Dashboards not deployed due to TableauServer not having a free tier and my Redshift/MWAA costs lightly bankrupting me.
- finish terraform migration (Lambda, MWAA, + IAM and terraform cloud)
- CD



