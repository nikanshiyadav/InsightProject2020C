# ETL Sans Servers


In a typical data science project 80% of time goes in data cleaning, a lot which is of repetetive nature. A customized ETL pipeline can help data scientists sace their valuable time, 
and leveraging the cloud infrastucture we can build one that doesn't add a burden on computation & storage. 
Using AWS S3, Lambda, Glue and Athena I designed a serverless ETL pipeline that ingests unprocessed data from multiple sources and transforms it for analysis. 

## Architecture
* __S3__ for Raw Data Storage
* __Lambda__ to trigger tasks
* __Glue Pyspark ETL__ jobs to clean and transgorm the data
* __Glue Catalogue__ to store metadata
* __Athena__ to query data using SQL
* __Quicksight__ to visualise data

## Data
[Chicago Crime Dataset](https://www.kaggle.com/chicago/chicago-crime)

__Disclaimer:__ Since I worked on a consultiong project, here I am deploying the pipeline with a diffrent dataset to protect the privacy of organisation involved. 
The tech stack and code is similar to the one used in my work for the consulting project.


