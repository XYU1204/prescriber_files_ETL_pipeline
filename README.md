# prescriber_files_ETL_pipeline_using_pyspark
* Used software engineering practices (error handling, Logging, Encapsulation, Inheritance) to create a data pipeline (ingestion, preprocessing, transform, storage ,persist and transfer) in pyspark.
* Installed a single Node Cluster at Google Cloud and integrate the cluster with Spark to run the pipeline.
* Performed data transfer from HDFS storage to local, then to AWS S3 and Azure Blobs. Added Data persist on Hive.

## Code and Resources Used 
**Python Version:** 3.7  
**Packages: pyspark, logging, datetime, os, sys, re

## Extraction
In this project, I focus on building pipeline to delivery data product on cloud that is analytic ready. Therefore, there's not that much emphasize on data analytics compared to other projects. I followed a course on udemy (https://www.udemy.com/course/end-to-end-pyspark-real-time-project-implementation-spark/learn/lecture/30622440#overview) and coded along, making variations and adjustment for my machine and my desired output. 

The data sources consist of two files. One file described US cities in parquet format (
https://prescpipeline.blob.core.windows.net/input-vendor-data/city/us_cities_dimension.parquet?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D). The columns in the parquet file includes city (name), city_ascii, state_id (eg:NY), state_name (full name), county_fips (Federal Information Processing Standard code), county_name, lat (latitude), lng (longitude), population, density (population), timezone, zips (zip codes). 
![image](https://user-images.githubusercontent.com/56236129/200837110-47cb6b5e-649f-454a-8a13-fac103d2d65b.png)

Another file contains information on Healthcare prescribing in the USA, the file is 4GB large (https://prescpipeline.blob.core.windows.net/input-vendor-data/presc/USA_Presc_Medicare_Data_2021.csv?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D). The columns consist of information on prescribers, including NPI (National Provider Identifier code of provider), last and first name, city, states, specialities, and years of experiences of providers, following by information on the prescription they give out, which include drug name, generic name, claim counts, fill counts, drug cost and supply etc.
![image](https://user-images.githubusercontent.com/56236129/200836889-42f6f50b-fc85-4b8f-9ff3-20bf00c896ed.png)

The extraction process consists of [creating a spark object](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/create_objects.py)

## Transformation

## Loading
