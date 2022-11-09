# prescriber_files_ETL_pipeline_using_pyspark
* Used software engineering practices (error handling, Logging, Encapsulation, Inheritance) to create a data pipeline (ingestion, preprocessing, transform, storage ,persist and transfer) in pyspark.
* Included pyspark, logging, datetime, os, sys, re (regular expressions), and pyspark.sql.functions in building the pipeline.
* Installed a single Node Cluster at Google Cloud and integrate the cluster with Spark to run the pipeline.
* Performed data transfer from HDFS storage to local, then to AWS S3 and Azure Blobs. Added Data persist on Hive.
* Delivered analytic ready data product on cloud.

## Code and Resources Used 
**Python Version:** 3.7  
**Packages: pyspark, logging, datetime, os, sys, re, pyspark.sql.functions

## Extraction
In this project, I focus on building pipeline to delivery data product on cloud that is analytic ready. Therefore, there's not that much emphasize on data analytics compared to other projects. I followed a course on udemy (https://www.udemy.com/course/end-to-end-pyspark-real-time-project-implementation-spark/learn/lecture/30622440#overview) and coded along, making variations and adjustment for my machine and my desired output. 

The data sources consist of two files. One file described US cities in parquet format (
https://prescpipeline.blob.core.windows.net/input-vendor-data/city/us_cities_dimension.parquet?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D). The columns in the parquet file includes city (name), city_ascii, state_id (eg:NY), state_name (full name), county_fips (Federal Information Processing Standard code), county_name, lat (latitude), lng (longitude), population, density (population), timezone, zips (zip codes). 
![image](https://user-images.githubusercontent.com/56236129/200837110-47cb6b5e-649f-454a-8a13-fac103d2d65b.png)

Another file contains information on Healthcare prescribing in the USA, the file is 4GB large (https://prescpipeline.blob.core.windows.net/input-vendor-data/presc/USA_Presc_Medicare_Data_2021.csv?st=2022-04-21T14:19:25Z&se=2022-12-31T22:19:25Z&si=read&spr=https&sv=2020-08-04&sr=c&sig=wjY0KtPvyy%2BbIpopBqMKAGmmSHsSvLhqL0n%2BBGFVXOQ%3D). The columns consist of information on prescribers, including NPI (National Provider Identifier code of provider), last and first name, city, states, specialities, and years of experiences of providers, following by information on the prescription they give out, which include drug name, generic name, claim counts, fill counts, drug cost and supply etc.
![image](https://user-images.githubusercontent.com/56236129/200836889-42f6f50b-fc85-4b8f-9ff3-20bf00c896ed.png)

The extraction process consists of [creating a spark object](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/create_objects.py), [loading two data files into spark object](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/presc_run_data_ingest.py), and [cleaning data](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/presc_run_data_preprocessing.py). To clean the city_dimension data, we keep only city, state_id, state_name, county_name, population and zips columns. To clean the prescription data, we keep only prescriber id, last name, first name, city state, specialty, years_of_exp, drug_name, total_claim_count, total_day_supply, and total_drug_cost. We combined prescriber first name and last name into a name column. We further clean the years_of_exp field by extracting digits in all cells under the column, and transforming them into integer format. We then check and clean all rows where prescriber ID or drug name is Nan or Null. For cells which total_claim_count for a specific drug by a specific prescriber is Nan, we assign the average of total_claim_count for the prescriber over all drugs to the Nan cell. After cleanning, the prescription table and schema structure looks like following: 

![image](https://user-images.githubusercontent.com/56236129/200845768-d131fd35-b09b-4209-a29d-bc991bb5fa62.png)


## [Transformation](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/presc_run_data_transform.py)
In the transformation step, we aim to generate two reports based on parquet and csv file. The city report includes City Name, State Name, County Name, City Population, Number of Zips, Prescriber Counts and Total claim counts. To generate the report, we follow 4 steps: calculating the Number of zips in each city, calculating the number of distinct Prescribers assigned for each City, calculating total claim count over all prescriber and all drugs for each city, and excluding a city in the final report if no prescriber is assigned to it. The city report looks like following: 
![image](https://user-images.githubusercontent.com/56236129/200850836-43934028-2c25-4944-8311-8fc397381bd6.png)

For the prescriber report, we calculate top 5 Prescribers with highest total claim count in each state, by considering the prescribers only from 20 to 50 years of experience. We first group the prescriber table by prescriber ID, name, state, and years of experience. We then calculate the sum of claim counts, drug cost, and daily supply for each prescriber over all drugs that they prescribed. We then implement a window function to list prescribers in each state by their total claim count in descending order. The final report has: 

![image](https://user-images.githubusercontent.com/56236129/200861858-ed8f002a-defa-44af-9ae8-b3c302c2fee8.png)


## Loading
In the final step, we load the reports from google cloud server (the production environment) to AWS storage and Azure container. 

The city report on AWS can be check on s3://prescriberpipelineproj/dimension_city/, and the prescriber report on AWS can be check on s3://prescriberpipelineproj/presc/. 

On Azure, these can be check on https://prescriberpipelineproj.blob.core.windows.net/presc?sp=r&st=2022-11-09T14:50:51Z&se=2022-12-31T22:50:51Z&spr=https&sv=2021-06-08&sr=c&sig=mZO5fl92IazFy3Gk7YPgRe87LVICDvYogECTAhwlKU4%3D.

We also added our reports in Hive database. However, the attempt to add reports to postgresql is unsuccessful, and I'm still learning to resolve the issue:
![image](https://user-images.githubusercontent.com/56236129/200863034-172445b0-be49-4b3c-9c5d-3412e45b0e1c.png)


## Configuration & Adaptation
we create [a separate script](https://github.com/XYU1204/prescriber_files_ETL_pipeline/blob/main/bin/get_all_variables.py) to set environment variables for running the pipeline. This separates testing and production, which makes it easier to test our pipeline in smaller scale on our local machine before pushing for production.
