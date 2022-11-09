# Import all the necessary Modules
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from subprocess import Popen, PIPE
from presc_run_data_extraction import extract_files
from presc_run_data_persist import data_persist_hive, data_persist_postgre

# Load the Logging Configuration File
logging.config.fileConfig(fname="../util/logging_to_file.conf")

def main():
    try:
        # Get Spark Object
        logging.info("main() is started ...")
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        # Initiate run_presc_data_ingest Script
        # Load the City File
        file_dir1 = "PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir1], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'csv' in out.decode():
            file_format1 = 'csv'
            header1 = gav.header
            inferSchema1 = gav.inferSchema
        elif 'parquet' in out.decode():
            file_format1 = 'parquet'
            header1 = 'NA'
            inferSchema1 = 'NA'

        df_city = load_files(spark, file_dir1, file_format1, header1, inferSchema1)

        # Load the Prescriber Fact File
        file_dir2 = "PrescPipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir2], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'csv' in out.decode():
            file_format2 = 'csv'
            header2 = gav.header
            inferSchema2 = gav.inferSchema
        elif 'parquet' in out.decode():
            file_format2 = 'parquet'
            header2 = 'NA'
            inferSchema2 = 'NA'

        df_fact = load_files(spark=spark, file_dir=file_dir2, file_format=file_format2, header=header2,
                             inferSchema=inferSchema2)

        # Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        # Initiate run_presc_data_preprocessing Script
        # Perform data Cleaning Operations
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        
        # Validate df_city_sel and df_fact_sel
        df_top10_rec(df_city_sel, "df_city_sel")
        df_top10_rec(df_fact_sel, "df_fact_sel")
        df_print_schema(df_fact_sel, "df_fact_sel")

        # Initiate run_presc_data_transform Script
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)
        df_city_final.write.csv("../output/city_dimension/df_city_final.csv")
        df_presc_final.write.csv("../output/presc/df_presc_final.csv")
        
        # Validation for df_city_final
        df_top10_rec(df_city_final, "df_city_final")
        df_print_schema(df_city_final, "df_city_final")
        df_top10_rec(df_presc_final, "df_presc_final")
        df_print_schema(df_presc_final, "df_presc_final")

        # Initiate run_data_extraction Script
        CITY_PATH = gav.output_city
        extract_files(df_city_final, "json", CITY_PATH, 1, False, 'bzip2')

        PRESC_PATH = gav.output_fact
        extract_files(df_presc_final, "orc", PRESC_PATH, 2, False, 'snappy')
        
        # Persist Data
        # Persist data at Hive
        data_persist_hive(spark = spark, df = df_city_final ,dfName = 'df_city_final' ,partitionBy = 'delivery_date',mode='append')
        data_persist_hive(spark = spark, df = df_presc_final,dfName = 'df_presc_final',partitionBy = 'delivery_date',mode='append')

        # persist data at Postgre
        data_persist_postgre(spark=spark, df=df_city_final, dfName='df_city_final', url="jdbc:postgresql://localhost:6432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user=gav.user, password=gav.password)

        data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://localhost:6432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user=gav.user, password=gav.password)

        # End of Application Part 1
        logging.info('presc_run_pipeline.py is completed.')
    except Exception as exp:
        logging.error("Error occurred in the main() method. Please check the Stack Trace "
                      "to go to the respective module and fix it. " + str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info('run_presc_pipeline is Started ...')
    main()
