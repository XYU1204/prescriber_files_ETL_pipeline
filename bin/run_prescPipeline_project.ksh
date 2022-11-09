### Part 1
### Call the copy_to_hdfs wrapper.
#printf "Calling copy_files_local_to_hdfs.ksh ..."
#/home/sibaramnanda2021/projects/PrescPipeline/src/main/python/bin/copy_files_local_to_hdfs.ksh
#printf "Executing copy_files_local_to_hdfs.ksh is completed.\n"

### Call below wrapper to delete HDFS Paths.
printf "Calling delete_hdfs_output_paths.ksh ..."
/home/sibaramnanda2021/projects/PrescPipeline/src/main/python/bin/delete_hdfs_output_paths.ksh
printf "Executing delete_hdfs_output_paths.ksh is completed.\n"

### Call below Spark Job to extract Fact and City Files
printf "Calling run_presc_pipeline.py ..."
spark3-submit --master yarn --num-executors 28 run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed."

### Part 2
### Call below script to copy files from HDFS to local.
printf "Calling copy_files_hdfs_to_local.ksh ..."
/home/sibaramnanda2021/projects/PrescPipeline/src/main/python/bin/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is completed.\n"

### Call below script to copy files to S3.
printf "Calling copy_files_to_s3.ksh ..."
/home/sibaramnanda2021/projects/PrescPipeline/src/main/python/bin/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is completed.\n"

### Call below script to copy files to Azure.
printf "Calling copy_files_to_azure.ksh ..."
/home/sibaramnanda2021/projects/PrescPipeline/src/main/python/bin/copy_files_to_azure.ksh
printf "Executing copy_files_to_azure.ksh is completed.\n"
