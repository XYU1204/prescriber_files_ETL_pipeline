############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script Name:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################
  
# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_azure.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/home/xyu041262/PrescPipeline/python/logs/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

### Define Local Directories
LOCAL_OUTPUT_PATH="/home/xyu041262/PrescPipeline/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

### Define SAS URLs
citySasUrl="https://prescriberpipelineproj.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=2022-11-08T19:51:58Z&se=2022-11-09T03:51:58Z&si=writeaccess&spr=https&sv=2021-06-08&sr=c&sig=%2F8A2WK3LoupoE%2F6a61EY8jffJ5cIokA%2FbW5s3Zi1yiQ%3D"
prescSasUrl="https://prescriberpipelineproj.blob.core.windows.net/presc/${bucket_subdir_name}?st=2022-11-08T19:54:03Z&se=2022-11-09T03:54:03Z&si=writeaccess&spr=https&sv=2021-06-08&sr=c&sig=cPdd%2FnhIxFal1mp6R8AGRTxrQB29F4G8GGtv6saFsxE%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
