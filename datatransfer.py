import os
import boto3
import time
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
import google.protobuf.json_format
import datetime
from google.protobuf.timestamp_pb2 import Timestamp

assert bigquery.Client().project == 'xxxxxxxxxxxxxxxxxxxxx'

client = bigquery_datatransfer.DataTransferServiceClient()

parent = client.common_project_path('xxxxxxxxxxxxxxxxxxxxx')

alltable=[#'actions','componentShows','inlineVideoPlays',
'inlineVideoQuarters','inlineVideoSeconds','locatorOccurrences','screenShows','sessions','unitShows','userErrors'
    ]
unixTime = int(datetime.datetime(2021, 11, 1, 17, 30, 00).strftime('%s'))
rundate = Timestamp(seconds=unixTime)
# print(rundate)
# exit()
#timeRange = [19,20,21,22,23,24]
date = datetime.date(2021, 10, 31)
for tn in alltable:
    
    #print(tn)
    tableName = tn
    #tableName = 'sessions'
    
    for x in range(1):
        #date += datetime.timedelta(days=1)
        print(tableName)
        #hour = "{0:0=2d}".format(x)
        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id= "XXXXXXXXXXXXXX",
            #display_name= tableName+"-quad4-hour-"+hour,
            display_name= tableName+'-'+str(date),
            #display_name= tableName,
            data_source_id= "amazon_s3",
            params= {
                "destination_table_name_template": tableName,
                #"data_path": 's3://etl-celtrareporting/Celtra-Reporting/utcDate={run_time-24h|"%Y-%m-%d"}/utcHour='+hour+'/'+tableName+'/*.json',
                #"data_path": 's3://etl-celtrareporting/Celtra-Reporting/utcDate={run_time|"%Y-%m-%d"}/utcHour='+hour+'/'+tableName+'/*.json',
                "data_path": "s3://etl-xxxxxxxxx/xxxxx/utcDate="+str(date)+"/*/"+tableName+"/*.json",
                "access_key_id": 'xxxxxxxxxx',
                "secret_access_key": 'xxxxxxxxxxxxxx',
                "file_format": "JSON",
                "ignore_unknown_values":"true"
            },
            schedule="every 24 hours",
            schedule_options= {
                #'disable_auto_scheduling': True,
                #"start_time": rundate,
            },
        )
        
        transfer_config = client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config,
        )
        time.sleep(5)
print('done')


