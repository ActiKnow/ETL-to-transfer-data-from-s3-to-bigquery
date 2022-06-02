import os
import boto3
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
import google.protobuf.json_format
import datetime
assert bigquery.Client().project == 'xxxxxxxxxxxxxxxxxxx'

client = bigquery_datatransfer.DataTransferServiceClient()

project_id = "xxxxxxxxxxxxxxxxxxx"

# Get the full path to your project.
parent = client.common_project_path(project_id)

transfer_client = bigquery_datatransfer.DataTransferServiceClient()

configs = transfer_client.list_transfer_configs(parent=parent)
#print("Got the following configs:")
for config in configs:
    if(config.disabled):
        print(config.display_name)
        #print(f"\tID: {config.name}, Schedule: {config.schedule}")
        transfer_config = client.delete_transfer_config(name=config.name)

print('done')

