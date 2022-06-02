import boto3 
from io import BytesIO
import gzip
import json
from datetime import date 
from datetime import timedelta 
import datetime
import numbers
s3 = boto3.resource(
    's3',
    aws_access_key_id='XXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXxxx'
)
bucket = s3.Bucket('XXXXXXXXXXXXXx')

newBucket = s3.Bucket('XXXXXXXXXXXXXX')

today = date.today()

yesterday = today - timedelta(days=1)

Prefix = 'XXXXXXXXXXXXXXX/utcDate=' + str(yesterday)

print('job started for - '+str(yesterday))
for obj in bucket.objects.filter(Prefix = Prefix):
    paths = obj.key.split('/')
    if(len(paths) < 5):
        continue
    kpi = paths[3]
    
    NewFileName = obj.key[:-3]
    
    if(kpi == '_SUCCESS'):
        continue
    else:
        body = obj.get()['Body'].read()
        gzipfile = BytesIO(body)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()

        #if(kpi == "actions" or kpi == "componentShows" or kpi == "inlineVideoPlays" or kpi == "inlineVideoQuarters" or kpi == "inlineVideoSeconds" or kpi == "locatorOccurrences" or kpi == "screenShows" or kpi == "sessions" or kpi == "unitShows" or kpi == "userErrors") : 
        newContent = content.decode('utf8').replace('customCampaignAttributes[campaignAgencyName]','customCampaignAttributes_campaignAgencyName').replace('{}','"{}"')
        if(kpi == "sessions"):
            sessionStr = ''
            for item in newContent.strip().split('\n'):
                jsonObj = json.loads(item)
                jsonObj["dynamicFeedContent"] = "{}"
                sessionStr+=json.dumps(jsonObj)+'\n'
            newContent = sessionStr
        if(kpi == "inlineVideoQuarters" or kpi == "inlineVideoPlays" or kpi == 'inlineVideoSeconds'):
            sessionStr = ''
            for item in newContent.strip().split('\n'):
                jsonObj = json.loads(item)
                if(isinstance(jsonObj["inlineVideoLocalId"], numbers.Number) == 0):
                    jsonObj["inlineVideoLocalId"] = 0
                sessionStr+=json.dumps(jsonObj)+'\n'
            newContent = sessionStr
        newBucket.put_object(Key=NewFileName, Body=newContent)
print('job completed for - '+str(yesterday))
print('-----------------------------------------------')