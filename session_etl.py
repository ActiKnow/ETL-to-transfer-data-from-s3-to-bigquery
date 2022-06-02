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
    aws_access_key_id='AKIAVE3VHLPFRRARDLEC',
    aws_secret_access_key='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'
)

bucket = s3.Bucket('celtrareporting')

newBucket = s3.Bucket('etl-celtrareporting')

today = date.today()

yesterday = today - timedelta(days=1)



#print('job started for - '+str(yesterday))
#s3://etl-celtrareporting/Celtra-Reporting/utcDate={run_time-1d|"%Y-%m-%d"}/utcHour=*/actions/*.json
#celtrareporting/Celtra-Reporting/utcDate=2021-01-14/utcHour=07/actions/
date = datetime.date(2021, 4, 4)
for x in range(1):
    date += datetime.timedelta(days=1)
    Prefix = 'Celtra-Reporting/utcDate='+str(date)
    for y in range(0,24):
        Prefix_1= Prefix+'/utcHour='+str("{:02d}".format(y))+'/inlineVideoQuarters/'
        print(Prefix_1)
        for obj in bucket.objects.filter(Prefix = Prefix_1):
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
                if(kpi == "inlineVideoQuarters"):
                    sessionStr = ''
                    for item in newContent.strip().split('\n'):
                        jsonObj = json.loads(item)
                        if(isinstance(jsonObj["inlineVideoLocalId"], numbers.Number) == 0):
                            jsonObj["inlineVideoLocalId"] = 0
                        sessionStr+=json.dumps(jsonObj)+'\n'
                    newContent = sessionStr
                newBucket.put_object(Key=NewFileName, Body=newContent)
        #print('job completed for - '+str(yesterday))
print('-----------------------------------------------')