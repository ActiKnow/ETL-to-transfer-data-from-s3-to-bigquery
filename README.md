# ETL to transfer data from s3 to bigquery

Technology Used : Python, bigquery, aws s3, bigquery data transfer service

In this process we fetch the data from s3 and then transform it according to our requirement and then push new data to another s3 storage.
Then we have created data transfer jobs in bigquery to fetch formated data from new s3 storage to bigqurey dataset

It transfer data of 9 kpi's for each hour from s3 to bigquery daily
