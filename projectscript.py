import boto3
import csv
import codecs

s3 = boto3.resource('s3',aws_access_key_id='',aws_secret_access_key='')

try:
    s3.create_bucket(Bucket='datacont-mdludwig', CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
except:
    print("this might already exist")

bucket = s3.Bucket("datacont-mdludwig")
bucket.Acl().put(ACL="public-read")

dyndb = boto3.resource('dynamodb',region_name='us-east-2',aws_access_key_id='',aws_secret_access_key='')
#table = dyndb.Table("MDLudwigDataTable")
try:
    table = dyndb.create_table(
        TableName="MDLudwigDataTable",
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("MDLudwigDataTable")

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open(item[4], 'rb')
        s3.Object('datacont-mdludwig', item[4]).put(Body=body)
        md = s3.Object('datacont-mdludwig', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-east-2.amazonaws.com/datacont-mdludwig/" + item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[3], 'date': item[2], 'url':url}

        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or other failure")

response = table.get_item(Key={'PartitionKey': 'experiment1','RowKey': 'data1'})
item = response['Item']
print(item)