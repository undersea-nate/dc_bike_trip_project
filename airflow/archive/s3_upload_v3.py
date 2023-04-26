import boto3


#client

client = boto3.client('s3')

list_bucket=client.list_buckets()

print(list_bucket)

#service

s3 = boto3.resource('s3')

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

# uploading file

bucket_name = 'shiny-head-aquamarine'

fileupload = s3.Bucket(bucket_name).upload_file('data/NetflixBestOf_reddit.csv', 'netflix_reddit.txt')

print(fileupload)

s3.Object(bucket_name, 'netflix_reddit.txt').delete()