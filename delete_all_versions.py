#!/usr/bin/env python3
import boto3
import json
import sys
import logging
from botocore.exceptions import ClientError

access_key=''
secret_key=''
endpoint=''
region=''

# This should be 1000 or less. The 1000 is an AWS thingie.
maxkeys=500

# List the stuff that is deleted or not
Quiet=True


# Choose one from DEBUG, INFO, WARNING, ERROR, CRITICAL
level=logging.INFO

logging.basicConfig(format='%(message)s', level=level)

def usage():
    logging.info("Usage: delete_all_versions.py <bucket name>")
    
    
# Get name bucket 
if len(sys.argv)==2:
    s3bucket=str(sys.argv[1])
else:
    logging.critical("Invalid input")
    usage()
    sys.exit(1)

# Open the connection.
try:
    client = boto3.client('s3',
                            region,
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)
except:
    logging.critical("Cannot connect")
    sys.exit(1)


# Check if the bucket has versioning enabled. If not, then exit.
try:
    status=client.get_bucket_versioning(Bucket=s3bucket)['Status']
except KeyError:
    logging.critical(s3bucket+" does not have versioning enabled")
    sys.exit(1)
except ClientError as error:
    if error.response['Error']['Code']=='NoSuchBucket':
        logging.critical(s3bucket+" does not exist")
        sys.exit(1)
    else:
        logging.critical("Cannot get versioning information on "+s3bucket)
        raise
    

if status != 'Enabled':
    logging.critical("The versioning status of "+s3bucket+" should be \"Enabled\" but it is "+status)
    sys.exit(1)
# We're good


is_truncated=True

while is_truncated:

# List object versions
    response=client.list_object_versions(Bucket=s3bucket,MaxKeys=maxkeys)

    logging.debug(json.dumps(response, indent=4,sort_keys=True, default=str))

    is_truncated=response['IsTruncated']

    Objects=[]

# Get the objects to delete
    if 'Versions' in response:
        for i in response['Versions']:
            Objects.append({'Key':i['Key'],'VersionId':i['VersionId']})

# Get the delete markers to delete
    if 'DeleteMarkers' in response:
        for i in response['DeleteMarkers']:
            Objects.append({'Key':i['Key'],'VersionId':i['VersionId']})

    if len(Objects)==0:
        logging.warning("Nothing to delete")
        sys.exit(0)

    Delete={'Objects':Objects, 'Quiet': Quiet}

    response=client.delete_objects(Bucket=s3bucket,
                                   Delete=Delete)

    logging.debug(json.dumps(response, indent=4,sort_keys=True, default=str))

    logging.info("Successfully deleted "+str(len(Objects))+" objects")
