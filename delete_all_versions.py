#!/usr/bin/env python3
import boto3
import json
import sys
from botocore.exceptions import ClientError

access_key=''
secret_key=''
endpoint=''
region=''

def usage():
    sys.stderr.write("Usage: delete_all_versions.py <bucket name>\n")
    
# Get name bucket 
if len(sys.argv)==2:
    s3bucket=str(sys.argv[1])
else:
    sys.stderr.write("Invalid input\n")
    usage()
    sys.exit(1)

# This should be 1000 or less. The 1000 is an AWS thingie.
maxkeys=500

# List the stuff that is deleted or not
Quiet=True

debug=False
verbose=True

# Open the connection.
try:
    client = boto3.client('s3',
                            region,
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)
except:
    sys.stderr.write("Cannot connect\n")
    sys.exit(1)


# Check if the bucket has versioning enabled. If not, then exit.
try:
    status=client.get_bucket_versioning(Bucket=s3bucket)['Status']
except KeyError:
    sys.stderr.write(s3bucket+" does not have versioning enabled\n")
    sys.exit(1)
except ClientError as error:
    if error.response['Error']['Code']=='NoSuchBucket':
        sys.stderr.write(s3bucket+" does not exist\n")
        sys.exit(1)
    else:
        sys.stderr.write("Cannot get versioning information on "+s3bucket+"\n")
        raise
    

if status != 'Enabled':
    sys.stderr.write("The versioning status of "+s3bucket+" is "+status)
    sys.exit(1)
# We're good


is_truncated=True

while is_truncated:

# List object versions
    response=client.list_object_versions(Bucket=s3bucket,MaxKeys=maxkeys)

    if debug:
        print (json.dumps(response, indent=4,sort_keys=True, default=str))
        print ('\n\n')

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
        if verbose:
            print ("Nothing to delete.")
        sys.exit(0)

    Delete={'Objects':Objects, 'Quiet': Quiet}

    response=client.delete_objects(Bucket=s3bucket,
                                   Delete=Delete)

    if debug:
        print (json.dumps(response, indent=4,sort_keys=True, default=str))
        print ('\n\n')

    if verbose:
        print ("Sucessfully deleted "+str(len(Objects))+" objects.")
