import boto3
import re
import datetime
from operator import itemgetter

s3 = boto3.client('s3')
client = boto3.client('s3')
s3_bucket = 'mybucket'
s3_prefix = 'myprefix'


def s3_list_objects(bucket, prefix):
    """Gets a python list of dictionaries of all S3 object properties matching the bucket and prefix."""
    partial_list = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    obj_list = partial_list['Contents']
    while partial_list['IsTruncated']:
        next_token = partial_list['NextContinuationToken']
        partial_list = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix, ContinuationToken=next_token)
        obj_list.extend(partial_list['Contents'])
    return obj_list

def parse_s3_response():
    """Processes the S3 response to a sorted list of object keys only."""
    response = s3_list_objects(s3_bucket, s3_prefix)
    # response is sorted by name by default, the following sort is redundant 
    sorted_list = sorted(response, key=itemgetter('Key'), reverse=False)
    keys = [d['Key'] for d in sorted_list]
    return keys

def find_missing_sequences(ids):
    """Checks for gaps in sequence by parsing a serial number."""
    sequence = 1
    for i in (ids):
        file_seq_number = int((re.findall('\d\d\d\d.zip', i)[0]).split('.')[0])
        if file_seq_number == sequence:
            print('Sequence check pass: expected:','%04d' % sequence,'Next in sequence is:',i)
            sequence += 1
        else:   
            print('Sequence check failed: expected:','%04d' % sequence,'Next file is:',i)
            sequence += 2
        
find_missing_sequences(parse_s3_response())