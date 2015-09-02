#!/usr/bin/env python

import json
import sys
# from os import environ

import boto3

if not len(sys.argv) == 4:
    sys.exit("Usage: python backfill.py region_name aws_sqs_url aws_s3_bucket")

region_name = sys.argv[1]
aws_sqs_url = sys.argv[2]
aws_s3_bucket = sys.argv[3]

bucket = boto3.resource('s3', region_name=region_name).Bucket(aws_s3_bucket)
queue = boto3.resource('sqs', region_name=region_name).Queue(aws_sqs_url)

items_queued = 0
for item in bucket.objects.all():
    if not item.key.endswith('.json.gz'):
        continue
    print item.key

    queue.send_message(
        MessageBody=json.dumps({
            'Message': json.dumps({
                's3Bucket': aws_s3_bucket,
                's3ObjectKey': [item.key]
            })
        })
    )
    items_queued += 1

print('Done! {} items were backfilled'.format(items_queued))
