import json
import boto3
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf

def lambda_handler(event, context):
# Define Tickers for Stock Data
    ticks = "FB SHOP BYND NFLX PINS SQ TTD OKTA SNAP DDOG"
    t_list = ticks.split()
# Download Data from Yahoo Finance
    data = yf.download(ticks, start="2020-05-14", end="2020-05-15", interval = "1m",group_by = 'ticker')
# Connect to Kinesis Firehose
    fh = boto3.client("firehose", "us-east-1")
# Stream Data to S3 bucket
    for i in t_list:
        for index, rows in data[i].iterrows():
            json_str = json.dumps({"high": rows.High, "low": rows.Low, "ts": str(index), 'name': i})
            fh.put_record(
                DeliveryStreamName="yfinance-datastream", 
                Record={"Data": json_str.encode('utf-8')})
    return {
        'statusCode': 200,
        'body': json.dumps('Thank you, data uploaded to S3 bucket')
        
    }
