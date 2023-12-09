import requests
import boto3
import json
import datetime
import dateutil.tz
from botocore.exceptions import ClientError
import math

import logging  
logger = logging.getLogger()

# Define your alarms
wheat_alarms = {
    "mc": {
        "high_limit": 12
    },
}

AWS_REGION = 'us-west-2'
SENDER_EMAIL = 'me@tmithun.com'
RECIPIENT_EMAIL = 'me@tmithun.com'
OUTPUT_TIMEZONE = 'US/Pacific'

wheat_moisture_table = {
    35: {25: 8.3, 30: 8.9, 35: 9.6, 40: 10.2, 45: 10.9, 50: 11.5, 55: 12.2, 60: 13, 65: 13.8, 70: 14.7, 75: 15.8, 80: 17.1, 85: 18.8, 90: 21.3},
    40: {25: 8.2, 30: 8.8, 35: 9.5, 40: 10.1, 45: 10.7, 50: 11.4, 55: 12.1, 60: 12.8, 65: 13.7, 70: 14.6, 75: 15.7, 80: 17, 85: 18.7, 90: 21.1},
    45: {25: 8.1, 30: 8.8, 35: 9.4, 40: 10, 45: 10.6, 50: 11.3, 55: 12, 60: 12.7, 65: 13.5, 70: 14.4, 75: 15.5, 80: 16.8, 85: 18.5, 90: 20.9},
    50: {25: 8, 30: 8.7, 35: 9.3, 40: 9.9, 45: 10.5, 50: 11.2, 55: 11.9, 60: 12.6, 65: 13.4, 70: 14.3, 75: 15.4, 80: 16.6, 85: 18.3, 90: 20.7},
    55: {25: 7.9, 30: 8.6, 35: 9.2, 40: 9.8, 45: 10.4, 50: 11.1, 55: 11.7, 60: 12.5, 65: 13.3, 70: 14.2, 75: 15.2, 80: 16.5, 85: 18.1, 90: 20.5},
    60: {25: 7.8, 30: 8.5, 35: 9.1, 40: 9.7, 45: 10.3, 50: 10.9, 55: 11.6, 60: 12.3, 65: 13.1, 70: 14, 75: 15.1, 80: 16.3, 85: 18, 90: 20.4},
    65: {25: 7.8, 30: 8.4, 35: 9, 40: 9.6, 45: 10.2, 50: 10.8, 55: 11.5, 60: 12.2, 65: 13, 70: 13.9, 75: 14.9, 80: 16.2, 85: 17.8, 90: 20.2},
    70: {25: 7.7, 30: 8.3, 35: 8.9, 40: 9.5, 45: 10.1, 50: 10.7, 55: 11.4, 60: 12.1, 65: 12.8, 70: 13.7, 75: 14.7, 80: 16, 85: 17.6, 90: 20},
    75: {25: 7.6, 30: 8.2, 35: 8.8, 40: 9.4, 45: 10, 50: 10.6, 55: 11.2, 60: 11.9, 65: 12.7, 70: 13.6, 75: 14.6, 80: 15.8, 85: 17.4, 90: 19.8},
    80: {25: 7.5, 30: 8.1, 35: 8.7, 40: 9.3, 45: 9.9, 50: 10.5, 55: 11.1, 60: 11.8, 65: 12.6, 70: 13.4, 75: 14.4, 80: 15.7, 85: 17.3, 90: 19.6},
    85: {25: 7.4, 30: 8, 35: 8.6, 40: 9.2, 45: 9.8, 50: 10.4, 55: 11, 60: 11.7, 65: 12.4, 70: 13.3, 75: 14.3, 80: 15.5, 85: 17.1, 90: 19.4},
    90: {25: 7.3, 30: 7.9, 35: 8.5, 40: 9.1, 45: 9.6, 50: 10.2, 55: 10.9, 60: 11.5, 65: 12.3, 70: 13.1, 75: 14.1, 80: 15.3, 85: 16.9, 90: 19.2},
    95: {25: 7.2, 30: 7.8, 35: 8.4, 40: 9, 45: 9.5, 50: 10.1, 55: 10.7, 60: 11.4, 65: 12.2, 70: 13, 75: 14, 80: 15.2, 85: 16.7, 90: 19},
    100: {25: 7.2, 30: 7.7, 35: 8.3, 40: 8.8, 45: 9.4, 50: 10, 55: 10.6, 60: 11.3, 65: 12, 70: 12.8, 75: 13.8, 80: 15, 85: 16.5, 90: 18.8}
    }

def send_email(subject, body):
    CHARSET = 'UTF-8'

    client = boto3.client('ses', region_name=AWS_REGION)

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT_EMAIL,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=SENDER_EMAIL,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:", response['MessageId'])

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    try:
        logger.info("STARTING TO PARSE RECORDS. " + str(len(event['Records'])))
        for record in event['Records']:
            
            if record['eventName'] == 'INSERT':
                logger.info("Parsing timestamp", extra = record)
                new_image = record['dynamodb']['NewImage']['payload']['M']
                sample_time = record['dynamodb']['NewImage']['sample_time']['N']
                logger.info("TIME " + str(sample_time) )
                print("TIME " + str(sample_time))
                local_time = datetime.datetime.utcfromtimestamp(int(sample_time)/1000).replace(tzinfo=dateutil.tz.tzutc()).astimezone(dateutil.tz.gettz(OUTPUT_TIMEZONE))
                time_str = local_time.strftime('%d/%b/%y - %H:%M:%S %Z')
                # time_str = datetime.datetime.fromtimestamp((int(sample_time)/1000), tz = )
                lookup_temp = float(new_image['temp']['N'])
                lookup_humidity = float(new_image['humidity']['N'])
                lookup_temp_ceil_5 = (math.ceil((lookup_temp % 5) / 5) + int(lookup_temp / 5)) * 5
                if(lookup_temp_ceil_5 < 25):
                    lookup_temp_ceil_5 = 25
                elif(lookup_temp_ceil_5 > 90):
                    lookup_temp_ceil_5 = 90
                
                lookup_humidity_ceil_5 = (math.ceil((lookup_humidity % 5) / 5) + int(lookup_humidity / 5)) * 5
                if(lookup_humidity_ceil_5 < 35):
                    lookup_humidity_ceil_5 = 35
                elif(lookup_humidity_ceil_5 > 100):
                    lookup_humidity_ceil_5 = 100
                
                
                print(f"Temp: {lookup_temp_ceil_5}, Humidity: {lookup_humidity_ceil_5}")
                mc = wheat_moisture_table[lookup_temp_ceil_5][lookup_humidity_ceil_5]
                new_image['mc'] = {'N': mc}
                print(f"mc: {mc}")
                
                try:
                    # send to thingsboard server here http://44.233.77.231:8080/api/v1/manlwqufjace6baspnw4/telemetry
                    record_ts = int(sample_time)
                    data = {"mc": mc, 'ts': record_ts}
                    print(data)
                    r = requests.post("http://44.233.77.231:8080/api/v1/manlwqufjace6baspnw4/telemetry", json = data)
                    print("TB RESPONSE CODE " + str(r.status_code))
                except Exception as e:
                    print("ERROR SENDING TO TB")
                    print(e)

                body = ""
                subject = ""
                flag = False
                for attribute, limits in wheat_alarms.items():
                    if attribute in new_image:
                        value = float(new_image[attribute]['N'])
                        if 'high_limit' in limits and value > limits['high_limit']:
                            subject = f"Alarms with your Granary"
                            body += f"The estimated {attribute} value ({value}) has exceeded the high limit of {limits['high_limit']}.\n"
                            body += f"Event occured at {time_str} with readings:\n"
                            body += f"Tempereture: {lookup_temp} degrees F rounded to {lookup_temp_ceil_5} degrees F\n"
                            body += f"Humidity: {lookup_humidity}% rounded to {lookup_humidity_ceil_5}%\n"
                            flag = True
                            # send_email(subject, body)
                        
                        if 'low_limit' in limits and value < limits['low_limit']:
                            subject = f"Alarms with your Granary"
                            body += f"The estimated {attribute} value ({value}) has fallen below the low limit of {limits['low_limit']}.\n"
                            body += f"Event occured at {time} with readings:\n"
                            body += f"Tempereture: {lookup_temp} degrees F rounded to {lookup_temp_ceil_5} degrees F\n"
                            body += f"Humidity: {lookup_humidity}% rounded to {lookup_humidity_ceil_5}%\n"
                            flag = True
                
            else:
                logger.info("Record type not insert, not supported")
        if flag:
            logger.info("Alarm found sending email")
            send_email(subject, body)

    except Exception as e:
        logger.exception("ERROR occured")
        
    