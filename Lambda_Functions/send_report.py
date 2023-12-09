import boto3
import json
from datetime import datetime, timedelta
import os
import scipy.stats
import numpy as np
import math
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

import logging  
logger = logging.getLogger()

import time

def current_milli_time():
    return round(time.time() * 1000)

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


def get_last_processed_timestamp():
    last_processed_timestamp = os.environ.get('LAST_PROCESSED_TIMESTAMP')
    if last_processed_timestamp:
        return int(last_processed_timestamp)
    return 0  # If not found, start from timestamp 0

def update_last_processed_timestamp(timestamp):
    os.environ['LAST_PROCESSED_TIMESTAMP'] = str(timestamp)

def get_new_data_from_dynamodb(table_name, last_processed_timestamp):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print("Query: ", {':last_processed': last_processed_timestamp})
    # Get all records sort by timestamp ascending
    response = table.scan(
        FilterExpression='#st > :last_timestamp',
        ExpressionAttributeNames={'#st': 'sample_time'},
        ExpressionAttributeValues={':last_timestamp': last_processed_timestamp},
    )
    
    # Sort the items based on 'sample_time' column in Python
    sorted_items = sorted(response['Items'], key=lambda x: x['sample_time'])
    
    return sorted_items

    # response = table.scan()
    
    # # Filter items based on 'payload.sample_time' greater than last_processed_timestamp
    # filtered_items = [
    #     item for item in response['Items'] 
    #     if 'payload' in item and 'sample_time' in item['payload'] and item['payload']['sample_time'] > last_processed_timestamp
    # ]
    
    # # Sort the filtered items based on 'payload.sample_time' column
    # sorted_items = sorted(filtered_items, key=lambda x: x['payload']['sample_time'])
    
    return sorted_items
    
    return sorted_items
    
def calculate_moisture_content(temp, humidity, grain_type):
    if(grain_type == 'wheat'):
        return wheat_moisture_table[temp][humidity]
    else:
        return 0

def generate_report(data):
    # Calculate statistics from the data
    # For example:
    total_entries = len(data)
    logger.info("Processing records "+ str(total_entries))
    
    sum_temp = 0
    sum_humidity = 0
    sum_ppm = 0
    sum_ppmc = 0
    all_ts = []
    all_ppms = []
    all_ppmcs = []
    for record in data:
        payload = record['payload']
        sum_temp = sum_temp + payload['temp']
        sum_humidity = sum_humidity + payload['humidity']
        sum_ppm = sum_ppm + payload['ppm']
        sum_ppmc = sum_ppmc + payload['ppmc']
        all_ts.append(float(payload['sample_time']))
        all_ppms.append(float(payload['ppm']))
        all_ppmcs.append(float(payload['ppmc']))

    avg_temp = sum_temp / total_entries
    avg_humidity = sum_humidity / total_entries
    avg_ppm = sum_ppm / total_entries
    avg_ppmc = sum_ppmc / total_entries

    # Use scipy.stats.linregress() here to print the slope and if the ppm is increasing or decreasing
    # TODO add ppm gradual increase stat and alarm
    print("X SHAPE: ", np.array(all_ts).shape)
    print("Y SHAPE: ", np.array(all_ppmcs).shape)
    # slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(all_ts, np.array(all_ppmcs))
    slope, intercept = np.polyfit(all_ts, all_ppmcs, 1)
    print("PPM Slope "+ str(slope))
    print("PPM Intercept "+ str(intercept))
    # print("PPM Std Err "+ str(std_err))
    report = f"Granary Environmental Report:\n\n"
    
    
    
    
        
    lookup_temp_ceil_5 = (math.ceil((avg_temp % 5) / 5) + int(avg_temp / 5)) * 5
    if(lookup_temp_ceil_5 < 25):
        lookup_temp_ceil_5 = 25
    elif(lookup_temp_ceil_5 > 90):
        lookup_temp_ceil_5 = 90
    
    lookup_humidity_ceil_5 = (math.ceil((avg_humidity % 5) / 5) + int(avg_humidity / 5)) * 5
    if(lookup_humidity_ceil_5 < 35):
        lookup_humidity_ceil_5 = 35
    elif(lookup_humidity_ceil_5 > 100):
        lookup_humidity_ceil_5 = 100
    
    # get latest record with max timestamp and calculate  its moisture content
    last_record = data[len(data) - 1]
    first_record = data[0]
    print("LAST RECORD", last_record)
    print("FIRST RECORD", first_record)
    last_temp = last_record['payload']['temp']
    last_humidity = last_record['payload']['humidity']
    
    lookup_last_temp = (math.ceil((last_temp % 5) / 5) + int(last_temp / 5)) * 5
    if(lookup_last_temp < 25):
        lookup_last_temp = 25
    elif(lookup_last_temp > 90):
        lookup_last_temp = 90
    
    lookup_last_humidity = (math.ceil((last_humidity % 5) / 5) + int(last_humidity / 5)) * 5
    if(lookup_last_humidity < 35):
        lookup_last_humidity = 35
    elif(lookup_last_humidity > 100):
        lookup_last_humidity = 100

    last_moisture_content = calculate_moisture_content(lookup_last_temp, lookup_last_humidity, 'wheat')

    
    moisture_content = calculate_moisture_content(lookup_temp_ceil_5, lookup_humidity_ceil_5, 'wheat')

    # TODO add ppm gradual increase stat and alarm
    report += "GENIE RECOMMENDATION: "
    
    recommendation = "NO MOLD. ALL OKAY! \n\n"
    if(slope > 0):
        if(last_moisture_content> 12):
            recommendation = "COULD BE MOLD. DAY 1 OF CONSEQUETIVE ALARM! \n\n"
        report += recommendation
        report += f"PPM is increasing with slope {slope} and at intercept {intercept}\n"
        print("PPM is increasing.")
    else:
        report += recommendation
        report += f"PPM is decreasing with slope {slope} and at intercept {intercept}\n"
        print("PPM is decreasing.")
    
    
    report += f"Total Entries: {total_entries}\n"
    report += f"Average Temperature: {avg_temp:.2f} °F\n"
    report += f"Average Humidity: {avg_humidity:.2f}%\n"
    report += f"Average Moisture Content: {moisture_content}\n"
    report += f"Latest Moisture Content: {last_moisture_content}\n"
    logger.info("Average MC "+ str(moisture_content))
    return report

def send_email(subject, body, sender, recipient):
    ses = boto3.client('ses')
    logger.info("Sending email")
    try:
        response = ses.send_email(
            Destination={
                'ToAddresses': [recipient],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject,
                },
            },
            Source=sender,
        )
    except ClientError as e:
        logger.exception(f"Error sending email: {e}")
    else:
        logger.info("Email sent successfully")

def format_timestamp(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000) 
    return dt.strftime('%Y %b %d - %H:%M:%S')

def lambda_handler(event, context):
    print("REPORT LAMBDA STARTED")
    table_name = 'device_data'
    email_recipient = 'me@tmithun.com'
    email_sender = 'me@tmithun.com'

    last_processed_timestamp = get_last_processed_timestamp()
    # last_processed_timestamp = current_milli_time()
    print("START TIME: " + str(last_processed_timestamp))
    response = get_new_data_from_dynamodb(table_name, last_processed_timestamp)
    new_data = response
    print("ND: ",len(new_data))
    if new_data:
        print("ND[0]: ",new_data[0])
        report = generate_report(new_data)
        # report = ""
        print("REPORT: ", report)

        latest_timestamp = new_data[len(new_data) - 1]['payload']['sample_time']
        print("last timestamp: ", latest_timestamp)
        update_last_processed_timestamp(latest_timestamp)

        last_processed_formatted = format_timestamp(last_processed_timestamp)
        current_timestamp_formatted = format_timestamp(latest_timestamp)

        email_subject = (
            f"Granary Environmental Report. From: {last_processed_formatted} To: {current_timestamp_formatted}"
        )

        print("REPORT: ", email_recipient)
        send_email(email_subject, report, email_sender, email_recipient)
    else:
        #update_last_processed_timestamp(latest_timestamp)
        print("NO NEW DATA")
    return {
        'statusCode': 200,
        'body': json.dumps('Report generated and sent successfully')
    }

