# EE542_Final_Project
EE542 Final Project Code Repository

## Setup Instructions
### Node-Red Flow on Gateway


### Flash xDot
Flash the xDot with the Mold Genie program by plugging in the device into a computer with Mbed Studio installed, and then import a program into Mbed Studio using the following link:

https://github.com/abustillos7/EE542_Final_Project_xDot.git

Then ensure the active program is ee542_final_project_xdot and the target is your xDot device, then click the run and install button outlined in green as shown below:
![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/Mbed_Flash.png?raw=true)

Wire the xDot, DHT11 and MQ135 as shown below:

![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/xDot%20Setup.jpg?raw=true)

### Save Data to DynamoDB
Create a DynamoDB table with "sample_time" partition key and "device_id" sort key, shown in the following settings:

![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/DynamoDB_Settings.png?raw=true)
Note: Example uses table named "device_data"

Once the table is create, navigate to IoT Core and create a new "Rule" under "Message Routing" with the following settings:

![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/Insert_Rule.png?raw=true)

Note: Remember to change the topic in the SQL command of the rule if the topic is not named "moldy1".

### Enable Lambda Functions
In the "Lambda_Functions" directory in this repository there are two python files that contain the code for two individual AWS lambda functions. Please follow the individual instructions for each file:

#### Lambda Function #1: send_moisture_alarm.py
In AWS create a new Lambda function with the permissions to read and write to the DynamoDB table you have previously created. Once the lambda function is created copy and paste the code provided inside of "Lambda_Functions/send_moisture_alarm.py" into the provided code editor for the newly created function in AWS. Next create a trigger event of type "DynamoDB" and associate the DynamoDB table you have previously created with the trigger.

![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/Alarm_Trigger.png?raw=true)

#### Lambda Function #2: send_report.py
In AWS create a new Lambda function with the permissions to read and write to the DynamoDB table you have previously created. Once the lambda function is created copy and paste the code provided inside of "Lambda_Functions/send_report.py" into the provided code editor for the newly created function in AWS. Next create a trigger event of type "EventBridge(CloudWatch) Events)" and then create a new rule with the desired time for reports to be sent. The example show 30 minutes, the long period the more data is available to fit for mold detection.

![alt text](https://github.com/abustillos7/EE542_Final_Project/blob/main/Pictures/Report_Trigger.png?raw=true)
