from datetime import datetime
import time
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

myMQTTClient = AWSIoTMQTTClient("test_client")
myMQTTClient.configureEndpoint("a72qba275aic3-ats.iot.eu-central-1.amazonaws.com", 8883)

myMQTTClient.configureCredentials(
    CAFilePath="/home/wilson/aws_iot/AmazonRootCA1.pem",
    KeyPath="/home/wilson/aws_iot/iot-private.pem.key",
    CertificatePath="/home/wilson/aws_iot/iot.pem.crt",
)
 
print('Initiating Realtime Data Transfer From Raspberry Pi...')

Myvar= myMQTTClient.connect()

date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
print(f"Timestamp:{date}")

while True:
    message = "Test message"
    myMQTTClient.publish("topic/pi", "{\"MotionMessage\":\""+ message + "\", \"Timestamp\" :\""+ str(date)+ "\"}", 0)
    time.sleep(1)
