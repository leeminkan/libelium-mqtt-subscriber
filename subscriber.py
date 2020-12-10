import time
import datetime
import paho.mqtt.client as paho
import settings
from bin.database import connect_to_database
import logging
import json

logging.basicConfig(filename='app.log', filemode='w', level=logging.INFO, format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s')
logger = logging.getLogger()  # get the root logger

#define callback
def on_connect(mqttc, obj, flags, rc):
    print("rc: "+ str(rc))
    logger.info('On connect')
    logger.info("rc: %s", str(rc))

def on_message(client, userdata, message):
    messageString = str(message.payload.decode("utf-8"))
    print("received message =", messageString)
    logger.info('Received message: %s', messageString)
    
    ## DB excute
    try:
        mysqlSettings = settings.database['mysql']
        mydb = connect_to_database(**mysqlSettings)
        mycursor = mydb.cursor()

        data = json.loads(messageString)
        time = datetime.datetime.fromtimestamp(data["created_at"])
        sql = "INSERT INTO data_collections (waspmote_id, sensor_key, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"
        val = (data["waspmote_id"], data["sensor_key"], data["value"], time, time)
        mycursor.execute(sql, val)
        mydb.commit()
    except Exception as e:
        logger.error('Error while add value to MySQL %s', e)

if __name__ == "__main__":
    logger.info('Start app...')
    mqtt=settings.mqtt
    client= paho.Client("subscriber") 
    client.username_pw_set(mqtt["username"], password=mqtt["password"])
    #create client object client1.on_publish = on_publish 
    ######Bind function to callback
    client.on_connect = on_connect
    client.on_message=on_message
    #####
    print("connecting to broker ", mqtt["address"])
    client.connect(mqtt["address"], mqtt["port"]) #connect
    print("subscribing ")
    client.subscribe(mqtt["topic"]) #subscribe
    # client.disconnect() #disconnect
    # client.loop_stop() #stop loop
    client.loop_forever()