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
    mqtt=settings.mqtt
    messageString = str(message.payload.decode("utf-8"))
    print("received message =", messageString)
    logger.info('Received message: %s', messageString)
    
    ## DB excute
    try:
        mysqlSettings = settings.database['mysql']
        mydb = connect_to_database(**mysqlSettings)
        mycursor = mydb.cursor()
        data = json.loads(messageString)

        if message.topic == mqtt["topic"]:
            time = datetime.datetime.fromtimestamp(data["created_at"])
            sql = "INSERT INTO data_collections (waspmote_id, sensor_key, value, time_get_sample, algorithm_parameter_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            time_get_sample = data["time_get_sample"] if ('time_get_sample' in data) else 0
            algorithm_parameter_id = data["algorithm_parameter_id"] if ('algorithm_parameter_id' in data) else None
            val = (data["waspmote_id"], data["sensor_key"], data["value"], time_get_sample, algorithm_parameter_id, time, time)
            mycursor.execute(sql, val)
            mydb.commit()
        elif message.topic == mqtt["error_rate_topic"]:
            getSql = "SELECT * FROM error_rates WHERE waspmote_algorithm = %s AND waspmote_not_algorithm = %s AND sensor_key = %s"
            val = (data["waspmote_algorithm"], data["waspmote_not_algorithm"], data["sensor_key"])
            mycursor.execute(getSql, val)
            myresult = mycursor.fetchall()
            if len(myresult) > 0:
                updateSql = "UPDATE error_rates SET value=%s WHERE id=%s"
                val = (data["value"], myresult[0][0])
                mycursor.execute(updateSql, val)
                mydb.commit()
            else:
                now = datetime.datetime.utcnow()
                time = now.strftime('%Y-%m-%d %H:%M:%S')
                addSql = "INSERT INTO error_rates (waspmote_algorithm, waspmote_not_algorithm, sensor_key, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
                val = (data["waspmote_algorithm"], data["waspmote_not_algorithm"], data["sensor_key"], data["value"], time, time)
                mycursor.execute(addSql, val)
                mydb.commit()
    except Exception as e:
        logger.error('Error while add value to MySQL %s', e)

if __name__ == "__main__":
    logger.info('Start app...')
    mqtt=settings.mqtt
    client= paho.Client(mqtt["username"])
    client.username_pw_set(mqtt["username"], password=mqtt["password"])
    #create client object client1.on_publish = on_publish 
    ######Bind function to callback
    client.on_connect = on_connect
    client.on_message=on_message
    #####
    print("connecting to broker ", mqtt["address"])
    client.connect(mqtt["address"], mqtt["port"]) #connect
    print("subscribing ")
    data_topic = mqtt["topic"]
    error_rate_topic = mqtt["error_rate_topic"] if ('error_rate_topic' in mqtt) else "libelium-error-rates"
    client.subscribe([(data_topic,0), (error_rate_topic,0)]) #subscribe
    # client.disconnect() #disconnect
    # client.loop_stop() #stop loop
    client.loop_forever()