import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(filename='app.log', filemode='w', level=logging.INFO, format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s')
logger = logging.getLogger()  # get the root logger

def connect_to_database(host, database, user, password):
    try:
        connection = mysql.connector.connect(host=host, database=database, user=user, password=password)
        if connection.is_connected():
            print("Connected to MySQL database.")
            return connection
    except Error as e:
        logger.error('Error while connecting to MySQL %s', e)
        print ("Error while connecting to MySQL", e) 

        
def close_database_connection(connection):
    # closing database connection.
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")