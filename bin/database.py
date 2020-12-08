import mysql.connector
from mysql.connector import Error

def connect_to_database(host, database, user, password):
    try:
        connection = mysql.connector.connect(host=host, database=database, user=user, password=password)
        if connection.is_connected():
            print("Connected to MySQL database.")
            return connection
    except Error as e:
        print ("Error while connecting to MySQL", e) 

        
def close_database_connection(connection):
    # closing database connection.
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")