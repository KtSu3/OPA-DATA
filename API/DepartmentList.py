import mysql.connector
from mysql.connector import Error
import json
import requests
import time
#-------------------------------------------------------------API---------------------------------------------------------#

url = "https://enderecoAplicacao/apiEndpoint"


print(url)
payload = json.dumps({
  "filter": {

},

})
headers = {
  'Authorization': 'Bearer TokenExample',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()

#--------------------------------------------------------CONECT DATABASE---------------------------------------------#

def connect_db3(): 
    try:
        connection = mysql.connector.connect(
            host='',
            user='',
            password='',
            database=''
        )
        if connection.is_connected():
            print('Sucess conection')
            insert_table(connection, data)
            connection.close()
    except Error as e:
        print('Falha ao conectar ao banco:', e)
        return None
    
def insert_table(connection, data):
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM  department;")
        
        for item in data["data"]:  
            department_id = item['_id']
            department = item['nome']
    
            sql = f'''
                INSERT IGNORE INTO department (id_department, department)
                VALUES ('{department_id}', '{department}' );
                    '''
            cursor.execute(sql)
            print(sql)    
        
        connection.commit()
        connection.close()
        print("Success insert data.")
    except Error as e:
            print("Error insert data:", e)
            connection.rollback()

connect_db3()