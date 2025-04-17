import mysql.connector
from mysql.connector import Error
import json
import requests
import time
#-------------------------------------------------------------API---------------------------------------------------------#

url = "https://enderecoAplicacao/apiEndpoint"
payload = json.dumps({
  "filter": {
"status": "AG",
},

})
headers = {
  'Authorization': 'Bearer TokenExample',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()


#-------------------------------------------------------------API---------------------------------------------------------#

url = "https://enderecoAplicacao/apiEndpoint"

payload= json.dumps({
    
  "options": {
    "limit": 1000
  }

})

headers = {
  'Authorization': 'Bearer TokenExample',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)
data2 = response.json()


#-------------------------------------------------------------API---------------------------------------------------------#

#--------------------------------------------------------CONECT DATABASE---------------------------------------------------#
def connect_db4(): 
    try:
        connection = mysql.connector.connect(
            host='',
            user='',
            password='',
            database=''
        )
        if connection.is_connected():
            print('Sucess conection')
            insert_table(connection, data, data2)
            connection.close()
    except Error as e:
        print('Falha ao conectar ao banco:', e)
        return None
#--------------------------------------------------------CONECT DATABASE---------------------------------------------#


#--------------------------------------------------------INSERT TABLE---------------------------------------------------#
def insert_table(connection, data, data2):
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM aguardando_atendimento;")
        
        for item in data["data"]:  
            protocol = item['protocolo'] 
            department_id = item['setor']
            attendant_id = item['id_atendente']
            client = item['id_cliente']
            service_start_date = item['date']
            s = service_start_date.split('T')[0] + ' ' + service_start_date.split('T')[1].split('.')[0]
            sql = f'''
                INSERT INTO aguardando_atendimento (protocol, department, attendant, client, service_start_date)
                VALUES ('{protocol}', '{department_id}', '{attendant_id}', '{client}', DATE_SUB('{s}', INTERVAL 3 HOUR));
                    '''
            
            cursor.execute(sql)
        
        
        connection.commit()
        connection.close()
        print("Success insert data.")
    except Error as e:
            print("Error insert data:", e)
            connection.rollback()

connect_db4()