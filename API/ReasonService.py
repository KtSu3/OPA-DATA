import mysql.connector
from mysql.connector import Error
from datetime import date
import json
import requests

#-------------------------------------------------------------API----------------------------------------------------------------------#

current_date = date.today()
current_date_str = current_date.isoformat()

url = "https://enderecoAplicacao/apiEndpoint"
payload = json.dumps({
  "filter": {
"status": "F",
"dataInicialAbertura": current_date_str,
"dataFinalAbertura":  current_date_str,
"dataInicialEncerramento": current_date_str,
"dataFinalEncerramento":  current_date_str
},

})
headers = {
  'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY2MDE4MWI0YTQ4MDMxN2RlMzBmMzBiOCIsImlhdCI6MTcyMDQ2MTU1MX0.C17b8No_sPRXyhmjk4kTCiMQj9eAvQJ10CwooC1HzOk',
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
  'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY2MDE4MWI0YTQ4MDMxN2RlMzBmMzBiOCIsImlhdCI6MTcyMDQ2MTU1MX0.C17b8No_sPRXyhmjk4kTCiMQj9eAvQJ10CwooC1HzOk',
  'Content-Type': 'application/json'
}

data2 = response.json()



#--------------------------------------------------------CONECT DATABASE---------------------------------------------------#
def connect_db2():
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
def insert_table(connection, data, data2):
    try:
        cursor = connection.cursor()

        for item in data["data"]:
            protocol = item['protocolo']
            department_id = item['setor']
            attendant_id = item['id_atendente']
            client = item['id_cliente']
            dateStart = item['date']
            dateSlipt = dateStart.split('T')[0] + ' ' + dateStart.split('T')[1].split('.')[0]
            dateEnd = item['fim']
            dateESplit = dateEnd.split('T')[0] + ' ' + dateEnd.split('T')[1].split('.')[0]
            service_start_date = item.get('date', 'Unknown')
            s = service_start_date.split('T')[0] + ' ' + service_start_date.split('T')[1].split('.')[0] if service_start_date else 'Unknown'
            reason = item.get('motivos', [])
            reason_item = reason[0].get('idMotivo', None) if reason else None

            sql = f'''
                INSERT IGNORE INTO motivo_atendimento (protocol, department, attendant, client, reason,  service_start_date, dateStart, dateEnd)
                VALUES ('{protocol}', '{department_id}', '{attendant_id}', '{client}', '{reason_item}', '{s}', '{dateSlipt}', '{dateESplit}');
                    '''
            cursor.execute(sql)


        connection.commit()
        connection.close()
        print("Success insert data.")
    except Error as e:
            print("Error insert data:", e)
            connection.rollback()
connect_db2()