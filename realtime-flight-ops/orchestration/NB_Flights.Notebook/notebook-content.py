# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

pip install requests pytz azure-servicebus

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.servicebus import ServiceBusClient, ServiceBusMessage
import requests, json, time
from datetime import datetime
import pytz

# Config
ES_CONN = ("Endpoint=sb://…;SharedAccessKeyName=…;SharedAccessKey=…;EntityPath=es_flights")
API_URL = "https://api.aviationstack.com/v1/flights?access_key=YOUR_KEY&limit=100"
POLL = 3600 
TZ   = pytz.timezone("UTC")

def fetch_flights():
    r = requests.get(API_URL); r.raise_for_status()
    return r.json().get("data",[])

def send_to_es(records):
    entity = next(p.split("=",1)[1] for p in ES_CONN.split(";") if p.startswith("EntityPath="))
    now = datetime.now(TZ).isoformat()
    for rec in records:
        rec["IngestedAt"] = now
    client = ServiceBusClient.from_connection_string(ES_CONN)
    with client.get_queue_sender(entity) as sender:
        sender.send_messages([ServiceBusMessage(json.dumps(rec)) for rec in records])
    client.close()

while True:
    data = fetch_flights()
    if data: send_to_es(data)
    time.sleep(POLL)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
