from kafka import KafkaConsumer
from json import loads
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

mongo_url = 'mongodb+srv://digitalskola:digitalskola@cluster0.nadbkym.mongodb.net/admin?authSource=admin&replicaSet=atlas-q2fmmq-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true'

mongo_client = MongoClient(mongo_url, server_api=ServerApi('1'))

db = mongo_client['digitalskola']
collection = db['finnhub_trades']

try:
    mongo_client.admin.command('ping')
    print('connected')
except Exception as e:
    print(e)


consumer = KafkaConsumer(
    'finnhub1',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='consumer-id-1',
    value_deserializer=lambda x: loads(x.decode("utf-8"))
)

while True :
    for message in consumer :
        message_raw = message.value
        json_message = loads(message_raw)
        if json_message['type'] == 'trade' :
            print(json_message['data'])
            for result in json_message['data'] :
                print(result)
