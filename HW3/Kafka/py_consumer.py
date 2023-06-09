from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
import json

consumer = KafkaConsumer('testTopic', bootstrap_servers=['localhost:9092'])
hdfs_base_path = 'hdfs://localhost:9000/giuseppericcio/bigdata/'

with open('capoluoghi.json') as file:
    capoluoghi = json.load(file)

nomi_citta = []
for capoluogo in capoluoghi['capoluoghi']:
    nomi_citta.append(capoluogo['nome'])

count = 0
for message in consumer:
    values = message.value.decode('utf-8')
    if values == '"EOF"':
        break

    hdfs_path = f"{hdfs_base_path}{nomi_citta[count]}.json"

    with hdfs.open(hdfs_path, 'wt') as f:
        f.write(values)
        count += 1
    
    f.fs.close()