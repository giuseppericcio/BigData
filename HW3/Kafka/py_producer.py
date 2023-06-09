import json
import requests
from kafka import KafkaProducer
from json import dumps

KAFKA_TOPIC = 'testTopic'

with open('capoluoghi.json') as file:
    capoluoghi = json.load(file)

with requests.Session() as s:
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

    for capoluogo in capoluoghi['capoluoghi']:
        print(capoluogo)
        nome = capoluogo['nome']
        latitudine = capoluogo['latitudine']
        longitudine = capoluogo['longitudine']
        
        API_URL = f"https://api.open-meteo.com/v1/gfs?latitude={latitudine}&longitude={longitudine}&hourly=temperature_2m,relativehumidity_2m,apparent_temperature,pressure_msl,precipitation,precipitation_probability,visibility,windspeed_10m&current_weather=true&past_days=3&forecast_days=3&timezone=auto"
        
        response = s.get(API_URL)
        data = response.json()
        
        producer.send(KAFKA_TOPIC, value=data)
    
    producer.send(KAFKA_TOPIC, 'EOF')