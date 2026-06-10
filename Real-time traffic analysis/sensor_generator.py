import json
import random
import time
from kafka import KafkaProducer

# إعداد Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# محاكاة بيانات المرور
locations = ['MainStreet', '2ndAve', 'Highway1', 'CentralPark', 'AirportRd']

try:
    while True:
        data = {
            "location": random.choice(locations),
            "cars_count": random.randint(0, 50),
            "average_speed": round(random.uniform(20, 100), 2),
            "timestamp": int(time.time())
        }
        producer.send('traffic_sensors', value=data)
        print(f"Sent: {data}")
        time.sleep(1)  # إرسال رسالة كل ثانية
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    producer.close()
