from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
  "clicks",
  bootstrap_servers="localhost:9092",
  auto_offset_reset="earliest",
  value_deserializer=lambda v: json.loads(v.decode("utf-8")),
  group_id="clicks-analytics"
)

click_counts = defaultdict(int)

print("Listening for clicks events...")

for message in consumer:
  event = message.value
  user = event["user"]
  click_counts[user]+= 1
  print(f"User {user} clicked {event['url']} {click_counts[user]} times")
