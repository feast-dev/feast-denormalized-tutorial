import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaException
from denormalized import Context, FeastDataStream
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit

from session_generator.login_attempt import LoginAttempt

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fake_sessions"

login_attempt_schema =  LoginAttempt(
    timestamp=datetime.now(),
    user_id="user_21",
    ip_address="127.0.01",
    success=True,
).to_dict()

ds = FeastDataStream(
    Context().from_topic(
        TOPIC,
        json.dumps(login_attempt_schema),
        BOOTSTRAP_SERVERS,
    )
)

# ds.window(
#     [],
#     [
#         f.count(col("@todo"), distinct=False, filter=None).alias("count"),
#     ],
#     1000,
#     None,
# )
