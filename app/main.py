import os
from app.kafka_settings.consumer import consume_topics
from dotenv import load_dotenv
from app.repository.news_repository import setup_index_for_json
from app.repository.terror_events_repository import setup_index
from app.service.handle_masseges import handle_message

load_dotenv(verbose=True)
elastic_topic = os.environ["ELASTIC_TOPIC"]
db_topic = os.environ["MONGO_TOPIC"]

topics = [os.environ["ELASTIC_TOPIC"], os.environ["MONGO_TOPIC"]]

if __name__ == "__main__":
    setup_index()
    setup_index_for_json()

    consume_topics(topics, handle_message)
