import json

from app.db.model.casualities import Casualties
from app.db.model.date import Date
from app.db.model.location import Location
from app.repository.news_repository import insert_news_document_if_unique
from app.repository.terror_events_repository import insert_terror_event
from app.service.event_service import convert_to_mongo_compatible
from app.service.validation_service import validate_event

def custom_serializer(obj):
    if isinstance(obj, (Location, Casualties, Date)):
        return obj.__dict__
    elif isinstance(obj, set):
        return list(obj)  # Convert set to list
    raise TypeError(f"Type {type(obj)} not serializable")

def handle_message(message, key):
    key_str = key.decode('utf-8')
    if key_str == "terror_event":
        event = convert_to_mongo_compatible(message)
        event = validate_event(event)

        if event:
            serialized_document = json.dumps(event.__dict__, default=custom_serializer)
            insert_terror_event(serialized_document)
            print("Validated Event: ")
        else:
            print("Event validation failed.")
    elif key_str == "news":
        insert_news_document_if_unique(message)
