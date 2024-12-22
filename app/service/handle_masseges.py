from app.repository.news_repository import insert_news_document_if_unique
from app.repository.terror_events_repository import insert_terror_event
from app.service.event_service import convert_to_mongo_compatible
from app.service.validation_service import validate_event


def handle_message(message, key):
    key_str = key.decode('utf-8')
    if key_str == "terror_event":
        event = convert_to_mongo_compatible(message)
        event = validate_event(event)

        if event:
            insert_terror_event(event)
            print("Validated Event: ")
        else:
            print("Event validation failed.")
    elif key_str == "news":
        insert_news_document_if_unique(message)
