from pydantic import ValidationError
from app.db.model.event import Event

def validate_event(value):
    try:
        event = Event(**value)
        return event
    except ValidationError as e:
        print(f"Validation failed: {e}")
        return None