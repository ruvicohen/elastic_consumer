from app.db.model.casualities import Casualties
from app.db.model.date import Date
from app.db.model.location import Location
import numpy as np


def is_nan(value):
    return value is None or (isinstance(value, float) and np.isnan(value))


def convert_to_mongo_compatible(data):
    if isinstance(data, dict):
        return {key: convert_to_mongo_compatible(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_to_mongo_compatible(item) for item in data]
    elif isinstance(data, (Casualties, Location, Date)):
        return convert_to_mongo_compatible(data.dict())
    elif is_nan(data):
        return None
    return data
