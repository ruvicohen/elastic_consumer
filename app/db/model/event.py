from pydantic import BaseModel
from typing import List
from app.db.model.casualities import Casualties
from app.db.model.date import Date
from app.db.model.location import Location
from typing import Optional


class Event(BaseModel):
    date: Date
    location: Location
    groups_involved: Optional[List[str]]
    attack_type: Optional[List[str]]
    target_type: Optional[List[str]]
    casualties: Casualties
    summary: Optional[str]