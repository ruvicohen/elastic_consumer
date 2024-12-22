from pydantic import BaseModel
from typing import Optional

class Casualties(BaseModel):
    fatalities: Optional[int]
    injuries: Optional[int]
    score: Optional[int]