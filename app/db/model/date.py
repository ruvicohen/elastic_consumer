from typing import Optional

from pydantic import BaseModel

class Date(BaseModel):
    year: Optional[int]
    month: Optional[int]
    day: Optional[int]