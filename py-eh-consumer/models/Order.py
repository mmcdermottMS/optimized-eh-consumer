from typing import List, Optional
from pydantic import BaseModel

from models.Item import Item

class Order(BaseModel):
    id: Optional[int] = -99
    items: Optional[List[Item]] = None
