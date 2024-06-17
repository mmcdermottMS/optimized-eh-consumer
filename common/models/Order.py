from typing import List, Optional
from pydantic import BaseModel

from common.models.Item import Item

class Order(BaseModel):
    id: str
    items: Optional[List[Item]] = None
