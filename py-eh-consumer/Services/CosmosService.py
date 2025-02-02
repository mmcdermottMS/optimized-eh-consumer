import logging
import os
from typing import List

from azure.cosmos.exceptions import CosmosHttpResponseError
from azure.cosmos import CosmosClient

from models.Item import Item

ENDPOINT = os.environ["COSMOS_ENDPOINT"]
KEY = os.environ["COSMOS_KEY"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
       
def upsertItemsInSeries(items: List[Item], order_id: str, container_name: str):
    with CosmosClient(url=ENDPOINT, credential=KEY) as client:
    
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(container_name)
        
        try: 
            for item in items:
                container.upsert_item(item.model_dump())
            
        except CosmosHttpResponseError as e:
            logging.error(f"Error operation: {e.status_code}, error operation response: {e.message}")