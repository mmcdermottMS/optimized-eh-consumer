import logging
import os
import time
from typing import List
from retry import retry

from azure.cosmos.exceptions import CosmosBatchOperationError, CosmosHttpResponseError
from azure.cosmos.aio import CosmosClient

from models.Item import Item

ENDPOINT = os.environ["COSMOS_ENDPOINT"]
KEY = os.environ["COSMOS_KEY"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
MAX_BATCH_SIZE_IN_CHAR = (int(os.environ["MAX_BATCH_SIZE_IN_MB"]) * 1024 * 1024) / 4 #Assuming UTF-8 encoding, and a possible size of 4 bytes per character
MAX_BATCH_ITEM_COUNT = int(os.environ["MAX_BATCH_ITEM_COUNT"])

#Addtional reading on transactional batch operations and the Cosmos async client
#https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/cosmos/azure-cosmos#using-transactional-batch
#https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=python
#https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/cosmos/azure-cosmos#using-the-asynchronous-client
async def upsertItemsInBatch(items: List[Item], order_id: str, container_name: str):
    async with CosmosClient(url=ENDPOINT, credential=KEY) as client:
    
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(container_name)

        batchOperations = []
        batchOperationCount = 0
        batchCharacterCount = 0
        for item in items:
            itemJson = item.model_dump()
            batchOperationCount += 1
            batchCharacterCount += len(itemJson)
            
            #If the current item would put the batch operation count or batch size over the limit, do not
            #add the item to the batch, instead send the current batch and start a new one.
            #Read more about transactional batch limits here: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=python#limitations
            if(batchOperationCount > MAX_BATCH_ITEM_COUNT or batchCharacterCount > MAX_BATCH_SIZE_IN_CHAR):
                await sendBatch(batchOperations, order_id, container)
                
                #reset batch
                batchOperationCount = 1
                batchCharacterCount = len(itemJson)
                batchOperations = []
                batchOperations.append(("upsert", (item.model_dump(),), {})) #The operations do not all have to be the same type, but they do need to have the same partition key
            else:
                batchOperations.append(("upsert", (item.model_dump(),), {}))
                
        await sendBatch(batchOperations, order_id, container)
      
      
#If any single item in the batch fails, the entire batch will fail.  The retry decorator will retry the batch operation to account
#for transient issues such as 412 Precondtion failure or 429 Too many requests error. 
@retry(CosmosBatchOperationError, tries=3, delay=1, backoff=2, logger=logging.getLogger(__name__))           
async def sendBatch(batch_operations, order_id, container):
        try:
            start = time.perf_counter()
            
            #Batch results can be used to interrogate the results of each individual operation
            request_charge = 0
            batch_results = await container.execute_item_batch(batch_operations, order_id)
            for result in batch_results:
                request_charge += result['requestCharge']

            logging.info(f"Upserted {len(batch_operations)} items in: {time.perf_counter() - start:0.4f} seconds for {request_charge} RUs. - Async in Batch")
            
        except CosmosBatchOperationError as e:
            error_operation_index = e.error_index
            error_operation_response = e.operation_responses[error_operation_index]
            error_operation = batch_operations[error_operation_index]
            logging.error(f"Error operation: {error_operation}, error operation response: {error_operation_response}")
            raise e

        
async def upsertItemsInSeries(items: List[Item], order_id: str, container_name: str):
    async with CosmosClient(url=ENDPOINT, credential=KEY) as client:
    
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(container_name)
        
        try: 
            for item in items:
                await container.upsert_item(item.model_dump())
            
        except CosmosHttpResponseError as e:
            logging.error(f"Error operation: {e.status_code}, error operation response: {e.message}")