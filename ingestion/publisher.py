import os
import logging
from typing import List

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from common.models.Order import Order

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['TRANSFORM_EH_NAME']

PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

async def publish(orders: List[Order]):

    for order in orders:
        #Explicitly setting the partition key
        event_data_batch = await producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(order.id))
        
        try:
            event_data_batch.add(EventData(str(order.model_dump())))
        except ValueError:
            await producer.send_batch(event_data_batch)
            logging.info(F"Generate: Published {len(event_data_batch)} orders in a batch. (Exception)")
            
            event_data_batch = await producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(order.id))
            
            try:
                event_data_batch.add(EventData(str(order.model_dump())))
            except ValueError:
                logging.error("Generate: Message too large to fit into EventDataBatch object")

        await producer.send_batch(event_data_batch)
        logging.info(F"Generate: Published {len(event_data_batch)} orders in a batch")