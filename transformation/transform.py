import os
import asyncio
import time
import azure.functions as func
import logging
from typing import List

# Azure Application Insights setup
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

from common.models.Order import Order
from common.models.Item import Item
import transformation.CosmosService as CosmosService

app = func.FunctionApp()

transform_function_bp = func.Blueprint()

configure_azure_monitor()

TRANSFORM_EH_NAME = os.environ['TRANSFORM_EH_NAME']
COSMOS_CONTAINER_NAME = os.environ['COSMOS_CONTAINER_NAME']

@transform_function_bp.event_hub_message_trigger(
    arg_name="messages", 
    event_hub_name=TRANSFORM_EH_NAME, 
    connection="EHNS_CONN_STRING",
    cardinality=func.Cardinality.MANY
) 
async def transform(messages: List[func.EventHubEvent], context) -> None:
    
    # Extract the trace context from the incoming request
    carrier = {
        "traceparent": context.trace_context.Traceparent, 
        "tracestate": context.trace_context.Tracestate,
    }
    
    # Create a new Open Telemetry span with the incoming trace context
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("transform", context = extract(carrier)): # span name matches the function name
        try:
            logging.info(f'{len(messages)} Order events received.')
                 
            total_item_count = 0;       
            order_tasks = []
            for message in messages:
                order = Order.model_validate_json(message.get_body().decode('utf-8'))
                items: List[Item] = []
                total_item_count += len(order.items)
                for item in order.items:
                    items.append(item)
                
                #Create a list of async tasks to upsert the items in a batch.  One task per
                #message recieved.
                order_tasks.append(CosmosService.upsertItemsInBatch(items, order.id, COSMOS_CONTAINER_NAME))
            
            #Upsert the order items using transactional batch operations
            start = time.perf_counter()    
            await asyncio.gather(*order_tasks)
            logging.info(f"OpType|Async Batch|{len(messages)}|{total_item_count}|{time.perf_counter() - start:0.4f}")
            
        except Exception as e:
            logging.error(e)
            raise
