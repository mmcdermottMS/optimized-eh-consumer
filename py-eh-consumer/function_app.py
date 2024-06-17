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

from models.Order import Order
from models.Item import Item
import Services.AsyncCosmosService as AsyncCosmosService
import Services.CosmosService as CosmosService

app = func.FunctionApp()

configure_azure_monitor()

@app.event_hub_message_trigger(
    arg_name="messages", 
    event_hub_name="transform", 
    connection="EH_CONN_STR",
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
                order_tasks.append(AsyncCosmosService.upsertItemsInBatch(items, order.id, "ItemsAsyncBatch"))
            
            #Upsert the order items using transactional batch operations
            start = time.perf_counter()    
            await asyncio.gather(*order_tasks)
            logging.info(f"OpType|Async Batch|{len(messages)}|{total_item_count}|{time.perf_counter() - start:0.4f}")
            
            #Upsert the order items asynchronously, but in series
            start = time.perf_counter() 
            await AsyncCosmosService.upsertItemsInSeries(items, order.id, "ItemsAsyncSeries")
            logging.info(f"OpType|Async Series|{len(messages)}|{total_item_count}|{time.perf_counter() - start:0.4f}")
            
            #Upsert the order items synchronously, but in series
            start = time.perf_counter() 
            CosmosService.upsertItemsInSeries(items, order.id, "ItemsSyncSeries")
            logging.info(f"OpType|Sync Series|{len(messages)}|{total_item_count}|{time.perf_counter() - start:0.4f}")
            
        except Exception as e:
            logging.error(e)
            raise
