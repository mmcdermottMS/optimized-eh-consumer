import os
import asyncio
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
                        
            order_tasks = []
            for message in messages:
                order = Order.model_validate_json(message.get_body().decode('utf-8'))
                items: List[Item] = []
                for item in order.items:
                    items.append(item)
                 
                order_tasks.append(AsyncCosmosService.upsertItemsInBatch(items, order.id, "ItemsAsyncBatch"))
                
            await asyncio.gather(*order_tasks)
            
            await AsyncCosmosService.upsertItemsInSeries(items, order.id, "ItemsAsyncSeries")
            
            CosmosService.upsertItemsInSeries(items, order.id, "ItemsSyncSeries")
            
        except Exception as e:
            logging.error(e)
            raise
