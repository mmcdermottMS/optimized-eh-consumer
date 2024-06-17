import os
import azure.functions as func
import logging
from typing import List

# Azure Application Insights setup
from azure.monitor.opentelemetry import configure_azure_monitor
from ingestion.publisher import publish
from opentelemetry import trace
from opentelemetry.propagate import extract

from common.models.Order import Order

ingest_function_bp = func.Blueprint()

configure_azure_monitor()

INGEST_EH_NAME = os.environ['INGEST_EH_NAME']

@ingest_function_bp.event_hub_message_trigger(
    arg_name="messages", 
    event_hub_name=INGEST_EH_NAME, 
    connection="EHNS_CONN_STRING",
    cardinality=func.Cardinality.MANY
) 
async def ingest(messages: List[func.EventHubEvent], context) -> None:
    
    # Extract the trace context from the incoming request
    carrier = {
        "traceparent": context.trace_context.Traceparent, 
        "tracestate": context.trace_context.Tracestate,
    }
    
    # Create a new Open Telemetry span with the incoming trace context
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("ingest", context = extract(carrier)): # span name matches the function name
        try:
            logging.info(f'{len(messages)} Order events received.')
                 
            orders = []
            for message in messages:
                order = Order.model_validate_json(message.get_body().decode('utf-8'))
                orderId = int(order.id)
                
                #Simulate a filtering of messages from ingestion to transformation
                if(orderId % 9 != 0):
                    orders.append(order)
            
            await publish(orders)
                
        except Exception as e:
            logging.error(e)
            raise
