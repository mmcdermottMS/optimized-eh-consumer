import random

#Import the Azure Open Telemetry and native Open Telemetry modules
from azure.monitor.opentelemetry import configure_azure_monitor
from generation.publisher import publish
from opentelemetry import trace
from opentelemetry.propagate import extract

from common.models.Item import Item
from common.models.Order import Order

import azure.functions as func 

generate_function_bp = func.Blueprint() 

configure_azure_monitor()

@generate_function_bp.route(route="generate") 
async def generate(req: func.HttpRequest, context) -> func.HttpResponse: 

 #From https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    #Capture the OTel traceparent and tracestate from the current execution context, these will be passed along in the downstream calls
    carrier = {
      "traceparent": context.trace_context.Traceparent,
      "tracestate": context.trace_context.Tracestate,
    }
    
    #Grab a handle to the current tracer and start a new span, but pass in the details captured above from the current TraceContext
    tracer = trace.get_tracer(__name__)    
    with tracer.start_as_current_span("generate_items", context=extract(carrier)): #Give the span a unique name, it will show up in App Insights as a parent dependency to all calls made within the span
        message_count_str = req.params.get('messageCount')
        if not message_count_str:
            try:
                req_body = req.get_json()
            except ValueError:
                message_count_str = "1"
            else:
                message_count_str = req_body.get('messageCount')
                
        try: 
            message_count: int = int(message_count_str)
        except ValueError:
            message_count: int = 1

        orders = []
        for x in range(message_count):
            order_id = str(random.randint(1, 1000))
            item_count = random.randint(1, 100)
            order_items = []
            for y in range(item_count):
                id = str(random.randint(1, 1000000))
                price = random.uniform(0, 1000)
                order_item = Item(id=id, order_id=order_id, description=f"Item {y} on Order {x}", price=price)
                order_items.append(order_item)
            order = Order(id=order_id, items=order_items)
            orders.append(order)
        
        await publish(orders)
        
        return func.HttpResponse(f"Generate: {message_count_str} events sent to Event Hub.", status_code=200)