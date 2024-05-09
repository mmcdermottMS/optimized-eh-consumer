import azure.functions as func
import logging

# Azure Application Insights setup
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="message", event_hub_name="transform", connection="EH_CONN_STR") 
async def transform(message: func.EventHubEvent, context):
    
    # Extract the trace context from the incoming request
    carrier = {
      "traceparent": context.trace_context.Traceparent,
      "tracestate": context.trace_context.Tracestate,
    }
    
    # Create a new Open Telemetry span with the incoming trace context
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("transform", context = extract(carrier)): # span name matches the function name
        try:
            logging.info('Python EventHub trigger processed an event: %s', message.get_body().decode('utf-8'))
            
        except Exception as e:
            logging.error(e)
            raise
