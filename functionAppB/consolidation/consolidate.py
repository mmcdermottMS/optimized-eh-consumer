#Import the Azure Open Telemetry and native Open Telemetry modules
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

import azure.functions as func 

#Instead of creating the app as an azure.functions app, we create it as a blueprint instead
#This allows us to use the app as a blueprint in the main function_app.py entry point used by the Azure Functions runtime
consolidate_function_bp = func.Blueprint() 

configure_azure_monitor()

@consolidate_function_bp.route(route="consolidate") 
async def consolidate(req: func.HttpRequest, context) -> func.HttpResponse:
    #From https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    #Capture the OTel traceparent and tracestate from the current execution context, these will be passed along in the downstream calls
    carrier = {
      "traceparent": context.trace_context.Traceparent,
      "tracestate": context.trace_context.Tracestate,
    }
    
    #Grab a handle to the current tracer and start a new span, but pass in the details captured above from the current TraceContext
    tracer = trace.get_tracer(__name__)    
    with tracer.start_as_current_span("consolidate_items", context=extract(carrier)): #Give the span a unique name, it will show up in App Insights as a parent dependency to all calls made within the span
        
        return func.HttpResponse(f"Consolidation Function Executed", status_code=200)