import azure.functions as func

#Each function blueprint must be imported here 
from transformation.transform import transform_function_bp
from aggregation.aggregate import aggregate_function_bp
from consolidation.consolidate import consolidate_function_bp

#Per the Python v2 programming model, there can only be a single entrypoint for the Azure Functions runtime
#More reading: https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python?tabs=asgi%2Capplication-level&pivots=python-mode-decorators#alternative-entry-point
app = func.FunctionApp() 

#Once each function has been imported, register them here
#More reading: https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python?tabs=asgi%2Capplication-level&pivots=python-mode-decorators#blueprints
app.register_functions(transform_function_bp)
app.register_functions(aggregate_function_bp)
app.register_functions(consolidate_function_bp)