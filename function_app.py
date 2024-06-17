import azure.functions as func 
from generation.generate import generate_function_bp
from ingestion.ingest import ingest_function_bp
from transformation.transform import transform_function_bp

app = func.FunctionApp() 

app.register_functions(generate_function_bp)
app.register_functions(ingest_function_bp)
app.register_functions(transform_function_bp)
