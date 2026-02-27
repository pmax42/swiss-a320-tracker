import os
import json
import pandas as pd
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from etl_script import fetch_flight_data, load_to_postgres

db_host = os.getenv("DB_HOST", "localhost")
DB_CONNECTION_STR = f"postgresql://admin:password123@{db_host}:5432/flight_tracker"

app = FastAPI(
    title="Swiss A320 Tracker API",
    description="API for retrieving real-time flight data above Switzerland",
    version="1.0.0"
)
engine = create_engine(DB_CONNECTION_STR)

@app.get("/")
def read_root():
    return {"status": "online", "message": "Welcome to the Swiss A320 Tracker API"}

@app.get("/flights/latest")
def get_latest_flights():
    """
    Retrieves the most recent batch of flight data.
    Returns an empty list if no data is available.
    """
    try:
        with engine.connect() as conn:
            query_max = text("SELECT MAX(ingestion_time) FROM flights")
            latest_time = conn.execute(query_max).scalar()
            
        if not latest_time:
            return {"count": 0, "data": []}

        query_flights = text("SELECT * FROM flights WHERE ingestion_time = :time")
        df = pd.read_sql(query_flights, engine, params={"time": latest_time})

        # Workaround: Pandas converts None to NaN, which breaks JSON serialization.
        # Exporting to JSON string first forces NaN to standard JSON 'null'.
        json_str = df.to_json(orient="records", date_format="iso")
        result = json.loads(json_str)
        
        return {
            "latest_ingestion": str(latest_time),
            "count": len(result),
            "data": result
        }

    except Exception as e:
        print(f"API error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/flights/refresh")
def force_refresh_etl():
    """
    Endpoint to trigger a manual refresh of flight data.
    This will call the ETL functions to fetch new data and store it in the database.
    """
    try:
        flights = fetch_flight_data()
        
        if not flights.empty:
            load_to_postgres(flights)
            return {"status": "success", "message": f"{len(flights)} new flights inserted."}
        else:
            return {"status": "warning", "message": "No flights retrieved."}
            
    except Exception as e:
        print(f"Error during forced refresh: {e}")
        raise HTTPException(status_code=500, detail=str(e))