import os
import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine

db_host = os.getenv("DB_HOST", "localhost")
DB_CONNECTION_STR = f"postgresql://admin:password123@{db_host}:5432/flight_tracker"

# Swiss Airspace Bounding Box (lat_min, lon_min, lat_max, lon_max)
BOUNDING_BOX = [45.0, 5.0, 48.0, 11.0]

def fetch_flight_data():
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": BOUNDING_BOX[0], "lomin": BOUNDING_BOX[1],
        "lamax": BOUNDING_BOX[2], "lomax": BOUNDING_BOX[3]
    }
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    
    try:
        # OAuth2 Token Request
        token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        token_data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        token_response = requests.post(token_url, data=token_data)
        
        if token_response.status_code != 200:
            print(f"OAuth2 error: {token_response.text}")
            return pd.DataFrame()
            
        access_token = token_response.json().get("access_token")
        
        # OpenSky API request with Bearer token
        headers = {
            'User-Agent': 'SwissA320Tracker/1.0 (Portfolio Project)',
            'Authorization': f'Bearer {access_token}'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data['states'] is None:
            return pd.DataFrame()

        columns = ["icao24", "callsign", "origin_country", "time_position", 
                   "last_contact", "longitude", "latitude", "baro_altitude", 
                   "on_ground", "velocity", "true_track", "vertical_rate", 
                   "sensors", "geo_altitude", "squawk", "spi", "position_source"]
        
        df = pd.DataFrame(data['states'], columns=columns)
        
        # TRANSFORMATION
        df['callsign'] = df['callsign'].str.strip()
        df_clean = df[['callsign', 'origin_country', 'longitude', 'latitude', 'velocity', 'baro_altitude', 'on_ground']].copy()
        df_clean['ingestion_time'] = datetime.utcnow()
        
        return df_clean

    except Exception as e:
        print(f"ETL error: {e}")
        return pd.DataFrame()

def load_to_postgres(df):
    if df.empty:
        print("No data to load.")
        return

    try:
        engine = create_engine(DB_CONNECTION_STR)
        df.to_sql('flights', engine, if_exists='append', index=False)
        print(f"Success: {len(df)} rows inserted into PostgreSQL.")
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    print("Process started. Fetching flight data every 10 seconds...")

    while True:
        try:
            print(f"[{datetime.utcnow()}] Fetching flight data...")
            flights = fetch_flight_data()
            load_to_postgres(flights)
            time.sleep(10)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(30)