import os
import time
import requests
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Optional, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

DB_CONNECTION_STR = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
engine = create_engine(DB_CONNECTION_STR)

BOUNDING_BOX = [45.0, 5.0, 48.0, 11.0]

def get_proxies() -> Optional[Dict[str, str]]:
    """Constructs the proxy dictionary from environment variables."""
    host = os.getenv("PROXY_HOST")
    port = os.getenv("PROXY_PORT")
    user = os.getenv("PROXY_USER")
    pwd = os.getenv("PROXY_PASS")

    if not host or not port:
        return None

    auth = f"{user}:{pwd}@" if user and pwd else ""
    proxy_url = f"http://{auth}{host}:{port}"
    
    return {"http": proxy_url, "https": proxy_url}

def fetch_flight_data() -> pd.DataFrame:
    """Fetches real-time flight data from OpenSky API."""
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": BOUNDING_BOX[0], "lomin": BOUNDING_BOX[1],
        "lamax": BOUNDING_BOX[2], "lomax": BOUNDING_BOX[3]
    }
    
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    proxies = get_proxies()
    
    try:
        # OAuth2 Token Request
        token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        token_response = requests.post(
            token_url, 
            data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}, 
            proxies=proxies, 
            timeout=10
        )
        
        if token_response.status_code != 200:
            logging.error(f"OAuth2 error: {token_response.text}")
            return pd.DataFrame()
            
        access_token = token_response.json().get("access_token")
        
        # API Request
        headers = {
            'User-Agent': 'SwissAirspaceTracker/1.0 (Portfolio Project)',
            'Authorization': f'Bearer {access_token}'
        }
        
        response = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('states'):
            return pd.DataFrame()

        columns = [
            "icao24", "callsign", "origin_country", "time_position", 
            "last_contact", "longitude", "latitude", "baro_altitude", 
            "on_ground", "velocity", "true_track", "vertical_rate", 
            "sensors", "geo_altitude", "squawk", "spi", "position_source"
        ]
        
        df = pd.DataFrame(data['states'], columns=columns)
        df['callsign'] = df['callsign'].str.strip()
        df_clean = df[['callsign', 'origin_country', 'longitude', 'latitude', 'velocity', 'baro_altitude', 'on_ground']].copy()
        df_clean['ingestion_time'] = datetime.utcnow()
        
        return df_clean

    except Exception as e:
        logging.error(f"ETL error: {e}")
        return pd.DataFrame()

def load_to_postgres(df: pd.DataFrame) -> None:
    """Loads dataframe to PostgreSQL and optimizes the table."""
    if df.empty:
        return

    try:
        df.to_sql('flights', engine, if_exists='append', index=False)
        
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ingestion_time ON flights(ingestion_time);"))
            conn.execute(text("DELETE FROM flights WHERE ingestion_time < NOW() - INTERVAL '2 hours';"))
            
        logging.info(f"Processed {len(df)} flights. DB optimized.")
    except Exception as e:
        logging.error(f"Database error: {e}")

def run_etl_cycle() -> None:
    try:
        flights = fetch_flight_data()
        if not flights.empty:
            load_to_postgres(flights)
        else:
            logging.warning("No data retrieved during this cycle.")
    except Exception as e:
        logging.error(f"Critical error during ETL cycle: {e}")

if __name__ == "__main__":
    logging.info("ETL process started. Fetching data every 90 seconds...")
    INTERVAL_SECONDS = 90

    while True:
        start_time = time.time()
        
        run_etl_cycle()
        
        execution_time = time.time() - start_time
        sleep_time = max(0.0, INTERVAL_SECONDS - execution_time)
        
        logging.info(f"Cycle completed in {execution_time:.2f}s. Next run in {sleep_time:.2f}s.")
        time.sleep(sleep_time)