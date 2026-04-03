import os
import pandas as pd
import requests
import pydeck as pdk
import pytz
from dateutil import parser
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_javascript import st_javascript

API_HOST = os.getenv("API_HOST", "localhost")

st.set_page_config(
    page_title="Swiss Airspace Tracker",
    page_icon="✈️",
    layout="wide"
)

if os.getenv("STREAMLIT_UI_HIDE_TOP_BAR") == "true":
    st.markdown("""
        <style>
            [data-testid="stMainBlockContainer"] {
                padding-top: 2rem; 
            }
            footer {
                visibility: hidden;
            }
        </style>
    """, unsafe_allow_html=True)

# Auto-refresh the dashboard every 90 seconds
st_autorefresh(interval=90000, key="data_refresh")

# Initialize session state variables
if 'client_tz_str' not in st.session_state:
    st.session_state.client_tz_str = None

def load_data():
    """Fetches the latest flight data from the backend API."""
    try:
        response = requests.get(f"http://{API_HOST}:8000/flights/latest", timeout=10)
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        st.error(f"API Connection Error: {e}")
        return None

st.title("🇨🇭 Swiss Air Traffic Tracker")
st.markdown("""
    ### Real-time flight monitoring over Switzerland
    - **Tech Stack:** Python, PostgreSQL, FastAPI, Streamlit, Docker
    - **Source:** OpenSky Network API
    - **Dashboard:** Auto-refreshes every 90 seconds with live flight data
    - **CI/CD Pipeline (GitHub Actions):**
        - Automatic linting and testing on every commit
        - Multi-stage Docker build for ETL, API, and Dashboard services
        - Automated deployment to AWS Lightsail on push to main branch
""")

data_json = load_data()

if data_json and data_json.get('count', 0) > 0:
    df = pd.DataFrame(data_json['data'])
    df['velocity'] = df['velocity'].fillna(0)
    df['baro_altitude'] = df['baro_altitude'].fillna(0)

    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Flights", data_json['count'])
    col2.metric("Average Altitude", f"{int(df['baro_altitude'].mean())} m")
    col3.metric("Max Speed", f"{int(df['velocity'].max() * 3.6)} km/h")
    col4.metric("Aircraft on Ground", int(df['on_ground'].sum()))

    # --- Client Timezone Handling (Optimized) ---
    # Only fetch timezone via JS once per session to avoid infinite reruns/slowness
    if not st.session_state.client_tz_str:
        tz = st_javascript("Intl.DateTimeFormat().resolvedOptions().timeZone")
        if tz:
            st.session_state.client_tz_str = tz

    display_time_str = data_json['latest_ingestion']

    if st.session_state.client_tz_str:
        try:
            utc_dt = parser.parse(data_json['latest_ingestion']).replace(tzinfo=pytz.UTC)
            client_tz = pytz.timezone(st.session_state.client_tz_str)
            local_dt = utc_dt.astimezone(client_tz)
            display_time_str = local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            pass
            
    st.subheader(f"Airspace Map (Updated: {display_time_str})")

    # 3D Map Configuration
    layer = pdk.Layer(
        "ColumnLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_elevation="baro_altitude",
        elevation_scale=5,
        radius=2000,
        get_fill_color=[255, 0, 0, 140],
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=46.8182, longitude=8.2275, zoom=7, pitch=45
    )

    tooltip = {
        "html": "<b>Callsign:</b> {callsign} <br/> <b>Origin:</b> {origin_country} <br/> <b>Altitude:</b> {baro_altitude} m",
        "style": {"backgroundColor": "#2c3e50", "color": "white"}
    }

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
    ))

    with st.expander("View Raw Data"):
        st.dataframe(df[['callsign', 'origin_country', 'velocity', 'baro_altitude', 'on_ground']])

else:
    st.info("Awaiting flight data. The ETL process fetches new data every 90 seconds.")