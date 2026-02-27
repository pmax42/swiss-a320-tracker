import os
import streamlit as st
import pandas as pd
import requests
import pydeck as pdk
import time

api_host = os.getenv("API_HOST", "localhost")

st.set_page_config(
    page_title="Swiss A320 Tracker",
    page_icon="✈️",
    layout="wide"
)

def load_data():
    try:
        url = f"http://{api_host}:8000/flights/latest"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Unable to contact API: {e}")
        return None

# --- HEADER & TITLE ---
st.title("Swiss Air Traffic Tracker")
st.markdown("""
    ### Realtime Dashboard of flights above Switzerland and border regions.

    - **Stack:** Python ETL ; PostgreSQL ; FastAPI ; Streamlit
    - **Data Source:** OpenSky Network API (https://openskynetwork.github.io/opensky-api/rest.html)
    - **Note:** Data is updated every 10 seconds by the ETL process, but you can also trigger a manual refresh using the button below.

    ***Disclaimer:** This dashboard is for demonstration purposes only.*
""")

data_json = load_data()

if data_json and data_json['count'] > 0:
    df = pd.DataFrame(data_json['data'])
    
    df['velocity'] = df['velocity'].fillna(0)
    df['baro_altitude'] = df['baro_altitude'].fillna(0)

    # KPIs
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Number of flights", data_json['count'])
    with col2:
        st.metric("Average altitude", f"{int(df['baro_altitude'].mean())} m")
    with col3:
        max_speed = df['velocity'].max() * 3.6
        st.metric("Max Speed", f"{int(max_speed)} km/h")

    # MAP
    st.subheader(f"Map of the sky ({data_json['latest_ingestion']})")

    # REFRESH BUTTON
    if st.button('Refresh data (Real-time)'):
        with st.spinner("Refreshing data..."):
            try:
                refresh_url = f"http://{api_host}:8000/flights/refresh"
                
                response = requests.post(refresh_url)
                
                if response.status_code == 200:
                    st.success("Data refreshed successfully!")
                else:
                    st.error("Error refreshing data.")
                    
            except Exception as e:
                st.error(f"Failed to contact API for refresh: {e}")
                
        time.sleep(1)
        st.rerun()

    # Center on Switzerland
    view_state = pdk.ViewState(
        latitude=46.8182,
        longitude=8.2275,
        zoom=7,
        pitch=45,
    )

    # 3D ColumnLayer to represent planes
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

    tooltip = {
        "html": "<b>Flight:</b> {callsign} <br/> <b>Country:</b> {origin_country} <br/> <b>Altitude:</b> {baro_altitude}m",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
    )

    st.pydeck_chart(r)

    # Raw data table
    with st.expander("View raw data"):
        st.dataframe(df[['callsign', 'origin_country', 'velocity', 'baro_altitude', 'on_ground']])

else:
    st.warning("No flight data available at the moment. Please check that the ETL process is running.")