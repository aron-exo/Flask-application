import streamlit as st
import pandas as pd
import psycopg2
import json
import os
from dotenv import load_dotenv
import folium
from streamlit_folium import st_folium
from folium.plugins import Draw

# Load environment variables
load_dotenv()

# Database connection function
def get_connection():
    return psycopg2.connect(
        host=os.getenv('SUPABASE_DB_HOST'),
        database=os.getenv('SUPABASE_DB_NAME'),
        user=os.getenv('SUPABASE_DB_USER'),
        password=os.getenv('SUPABASE_DB_PASSWORD'),
        port=os.getenv('SUPABASE_DB_PORT')
    )

# Query data from the database
def query_data(polygon_geojson):
    conn = get_connection()
    query = f"""
    SELECT * FROM your_table
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromGeoJSON('{polygon_geojson}'), 4326),
        your_table.geometry_column
    );
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title('Streamlit Map Application')

# Create a Folium map
m = folium.Map(location=[51.505, -0.09], zoom_start=13)

# Add drawing options to the map
draw = Draw(
    export=True,
    filename='data.geojson',
    position='topleft',
    draw_options={'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False},
    edit_options={'edit': False}
)
draw.add_to(m)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

# Handle the drawn polygon
if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    st.write('Polygon GeoJSON:', polygon_geojson)
    
    if st.button('Query Database'):
        try:
            df = query_data(polygon_geojson)
            st.write(df)
        except Exception as e:
            st.error(f"Error: {e}")
