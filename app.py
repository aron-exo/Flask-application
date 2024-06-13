import streamlit as st
import pandas as pd
import psycopg2
import json
import leafmap.foliumap as leafmap
from streamlit_folium import st_folium
import folium

# Database connection function
def get_connection():
    try:
        conn = psycopg2.connect(
            host=st.secrets["db_host"],
            database=st.secrets["db_name"],
            user=st.secrets["db_user"],
            password=st.secrets["db_password"],
            port=st.secrets["db_port"]
        )
        st.write("Connection to database established.")
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Query all geometries from the table
def query_all_geometries(conn):
    try:
        query = """
        SELECT ST_AsGeoJSON(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("SHAPE"::text), srid), 4326)) as geometry
        FROM public.rwmainlineonli_exportfeature
        WHERE "SHAPE" IS NOT NULL;
        """
        st.write("Running query:")
        st.write(query)
        df = pd.read_sql(query, conn)
        st.write(f"Result: {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# Function to add geometries to map
def add_geometries_to_map(geojson_list, map_object):
    for geojson in geojson_list:
        st.write(f"Adding geometry to map: {geojson}")
        geometry = json.loads(geojson)
        
        if geometry['type'] == 'Point':
            folium.Marker(location=[geometry['coordinates'][1], geometry['coordinates'][0]]).add_to(map_object)
        elif geometry['type'] == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in geometry['coordinates']]).add_to(map_object)
        elif geometry['type'] == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in geometry['coordinates'][0]]).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {geometry['type']}")

st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles
m = leafmap.Map(center=[34.0522, -118.2437], zoom_start=10)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

if st.button('Display All Geometries'):
    try:
        conn = get_connection()
        if conn:
            df = query_all_geometries(conn)
            if not df.empty:
                geojson_list = df['geometry'].tolist()
                add_geometries_to_map(geojson_list, m)
                st_data = st_folium(m, width=700, height=500)
            else:
                st.write("No geometries found in the table.")
            conn.close()
    except Exception as e:
        st.error(f"Error: {e}")
