import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from streamlit_folium import st_folium

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
        SELECT "SHAPE"::text as geometry, srid
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

# Function to add geometries to map with coordinate transformation
def add_geometries_to_map(geojson_list, srid_list, map_object):
    for geojson, srid in zip(geojson_list, srid_list):
        st.write(f"Adding geometry to map: {geojson}")
        geometry = json.loads(geojson)

        # Define the source and destination coordinate systems
        src_crs = pyproj.CRS(f"EPSG:{srid}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Transform the geometry to the geographic coordinate system
        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)
        
        if transformed_geom.geom_type == 'Point':
            folium.Marker(location=[transformed_geom.y, transformed_geom.x]).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords]).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords]).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            for line in transformed_geom:
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords]).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {transformed_geom.geom_type}")

st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles
m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

if st.button('Display All Geometries'):
    try:
        conn = get_connection()
        if conn:
            df = query_all_geometries(conn)
            if not df.empty:
                geojson_list = df['geometry'].tolist()
                srid_list = df['srid'].tolist()
                add_geometries_to_map(geojson_list, srid_list, m)
                st_data = st_folium(m, width=700, height=500)
            else:
                st.write("No geometries found in the table.")
            conn.close()
    except Exception as e:
        st.error(f"Error: {e}")
