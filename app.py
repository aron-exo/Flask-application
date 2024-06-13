import streamlit as st
import pandas as pd
import psycopg2
import json
import leafmap.foliumap as leafmap
from streamlit_folium import st_folium
from folium.plugins import Draw

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

# Query geometries within the current map view bounding box
def query_geometries_within_bbox(conn, min_lat, min_long, max_lat, max_long):
    try:
        query = f"""
        SELECT geometry
        FROM geometries_in_bbox({min_lat}, {min_long}, {max_lat}, {max_long});
        """
        st.write(f"Running query:")
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
            conn = get_connection()
            if conn:
                # Extract the bounding box coordinates from the polygon
                coords = st_data['last_active_drawing']['geometry']['coordinates'][0]
                min_long = min([point[0] for point in coords])
                max_long = max([point[0] for point in coords])
                min_lat = min([point[1] for point in coords])
                max_lat = max([point[1] for point in coords])
                
                df = query_geometries_within_bbox(conn, min_lat, min_long, max_lat, max_long)
                if not df.empty:
                    geojson_list = df['geometry'].tolist()
                    add_geometries_to_map(geojson_list, m)
                    st_data = st_folium(m, width=700, height=500)
                else:
                    st.write("No geometries found within the drawn polygon.")
                conn.close()
        except Exception as e:
            st.error(f"Error: {e}")
