import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from streamlit_folium import st_folium
from folium.plugins import Draw

# Initialize session state for geometries if not already done
if 'geojson_list' not in st.session_state:
    st.session_state.geojson_list = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []
if 'map_initialized' not in st.session_state:
    st.session_state.map_initialized = False

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

# Query geometries within a polygon
def query_geometries_within_polygon(polygon_geojson):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = f"""
        SELECT *, "SHAPE"::text as geometry, srid
        FROM public.rwmainlineonli_exportfeature
        WHERE ST_Intersects(
            ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("SHAPE"::json), srid), 4326),
            ST_SetSRID(
                ST_GeomFromGeoJSON('{polygon_geojson}'),
                4326
            )
        );
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# Function to add geometries to map with coordinate transformation
def add_geometries_to_map(geojson_list, metadata_list, map_object):
    for geojson, metadata in zip(geojson_list, metadata_list):
        if 'srid' not in metadata:
            st.error(f"SRID not found in metadata: {metadata}")
            continue

        srid = metadata.pop('srid')
        geometry = json.loads(geojson)

        # Define the source and destination coordinate systems
        src_crs = pyproj.CRS(f"EPSG:{srid}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Transform the geometry to the geographic coordinate system
        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)

        # Remove the 'geometry' field from metadata for the popup
        metadata.pop('geometry', None)
        
        # Create a popup with metadata (other columns)
        metadata_html = "<br>".join([f"<b>{key}:</b> {value}" for key, value in metadata.items()])
        popup = folium.Popup(metadata_html, max_width=300)

        if transformed_geom.geom_type == 'Point':
            folium.Marker(location=[transformed_geom.y, transformed_geom.x], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            for line in transformed_geom.geoms:
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords], popup=popup).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {transformed_geom.geom_type}")

st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles if not already done
if not st.session_state.map_initialized:
    m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)
    draw = Draw(
        export=True,
        filename='data.geojson',
        position='topleft',
        draw_options={'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False},
        edit_options={'edit': False}
    )
    draw.add_to(m)
    st.session_state.map = m
    st.session_state.map_initialized = True

# Handle the drawn polygon
st_data = st_folium(st.session_state.map, width=700, height=500, key="initial_map")

if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    st.write('Polygon GeoJSON:', polygon_geojson)
    
    if st.button('Query Database'):
        try:
            df = query_geometries_within_polygon(polygon_geojson)
            if not df.empty:
                st.session_state.geojson_list = df['geometry'].tolist()
                st.session_state.metadata_list = df.drop(columns=['geometry', 'SHAPE']).to_dict(orient='records')
                
                # Clear the existing map and reinitialize it
                m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)
                draw.add_to(m)
                add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)
                st.session_state.map = m
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

# Display the map using Streamlit-Folium
st_folium(st.session_state.map, width=700, height=500, key="final_map")
