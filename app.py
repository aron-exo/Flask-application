import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from streamlit_folium import st_folium

# Initialize session state for geometries if not already done
if 'geojson_list' not in st.session_state:
    st.session_state.geojson_list = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []

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
        geometry = json.loads(geojson)

        # Define the source and destination coordinate systems
        src_crs = pyproj.CRS(f"EPSG:{metadata.pop('srid')}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Transform the geometry to the geographic coordinate system
        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)
        
        popup_text = "<br>".join([f"{key}: {value}" for key, value in metadata.items() if key != 'SHAPE'])
        
        if transformed_geom.geom_type == 'Point':
            folium.Marker(location=[transformed_geom.y, transformed_geom.x], popup=popup_text).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords], popup=popup_text).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords], popup=popup_text).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            for line in transformed_geom.geoms:
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords], popup=popup_text).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {transformed_geom.geom_type}")

st.title('Streamlit Folium Map Application')

# Create a Folium map centered on Los Angeles
m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)

# Add geometries from session state to the map
if st.session_state.geojson_list:
    add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

# Handle the drawn polygon
if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    
    if st.button('Query Database'):
        try:
            df = query_geometries_within_polygon(polygon_geojson)
            if not df.empty:
                st.session_state.geojson_list = df['geometry'].tolist()
                st.session_state.metadata_list = df.drop(columns=['geometry', 'SHAPE']).to_dict(orient='records')
                add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)
                st_data = st_folium(m, width=700, height=500)
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

# Add drawing functionality using a hidden input to store the drawing
st.write("Draw a polygon on the map")
st.markdown(f"""
    <div id="map" style="height: 500px;"></div>
    <input type="hidden" id="last_active_drawing" value=''>
    <script src="https://api.tiles.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.js"></script>
    <script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.4.3/mapbox-gl-draw.js"></script>
    <link href="https://api.tiles.mapbox.com/mapbox-gl-js/v3.4.0/mapbox-gl.css" rel="stylesheet">
    <link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.4.3/mapbox-gl-draw.css" type="text/css">
    <script>
        mapboxgl.accessToken = '{st.secrets["mapbox_access_token"]}';
        var map = new mapboxgl.Map({{
            container: 'map',
            style: 'mapbox://styles/mapbox/satellite-v9',
            center: [-118.2437, 34.0522],
            zoom: 10
        }});
        var draw = new MapboxDraw({{
            displayControlsDefault: false,
            controls: {{
                polygon: true,
                trash: true
            }},
            defaultMode: 'draw_polygon'
        }});
        map.addControl(draw);
        map.on('draw.create', updateGeometry);
        map.on('draw.delete', updateGeometry);
        map.on('draw.update', updateGeometry);
        function updateGeometry(e) {{
            var data = draw.getAll();
            var polygon = data.features[0];
            document.getElementById('last_active_drawing').value = JSON.stringify(polygon.geometry);
        }}
    </script>
""", unsafe_allow_html=True)
