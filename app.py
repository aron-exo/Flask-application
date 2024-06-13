import streamlit as st
import pandas as pd
import psycopg2
import json
import plotly.express as px
import plotly.graph_objects as go
from shapely.geometry import shape
from shapely.ops import transform
import pyproj

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

# Function to plot geometries with Plotly
def plot_geometries(geojson_list, metadata_list):
    fig = go.Figure()

    for geojson, metadata in zip(geojson_list, metadata_list):
        geometry = json.loads(geojson)

        if geometry['type'] == 'MultiLineString':
            for line in geometry['coordinates']:
                lons, lats = zip(*line)
                text = "<br>".join([f"{key}: {value}" for key, value in metadata.items()])
                fig.add_trace(go.Scattermapbox(
                    mode="lines",
                    lon=lons,
                    lat=lats,
                    text=text,
                    hoverinfo="text"
                ))

        elif geometry['type'] == 'LineString':
            lons, lats = zip(*geometry['coordinates'])
            text = "<br>".join([f"{key}: {value}" for key, value in metadata.items()])
            fig.add_trace(go.Scattermapbox(
                mode="lines",
                lon=lons,
                lat=lats,
                text=text,
                hoverinfo="text"
            ))

        elif geometry['type'] == 'Point':
            lon, lat = geometry['coordinates']
            text = "<br>".join([f"{key}: {value}" for key, value in metadata.items()])
            fig.add_trace(go.Scattermapbox(
                mode="markers",
                lon=[lon],
                lat=[lat],
                text=text,
                hoverinfo="text"
            ))

    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=34.0522, lon=-118.2437),
            zoom=10
        ),
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    return fig

st.title('Streamlit Plotly Map Application')

# Initialize session state for geometries if not already done
if 'geojson_list' not in st.session_state:
    st.session_state.geojson_list = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []

# Handle the drawn polygon
if 'last_active_drawing' in st.session_state:
    polygon_geojson = json.dumps(st.session_state['last_active_drawing']['geometry'])
    
    if st.button('Query Database'):
        try:
            df = query_geometries_within_polygon(polygon_geojson)
            if not df.empty:
                st.session_state.geojson_list = df['geometry'].tolist()
                st.session_state.metadata_list = df.drop(columns=['geometry', 'SHAPE']).to_dict(orient='records')
                st.session_state['last_active_drawing'] = None
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

# Create a Plotly map centered on Los Angeles
fig = go.Figure()
if st.session_state.geojson_list:
    fig = plot_geometries(st.session_state.geojson_list, st.session_state.metadata_list)

st.plotly_chart(fig, use_container_width=True)

# Add drawing functionality using a hidden input to store the drawing
st.write("Draw a polygon on the map")
st.markdown(f"""
    <div id="map" style="height: 500px;"></div>
    <input type="hidden" id="last_active_drawing" value=''>
    <script src="https://api.tiles.mapbox.com/mapbox-gl-js/v1.12.0/mapbox-gl.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@mapbox/mapbox-gl-draw@1.2.0/dist/mapbox-gl-draw.min.js"></script>
    <link href="https://api.tiles.mapbox.com/mapbox-gl-js/v1.12.0/mapbox-gl.css" rel="stylesheet" />
    <link href="https://cdn.jsdelivr.net/npm/@mapbox/mapbox-gl-draw@1.2.0/dist/mapbox-gl-draw.css" rel="stylesheet" />
    <script>
        mapboxgl.accessToken = '{st.secrets["mapbox_access_token"]}';
        var map = new mapboxgl.Map({{
            container: 'map',
            style: 'mapbox://styles/mapbox/streets-v11',
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
        map.on('draw.update', updateGeometry);
        function updateGeometry(e) {{
            var data = draw.getAll();
            var polygon = data.features[0];
            document.getElementById('last_active_drawing').value = JSON.stringify(polygon.geometry);
        }}
    </script>
""", unsafe_allow_html=True)
