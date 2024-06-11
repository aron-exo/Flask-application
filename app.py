import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
from streamlit_folium import st_folium
from folium.plugins import Draw

# Database connection function
def get_connection():
    return psycopg2.connect(
        host=st.secrets["db_host"],
        database=st.secrets["db_name"],
        user=st.secrets["db_user"],
        password=st.secrets["db_password"],
        port=st.secrets["db_port"]
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

# Display secrets for debugging purposes (remove in production)
st.write("DB username:", st.secrets["db_user"])
st.write("DB password:", st.secrets["db_password"])
st.write("My cool secrets:", st.secrets["my_cool_secrets"]["things_i_like"])
