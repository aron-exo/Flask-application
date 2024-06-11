from flask import Flask, request, jsonify, render_template
import psycopg2
import json
import os

app = Flask(__name__)

# Database connection
conn = psycopg2.connect(
    host=os.getenv('SUPABASE_DB_HOST'),
    database=os.getenv('SUPABASE_DB_NAME'),
    user=os.getenv('SUPABASE_DB_USER'),
    password=os.getenv('SUPABASE_DB_PASSWORD'),
    port=os.getenv('SUPABASE_DB_PORT')
)
cur = conn.cursor()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/query_polygon', methods=['POST'])
def query_polygon():
    data = request.get_json()
    polygon_geojson = json.dumps(data['geometry'])

    # Example query (replace 'your_table' and 'geometry_column' with actual table and geometry column names)
    query = f"""
    SELECT * FROM your_table
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromGeoJSON('{polygon_geojson}'), 4326),
        your_table.geometry_column
    );
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    results = [dict(zip(columns, row)) for row in rows]

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
