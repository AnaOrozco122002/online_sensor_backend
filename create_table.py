import psycopg2

# URL externa de tu base de datos (Render)
DATABASE_URL = "postgresql://online_database_sensor_conect_user:J3Tx7edUNw2gTFDNx91qBcI6UwcaEatc@dpg-d2i9vp8gjchc73e86c10-a.oregon-postgres.render.com/online_database_sensor_conect"

# Conexión y creación de tabla
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS sensor_samples (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    activity TEXT,
    accel_x FLOAT,
    accel_y FLOAT,
    accel_z FLOAT,
    gyro_x FLOAT,
    gyro_y FLOAT,
    gyro_z FLOAT
);
""")

conn.commit()
conn.close()

print("✅ Tabla creada correctamente.")
