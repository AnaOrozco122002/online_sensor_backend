import asyncio
import websockets
import os
import json
import psycopg2
from datetime import datetime

# Conexión a la base de datos
DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

async def handle_connection(websocket):
    print("✅ Cliente conectado.")
    try:
        async for message in websocket:
            data = json.loads(message)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            print(f"\n📩 {timestamp} - Datos recibidos:")
            print(json.dumps(data, indent=2))

            # Insertar en base de datos
            cursor.execute("""
                INSERT INTO sensor_samples (
                    timestamp, activity,
                    accel_x, accel_y, accel_z,
                    gyro_x, gyro_y, gyro_z
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                data["timestamp"],
                data["activity"],
                data["accelerometer"]["x"],
                data["accelerometer"]["y"],
                data["accelerometer"]["z"],
                data["gyroscope"]["x"],
                data["gyroscope"]["y"],
                data["gyroscope"]["z"]
            ])
            conn.commit()

    except websockets.ConnectionClosed:
        print("❌ Cliente desconectado.")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handle_connection, "0.0.0.0", port):
        print(f"🚀 Servidor WebSocket en puerto {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
