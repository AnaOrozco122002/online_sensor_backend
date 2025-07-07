import asyncio
import websockets
import os
import json
from datetime import datetime

async def handle_connection(websocket):
    print("✅ Cliente conectado.")
    try:
        async for message in websocket:
            data = json.loads(message)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n📩 {timestamp} - Datos recibidos:")
            print(json.dumps(data, indent=2))
    except websockets.ConnectionClosed:
        print("❌ Cliente desconectado.")

async def main():
    port = int(os.environ.get("PORT", 8080))  # Render usa PORT env var
    async with websockets.serve(handle_connection, "0.0.0.0", port):
        print(f"🚀 Servidor WebSocket en puerto {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
