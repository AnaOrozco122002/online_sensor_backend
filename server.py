# server.py
import asyncio
import websockets
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

DATA_DIR = Path("data")
SAMPLES_DIR = DATA_DIR / "samples"
WINDOWS_DIR = DATA_DIR / "windows"
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
WINDOWS_DIR.mkdir(parents=True, exist_ok=True)

def is_window_payload(d: Dict[str, Any]) -> bool:
    """Heurística: una ventana trae 'features' y 'start_time'/'end_time'."""
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def is_sample_payload(d: Dict[str, Any]) -> bool:
    """Heurística: un sample debe traer los 6 ejes; 'timestamp' y 'activity' son opcionales."""
    required = {"ax", "ay", "az", "gx", "gy", "gz"}
    return isinstance(d, dict) and required.issubset(d.keys())

def today_file(base_dir: Path) -> Path:
    return base_dir / (datetime.utcnow().strftime("%Y-%m-%d") + ".jsonl")

def safe_jsonl_write(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

async def handle_connection(websocket):
    peer = websocket.remote_address
    print(f"✅ Cliente conectado: {peer}")
    try:
        async for message in websocket:
            ts = datetime.utcnow().isoformat() + "Z"
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON inválido ({e}); mensaje crudo:\n{message[:500]}")
                # Opcional: responder error
                await websocket.send(json.dumps({"ok": False, "error": "invalid_json"}))
                continue

            # Normaliza a dict (si te llega lista, procesa elemento a elemento)
            items = data if isinstance(data, list) else [data]

            for item in items:
                if is_window_payload(item):
                    # ----- Ventana -----
                    window_rec = {
                        "received_at": ts,
                        "type": "window",
                        # Campos clave esperados:
                        "start_time": item.get("start_time"),
                        "end_time": item.get("end_time"),
                        "sample_count": item.get("sample_count"),
                        "sample_rate_hz": item.get("sample_rate_hz"),
                        "activity": item.get("activity"),
                        "features": item.get("features"),
                        # Si te interesa conservar crudo, guárdalo; si no, comenta la línea:
                        "samples": item.get("samples"),  # <-- comenta si no quieres guardar crudo
                    }
                    safe_jsonl_write(today_file(WINDOWS_DIR), window_rec)

                    print("\n📦 Ventana recibida:")
                    print(f"  ⏱  {window_rec['start_time']} → {window_rec['end_time']}")
                    print(f"  🔢  muestras={window_rec['sample_count']} fs={window_rec['sample_rate_hz']}")
                    print(f"  🏷  activity={window_rec['activity']}")
                    print(f"  🧮  features={len(window_rec['features'] or {})}")

                elif is_sample_payload(item):
                    # ----- Muestra suelta -----
                    sample_rec = {
                        "received_at": ts,
                        "type": "sample",
                        "timestamp": item.get("timestamp"),  # puede venir del cliente o ser None
                        "ax": item.get("ax"),
                        "ay": item.get("ay"),
                        "az": item.get("az"),
                        "gx": item.get("gx"),
                        "gy": item.get("gy"),
                        "gz": item.get("gz"),
                        "activity": item.get("activity"),
                    }
                    safe_jsonl_write(today_file(SAMPLES_DIR), sample_rec)

                    print("\n📩 Muestra recibida:")
                    print(json.dumps(sample_rec, indent=2, ensure_ascii=False))

                else:
                    # Desconocido: lo guardamos tal cual para diagnóstico
                    unknown_rec = {
                        "received_at": ts,
                        "type": "unknown",
                        "payload": item,
                    }
                    safe_jsonl_write(today_file(DATA_DIR), unknown_rec)
                    print("\n❓ Payload desconocido, guardado en data/YYYY-MM-DD.jsonl")

            # Opcional: ACK simple para que el cliente sepa que llegó
            try:
                await websocket.send(json.dumps({"ok": True}))
            except Exception:
                pass

    except websockets.ConnectionClosed:
        print(f"❌ Cliente desconectado: {peer}")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

async def main():
    port = int(os.environ.get("PORT", 8080))
    # Escucha en raíz ("/"); tu cliente usa wss://... sin ruta adicional
    async with websockets.serve(handle_connection, "0.0.0.0", port, ping_interval=30, ping_timeout=30):
        print(f"🚀 Servidor WebSocket escuchando en puerto {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
