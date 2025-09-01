# server.py
import asyncio
import websockets
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, DefaultDict
from collections import defaultdict

DATA_DIR = Path("data")
SAMPLES_DIR = DATA_DIR / "samples"
WINDOWS_DIR = DATA_DIR / "windows"
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
WINDOWS_DIR.mkdir(parents=True, exist_ok=True)

# Contadores en memoria de líneas por archivo .jsonl
LINE_COUNTS: DefaultDict[str, int] = defaultdict(int)

def is_window_payload(d: Dict[str, Any]) -> bool:
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def is_sample_payload(d: Dict[str, Any]) -> bool:
    required = {"ax", "ay", "az", "gx", "gy", "gz"}
    return isinstance(d, dict) and required.issubset(d.keys())

def today_file(base_dir: Path) -> Path:
    return base_dir / (datetime.utcnow().strftime("%Y-%m-%d") + ".jsonl")

def safe_jsonl_write(path: Path, obj: Dict[str, Any]) -> int:
    """Escribe una línea JSONL y devuelve el total de líneas escritas (según contador en memoria)."""
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    # Actualiza contador
    LINE_COUNTS[str(path)] += 1
    return LINE_COUNTS[str(path)]

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
                await websocket.send(json.dumps({"ok": False, "error": "invalid_json"}))
                continue

            items = data if isinstance(data, list) else [data]
            acks = []  # acumulamos ACKs por item

            for item in items:
                if is_window_payload(item):
                    save_path = today_file(WINDOWS_DIR)
                    rec = {
                        "received_at": ts,
                        "type": "window",
                        "start_time": item.get("start_time"),
                        "end_time": item.get("end_time"),
                        "sample_count": item.get("sample_count"),
                        "sample_rate_hz": item.get("sample_rate_hz"),
                        "activity": item.get("activity"),
                        "features": item.get("features"),
                        "samples": item.get("samples"),
                    }
                    total_lines = safe_jsonl_write(save_path, rec)

                    print("\n📦 Ventana recibida:")
                    print(f"  ⏱  {rec['start_time']} → {rec['end_time']}")
                    print(f"  🔢  muestras={rec['sample_count']} fs={rec['sample_rate_hz']}")
                    print(f"  🏷  activity={rec['activity']}")
                    print(f"  🧮  features={len((rec['features'] or {}))}")
                    print(f"  💾 Guardado en: {save_path} (líneas={total_lines})")

                    acks.append({"ok": True, "type": "window",
                                 "saved_to": str(save_path), "lines": total_lines})

                elif is_sample_payload(item):
                    save_path = today_file(SAMPLES_DIR)
                    rec = {
                        "received_at": ts,
                        "type": "sample",
                        "timestamp": item.get("timestamp"),
                        "ax": item.get("ax"), "ay": item.get("ay"), "az": item.get("az"),
                        "gx": item.get("gx"), "gy": item.get("gy"), "gz": item.get("gz"),
                        "activity": item.get("activity"),
                    }
                    total_lines = safe_jsonl_write(save_path, rec)

                    print("\n📩 Muestra recibida (guardada).")
                    print(f"  💾 Guardado en: {save_path} (líneas={total_lines})")

                    acks.append({"ok": True, "type": "sample",
                                 "saved_to": str(save_path), "lines": total_lines})

                else:
                    # payload desconocido → guardamos para diagnóstico en data/YYYY-MM-DD.jsonl
                    save_path = today_file(DATA_DIR)
                    rec = {"received_at": ts, "type": "unknown", "payload": item}
                    total_lines = safe_jsonl_write(save_path, rec)
                    print("\n❓ Payload desconocido.")
                    print(f"  💾 Guardado en: {save_path} (líneas={total_lines})")

                    acks.append({"ok": True, "type": "unknown",
                                 "saved_to": str(save_path), "lines": total_lines})

            # Respuesta: si hubo varios items, devuelve lista; si uno, el objeto
            if len(acks) == 1:
                await websocket.send(json.dumps(acks[0], ensure_ascii=False))
            else:
                await websocket.send(json.dumps(acks, ensure_ascii=False))

    except websockets.ConnectionClosed:
        print(f"❌ Cliente desconectado: {peer}")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(
        handle_connection, "0.0.0.0", port, ping_interval=30, ping_timeout=30
    ):
        print(f"🚀 Servidor WebSocket escuchando en puerto {port}")
        # Nota: en Render, el filesystem es efímero si no configuras Persistent Disk.
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
