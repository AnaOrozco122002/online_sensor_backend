# server.py
import os
import json
import asyncio
import asyncpg
import websockets
from datetime import datetime, timezone
from typing import Dict, Any

# =========================
# Configuración
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")  # Debe estar configurada en Render
PORT = int(os.environ.get("PORT", 8080))

POOL: asyncpg.Pool | None = None

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS windows (
  id             BIGSERIAL PRIMARY KEY,
  received_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  start_time     TIMESTAMPTZ NOT NULL,
  end_time       TIMESTAMPTZ NOT NULL,
  sample_count   INT NOT NULL,
  sample_rate_hz DOUBLE PRECISION NOT NULL,
  activity       TEXT,
  features       JSONB NOT NULL,
  samples_json   JSONB
);
CREATE INDEX IF NOT EXISTS idx_windows_received_at ON windows (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_windows_activity ON windows (activity);
CREATE INDEX IF NOT EXISTS idx_windows_features_gin ON windows USING GIN (features);
"""

WINDOWS_INSERT_SQL = """
INSERT INTO windows (
  received_at, start_time, end_time, sample_count, sample_rate_hz, activity, features, samples_json
) VALUES (
  $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb
)
RETURNING id;
"""

# =========================
# Utilidades
# =========================
def is_window_payload(d: Dict[str, Any]) -> bool:
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def is_sample_payload(d: Dict[str, Any]) -> bool:
    req = {"ax", "ay", "az", "gx", "gy", "gz"}
    return isinstance(d, dict) and req.issubset(d.keys())

def _parse_ts(value):
    """Convierte ISO-8601 (str) o datetime a datetime tz-aware (UTC)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # fallback por microsegundos muy largos
            if '.' in s:
                head, tail = s.split('.', 1)
                frac = ''.join(ch for ch in tail if ch.isdigit())
                frac = (frac + '000000')[:6]
                tz = ''
                # conserva offset si existe
                for i in range(len(tail)-1, -1, -1):
                    if tail[i] in '+-':
                        tz = tail[i:]
                        break
                s2 = f"{head}.{frac}{tz}"
                dt = datetime.fromisoformat(s2)
            else:
                raise
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return value  # deja que asyncpg se queje si es tipo inesperado

# =========================
# DB
# =========================
async def init_db():
    try:
        url = DATABASE_URL
        if not url:
            print("❌ DATABASE_URL no está configurada")
            raise RuntimeError("DATABASE_URL missing")
        print("🔌 Conectando a Postgres...")
        global POOL
        POOL = await asyncpg.create_pool(url, min_size=1, max_size=5)
        async with POOL.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        print("🗄️  DB lista (tabla/índices verificados).")
    except Exception as e:
        print(f"💥 Error init_db: {e}")
        raise

async def save_window(item: Dict[str, Any]) -> int:
    assert POOL is not None
    received_at = datetime.utcnow().replace(tzinfo=timezone.utc)

    start_time   = _parse_ts(item.get("start_time"))
    end_time     = _parse_ts(item.get("end_time"))
    sample_count = item.get("sample_count")
    sample_rate  = item.get("sample_rate_hz")
    activity     = item.get("activity")
    features     = item.get("features") or {}
    samples      = item.get("samples")  # puede ser None o lista

    async with POOL.acquire() as conn:
        win_id = await conn.fetchval(
            WINDOWS_INSERT_SQL,
            received_at,
            start_time,
            end_time,
            int(sample_count) if sample_count is not None else None,
            float(sample_rate) if sample_rate is not None else None,
            activity,
            json.dumps(features, ensure_ascii=False),
            json.dumps(samples, ensure_ascii=False) if samples is not None else None
        )
    return win_id

# =========================
# WS Handler
# =========================
async def handle_connection(websocket):
    peer = websocket.remote_address
    print(f"✅ Cliente conectado: {peer}")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON inválido: {e}")
                await websocket.send(json.dumps({"ok": False, "error": "invalid_json"}))
                continue

            items = data if isinstance(data, list) else [data]
            acks = []

            for item in items:
                if is_window_payload(item):
                    try:
                        win_id = await save_window(item)
                        print("\n📦 Ventana recibida (DB):")
                        print(f"  ⏱  {item.get('start_time')} → {item.get('end_time')}")
                        print(f"  🔢  muestras={item.get('sample_count')} fs={item.get('sample_rate_hz')}")
                        print(f"  🏷  activity={item.get('activity')}")
                        print(f"  🧮  features={len((item.get('features') or {}))}")
                        print(f"  🆔  window_id={win_id}")
                        acks.append({"ok": True, "type": "window", "id": win_id})
                    except Exception as db_e:
                        print(f"💥 Error guardando ventana: {db_e}")
                        acks.append({"ok": False, "type": "window", "error": str(db_e)})

                elif is_sample_payload(item):
                    # Si luego quieres, crea otra tabla para muestras sueltas.
                    print("\n📩 Muestra suelta recibida (no guardada en DB en esta versión).")
                    acks.append({"ok": True, "type": "sample"})

                else:
                    print("\n❓ Payload desconocido (ignorado).")
                    acks.append({"ok": True, "type": "unknown"})

            await websocket.send(json.dumps(acks[0] if len(acks) == 1 else acks, ensure_ascii=False))

    except websockets.ConnectionClosed:
        print(f"❌ Cliente desconectado: {peer}")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

# =========================
# Main
# =========================
async def main():
    await init_db()
    port = PORT
    print(f"🌐 Iniciando WebSocket en 0.0.0.0:{port} ...")
    async with websockets.serve(
        handle_connection, "0.0.0.0", port, ping_interval=30, ping_timeout=30
    ):
        print(f"🚀 WS server escuchando en puerto {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
