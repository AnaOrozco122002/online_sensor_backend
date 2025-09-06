# server.py
import os
import json
import math
import asyncio
import asyncpg
import websockets
from datetime import datetime, timezone
from typing import Dict, Any

DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))
POOL: asyncpg.Pool | None = None

CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id_usuario   TEXT PRIMARY KEY,
  display_name TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ
);
"""

CREATE_WINDOWS_SQL = """
CREATE TABLE IF NOT EXISTS windows (
  id               BIGSERIAL PRIMARY KEY,
  id_usuario       TEXT REFERENCES users(id_usuario),
  received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  start_time       TIMESTAMPTZ NOT NULL,
  end_time         TIMESTAMPTZ NOT NULL,
  sample_count     INT NOT NULL,
  sample_rate_hz   DOUBLE PRECISION NOT NULL,
  activity         TEXT,
  features         JSONB,
  samples_json     JSONB,

  start_index      BIGINT,
  end_index        BIGINT,
  n_muestras       INT,
  etiqueta         TEXT,

  ax_mean          DOUBLE PRECISION, ax_std DOUBLE PRECISION, ax_min DOUBLE PRECISION, ax_max DOUBLE PRECISION, ax_range DOUBLE PRECISION,
  ay_mean          DOUBLE PRECISION, ay_std DOUBLE PRECISION, ay_min DOUBLE PRECISION, ay_max DOUBLE PRECISION, ay_range DOUBLE PRECISION,
  az_mean          DOUBLE PRECISION, az_std DOUBLE PRECISION, az_min DOUBLE PRECISION, az_max DOUBLE PRECISION, az_range DOUBLE PRECISION,

  gx_mean          DOUBLE PRECISION, gx_std DOUBLE PRECISION, gx_min DOUBLE PRECISION, gx_max DOUBLE PRECISION, gx_range DOUBLE PRECISION,
  gy_mean          DOUBLE PRECISION, gy_std DOUBLE PRECISION, gy_min DOUBLE PRECISION, gy_max DOUBLE PRECISION, gy_range DOUBLE PRECISION,
  gz_mean          DOUBLE PRECISION, gz_std DOUBLE PRECISION, gz_min DOUBLE PRECISION, gz_max DOUBLE PRECISION, gz_range DOUBLE PRECISION,

  pitch_mean       DOUBLE PRECISION, pitch_std DOUBLE PRECISION, pitch_min DOUBLE PRECISION, pitch_max DOUBLE PRECISION, pitch_range DOUBLE PRECISION,
  roll_mean        DOUBLE PRECISION, roll_std DOUBLE PRECISION, roll_min DOUBLE PRECISION, roll_max DOUBLE PRECISION, roll_range DOUBLE PRECISION,

  acc_mag_mean     DOUBLE PRECISION, acc_mag_std DOUBLE PRECISION, acc_mag_min DOUBLE PRECISION, acc_mag_max DOUBLE PRECISION, acc_mag_range DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_windows_received_at ON windows (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_windows_etiqueta    ON windows (etiqueta);
CREATE INDEX IF NOT EXISTS idx_windows_start_index ON windows (start_index);
CREATE INDEX IF NOT EXISTS idx_windows_end_index   ON windows (end_index);
CREATE INDEX IF NOT EXISTS idx_windows_id_usuario  ON windows (id_usuario);
"""

MIGRATE_SQL = """
ALTER TABLE windows
  ADD COLUMN IF NOT EXISTS id_usuario TEXT REFERENCES users(id_usuario);
"""

UPSERT_USER_SQL = """
INSERT INTO users (id_usuario, display_name, last_seen_at)
VALUES ($1, $2, NOW())
ON CONFLICT (id_usuario) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
RETURNING id_usuario;
"""

WINDOWS_INSERT_SQL = """
INSERT INTO windows (
  id_usuario,
  received_at, start_time, end_time, sample_count, sample_rate_hz,
  activity, features, samples_json,
  start_index, end_index, n_muestras, etiqueta,

  ax_mean, ax_std, ax_min, ax_max, ax_range,
  ay_mean, ay_std, ay_min, ay_max, ay_range,
  az_mean, az_std, az_min, az_max, az_range,

  gx_mean, gx_std, gx_min, gx_max, gx_range,
  gy_mean, gy_std, gy_min, gy_max, gy_range,
  gz_mean, gz_std, gz_min, gz_max, gz_range,

  pitch_mean, pitch_std, pitch_min, pitch_max, pitch_range,
  roll_mean,  roll_std,  roll_min,  roll_max,  roll_range,

  acc_mag_mean, acc_mag_std, acc_mag_min, acc_mag_max, acc_mag_range
) VALUES (
  $1,
  $2, $3, $4, $5, $6,
  $7, $8::jsonb, $9::jsonb,
  $10, $11, $12, $13,

  $14, $15, $16, $17, $18,
  $19, $20, $21, $22, $23,
  $24, $25, $26, $27, $28,

  $29, $30, $31, $32, $33,
  $34, $35, $36, $37, $38,
  $39, $40, $41, $42, $43,

  $44, $45, $46, $47, $48,
  $49, $50, $51, $52, $53,

  $54, $55, $56, $57, $58
)
RETURNING id;
"""

def is_window_payload(d: Dict[str, Any]) -> bool:
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def _parse_ts(value):
    if value is None: return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            if '.' in s:
                head, tail = s.split('.', 1)
                frac = ''.join(ch for ch in tail if ch.isdigit())
                frac = (frac + '000000')[:6]
                tz = ''
                for i in range(len(tail)-1, -1, -1):
                    if tail[i] in '+-': tz = tail[i:]; break
                s2 = f"{head}.{frac}{tz}"
                dt = datetime.fromisoformat(s2)
            else:
                raise
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return value

def _normf(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def _normi(v):
    try:
        return int(v)
    except Exception:
        return None

async def init_db():
    try:
        if not DATABASE_URL:
            print("❌ DATABASE_URL no está configurada")
            raise RuntimeError("DATABASE_URL missing")
        print("🔌 Conectando a Postgres...")
        global POOL
        POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with POOL.acquire() as conn:
            await conn.execute(CREATE_USERS_SQL)
            await conn.execute(CREATE_WINDOWS_SQL)
            await conn.execute(MIGRATE_SQL)
        print("🗄️  DB lista (tablas/índices/columnas verificados).")
    except Exception as e:
        print(f"💥 Error init_db: {e}")
        raise

async def save_window(item: Dict[str, Any]) -> int:
    assert POOL is not None
    received_at = datetime.utcnow().replace(tzinfo=timezone.utc)

    user_id = item.get("id_usuario") or "anon"
    user_name = None  # si algún día recibes display_name, asigna aquí

    start_time   = _parse_ts(item.get("start_time"))
    end_time     = _parse_ts(item.get("end_time"))
    sample_count = _normi(item.get("sample_count"))
    sample_rate  = _normf(item.get("sample_rate_hz"))
    activity     = item.get("activity")
    feats        = item.get("features") or {}
    samples      = item.get("samples")

    start_index  = _normi(feats.get("start_index"))
    end_index    = _normi(feats.get("end_index"))
    n_muestras   = _normi(feats.get("n_muestras"))
    etiqueta     = activity if activity is not None else feats.get("etiqueta")

    # Normaliza todos los floats (NaN/Inf -> NULL)
    def f(k): return _normf(feats.get(k))

    args = [
        user_id,
        received_at, start_time, end_time, sample_count, sample_rate,
        activity, json.dumps(feats, ensure_ascii=False),
        json.dumps(samples, ensure_ascii=False) if samples is not None else None,
        start_index, end_index, n_muestras, etiqueta,

        f("ax_mean"), f("ax_std"), f("ax_min"), f("ax_max"), f("ax_range"),
        f("ay_mean"), f("ay_std"), f("ay_min"), f("ay_max"), f("ay_range"),
        f("az_mean"), f("az_std"), f("az_min"), f("az_max"), f("az_range"),

        f("gx_mean"), f("gx_std"), f("gx_min"), f("gx_max"), f("gx_range"),
        f("gy_mean"), f("gy_std"), f("gy_min"), f("gy_max"), f("gy_range"),
        f("gz_mean"), f("gz_std"), f("gz_min"), f("gz_max"), f("gz_range"),

        f("pitch_mean"), f("pitch_std"), f("pitch_min"), f("pitch_max"), f("pitch_range"),
        f("roll_mean"),  f("roll_std"),  f("roll_min"),  f("roll_max"),  f("roll_range"),

        f("acc_mag_mean"), f("acc_mag_std"), f("acc_mag_min"), f("acc_mag_max"), f("acc_mag_range"),
    ]

    async with POOL.acquire() as conn:
        # UPSERT del usuario
        await conn.fetchval(UPSERT_USER_SQL, user_id, user_name)
        # Insert de la ventana
        win_id = await conn.fetchval(WINDOWS_INSERT_SQL, *args)
    return win_id

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
                        print(f"  👤 id_usuario={item.get('id_usuario')}")
                        print(f"  ⏱  {item.get('start_time')} → {item.get('end_time')}")
                        print(f"  🔢  muestras={item.get('sample_count')} fs={item.get('sample_rate_hz')}")
                        print(f"  🏷  activity={item.get('activity')}")
                        print(f"  🧮  features={len((item.get('features') or {}))}")
                        print(f"  🆔  window_id={win_id}")
                        acks.append({"ok": True, "type": "window", "id": win_id})
                    except Exception as db_e:
                        print(f"💥 Error guardando ventana: {db_e}")
                        acks.append({"ok": False, "type": "window", "error": str(db_e)})
                else:
                    acks.append({"ok": True, "type": "unknown"})

            await websocket.send(json.dumps(acks[0] if len(acks) == 1 else acks, ensure_ascii=False))
    except websockets.ConnectionClosed:
        print(f"❌ Cliente desconectado: {peer}")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

async def main():
    await init_db()
    print(f"🌐 Iniciando WebSocket en 0.0.0.0:{PORT} ...")
    async with websockets.serve(handle_connection, "0.0.0.0", PORT, ping_interval=30, ping_timeout=30):
        print(f"🚀 WS server escuchando en puerto {PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
