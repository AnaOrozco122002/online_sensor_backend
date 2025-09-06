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

# --- CREATE TABLE completa (si no existe) ---
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS windows (
  id               BIGSERIAL PRIMARY KEY,
  received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  start_time       TIMESTAMPTZ NOT NULL,
  end_time         TIMESTAMPTZ NOT NULL,
  sample_count     INT NOT NULL,
  sample_rate_hz   DOUBLE PRECISION NOT NULL,
  activity         TEXT,
  features         JSONB,
  samples_json     JSONB,

  -- 49 columnas requeridas por el modelo
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
"""

# --- ALTER defensivo (por si ya existía la tabla sin todas las columnas) ---
MIGRATE_SQL = """
ALTER TABLE windows
  ADD COLUMN IF NOT EXISTS start_index     BIGINT,
  ADD COLUMN IF NOT EXISTS end_index       BIGINT,
  ADD COLUMN IF NOT EXISTS n_muestras      INT,
  ADD COLUMN IF NOT EXISTS etiqueta        TEXT,

  ADD COLUMN IF NOT EXISTS ax_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ax_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ax_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ax_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ax_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS ay_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ay_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ay_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ay_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ay_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS az_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS az_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS az_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS az_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS az_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS gx_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gx_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gx_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gx_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gx_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS gy_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gy_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gy_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gy_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gy_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS gz_mean         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gz_std          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gz_min          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gz_max          DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS gz_range        DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS pitch_mean      DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS pitch_std       DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS pitch_min       DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS pitch_max       DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS pitch_range     DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS roll_mean       DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS roll_std        DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS roll_min        DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS roll_max        DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS roll_range      DOUBLE PRECISION,

  ADD COLUMN IF NOT EXISTS acc_mag_mean    DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acc_mag_std     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acc_mag_min     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acc_mag_max     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS acc_mag_range   DOUBLE PRECISION;
"""

# --- INSERT que escribe las 49 columnas (además de features/samples_json si quieres) ---
WINDOWS_INSERT_SQL = """
INSERT INTO windows (
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
  $1, $2, $3, $4, $5,
  $6, $7::jsonb, $8::jsonb,
  $9, $10, $11, $12,

  $13, $14, $15, $16, $17,
  $18, $19, $20, $21, $22,
  $23, $24, $25, $26, $27,

  $28, $29, $30, $31, $32,
  $33, $34, $35, $36, $37,
  $38, $39, $40, $41, $42,

  $43, $44, $45, $46, $47,
  $48, $49, $50, $51, $52,

  $53, $54, $55, $56, $57
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
            if '.' in s:
                head, tail = s.split('.', 1)
                frac = ''.join(ch for ch in tail if ch.isdigit())
                frac = (frac + '000000')[:6]
                tz = ''
                for i in range(len(tail)-1, -1, -1):
                    if tail[i] in '+-':
                        tz = tail[i:]
                        break
                s2 = f"{head}.{frac}{tz}"
                dt = datetime.fromisoformat(s2)
            else:
                raise
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return value

def _getf(feats: dict, key: str):
    v = feats.get(key)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

def _geti(feats: dict, key: str):
    v = feats.get(key)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

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
            # Crea tabla completa si no existe
            await conn.execute(CREATE_TABLE_SQL)
            # Asegura columnas si la tabla ya existía de antes
            await conn.execute(MIGRATE_SQL)
        print("🗄️  DB lista (tabla/índices/columnas verificados).")
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
    activity     = item.get("activity")  # texto
    feats        = item.get("features") or {}
    samples      = item.get("samples")

    start_index  = _geti(feats, "start_index")
    end_index    = _geti(feats, "end_index")
    n_muestras   = _geti(feats, "n_muestras")
    etiqueta     = activity if activity is not None else feats.get("etiqueta")

    args = [
        received_at, start_time, end_time, int(sample_count), float(sample_rate),
        activity, json.dumps(feats, ensure_ascii=False),
        json.dumps(samples, ensure_ascii=False) if samples is not None else None,

        start_index, end_index, n_muestras, etiqueta,

        _getf(feats,"ax_mean"), _getf(feats,"ax_std"), _getf(feats,"ax_min"), _getf(feats,"ax_max"), _getf(feats,"ax_range"),
        _getf(feats,"ay_mean"), _getf(feats,"ay_std"), _getf(feats,"ay_min"), _getf(feats,"ay_max"), _getf(feats,"ay_range"),
        _getf(feats,"az_mean"), _getf(feats,"az_std"), _getf(feats,"az_min"), _getf(feats,"az_max"), _getf(feats,"az_range"),

        _getf(feats,"gx_mean"), _getf(feats,"gx_std"), _getf(feats,"gx_min"), _getf(feats,"gx_max"), _getf(feats,"gx_range"),
        _getf(feats,"gy_mean"), _getf(feats,"gy_std"), _getf(feats,"gy_min"), _getf(feats,"gy_max"), _getf(feats,"gy_range"),
        _getf(feats,"gz_mean"), _getf(feats,"gz_std"), _getf(feats,"gz_min"), _getf(feats,"gz_max"), _getf(feats,"gz_range"),

        _getf(feats,"pitch_mean"), _getf(feats,"pitch_std"), _getf(feats,"pitch_min"), _getf(feats,"pitch_max"), _getf(feats,"pitch_range"),
        _getf(feats,"roll_mean"),  _getf(feats,"roll_std"),  _getf(feats,"roll_min"),  _getf(feats,"roll_max"),  _getf(feats,"roll_range"),

        _getf(feats,"acc_mag_mean"), _getf(feats,"acc_mag_std"), _getf(feats,"acc_mag_min"), _getf(feats,"acc_mag_max"), _getf(feats,"acc_mag_range"),
    ]

    async with POOL.acquire() as conn:
        win_id = await conn.fetchval(WINDOWS_INSERT_SQL, *args)
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
