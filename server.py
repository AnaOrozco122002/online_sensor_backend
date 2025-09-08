# server.py
import os
import re
import json
import math
import asyncio
import asyncpg
import websockets
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))
POOL: asyncpg.Pool | None = None
PH = PasswordHasher()

# ---------- SQL ----------
CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id_usuario    BIGSERIAL PRIMARY KEY,
  email         TEXT UNIQUE,
  display_name  TEXT,
  birthday      DATE,
  password_hash TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ
);
"""

CREATE_INTERVALS_SQL = """
CREATE TABLE IF NOT EXISTS intervalos_label (
  id          BIGSERIAL PRIMARY KEY,
  session_id  BIGINT GENERATED ALWAYS AS (id) STORED UNIQUE,
  user_id     BIGINT,
  label       TEXT NOT NULL,
  reason      TEXT,
  start_ts    TIMESTAMPTZ NOT NULL,
  end_ts      TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CREATE_WINDOWS_SQL = """
CREATE TABLE IF NOT EXISTS windows (
  id               BIGSERIAL PRIMARY KEY,
  id_usuario       BIGINT REFERENCES users(id_usuario),
  session_id       BIGINT REFERENCES intervalos_label(id),
  received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  start_time       TIMESTAMPTZ NOT NULL,
  end_time         TIMESTAMPTZ NOT NULL,
  sample_count     INT NOT NULL,
  sample_rate_hz   DOUBLE PRECISION NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_windows_start_index ON windows (start_index);
CREATE INDEX IF NOT EXISTS idx_windows_end_index   ON windows (end_index);
CREATE INDEX IF NOT EXISTS idx_windows_user        ON windows (id_usuario);
CREATE INDEX IF NOT EXISTS idx_windows_session     ON windows (session_id);
"""

# MIGRACIÓN DEFENSIVA
MIGRATE_SQL = """
-- USERS: asegurar columnas básicas
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email         TEXT,
  ADD COLUMN IF NOT EXISTS display_name  TEXT,
  ADD COLUMN IF NOT EXISTS birthday      DATE,
  ADD COLUMN IF NOT EXISTS password_hash TEXT;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='users' AND column_name='email'
  ) THEN
    CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
  END IF;
END $$;

-- INTERVALOS_LABEL: asegurar columnas e índice
ALTER TABLE intervalos_label
  ADD COLUMN IF NOT EXISTS user_id BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name='intervalos_label' AND column_name='session_id'
  ) THEN
    ALTER TABLE intervalos_label
      ADD COLUMN session_id BIGINT GENERATED ALWAYS AS (id) STORED UNIQUE;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_intervals_session_id
  ON intervalos_label(session_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'intervalos_label'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND tc.constraint_name = 'intervalos_label_user_id_fkey'
  ) THEN
    ALTER TABLE intervalos_label
      ADD CONSTRAINT intervalos_label_user_id_fkey
      FOREIGN KEY (user_id) REFERENCES users(id_usuario);
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='intervalos_label' AND column_name='user_id'
  ) THEN
    CREATE INDEX IF NOT EXISTS idx_intervals_user ON intervalos_label (user_id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_intervals_created ON intervalos_label (created_at DESC);

-- WINDOWS: asegurar session_id BIGINT y eliminar activity si existe
DO $$
DECLARE
  _data_type TEXT;
BEGIN
  SELECT data_type INTO _data_type
  FROM information_schema.columns
  WHERE table_name='windows' AND column_name='session_id';

  IF _data_type IS NULL THEN
    ALTER TABLE windows ADD COLUMN session_id BIGINT;
  ELSIF _data_type <> 'bigint' THEN
    ALTER TABLE windows ADD COLUMN IF NOT EXISTS session_id_new BIGINT;

    UPDATE windows
    SET session_id_new = NULLIF(session_id::text, '')::bigint
    WHERE session_id IS NOT NULL
      AND session_id::text ~ '^[0-9]+$'
      AND session_id_new IS NULL;

    UPDATE windows w
    SET session_id_new = i.id
    FROM intervalos_label i
    WHERE w.session_id_new IS NULL
      AND w.session_id IS NOT NULL
      AND w.session_id::text = i.session_id;

    ALTER TABLE windows DROP COLUMN session_id;
    ALTER TABLE windows RENAME COLUMN session_id_new TO session_id;
  END IF;

  -- eliminar activity si existe
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='windows' AND column_name='activity'
  ) THEN
    ALTER TABLE windows DROP COLUMN activity;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = 'windows'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND tc.constraint_name = 'windows_session_id_fkey'
  ) THEN
    ALTER TABLE windows
      ADD CONSTRAINT windows_session_id_fkey
      FOREIGN KEY (session_id) REFERENCES intervalos_label(id);
  END IF;
END $$;
"""

GET_USER_BY_EMAIL_SQL = "SELECT id_usuario, email, password_hash, display_name FROM users WHERE email = $1"
GET_USER_BY_ID_SQL    = "SELECT id_usuario, email, password_hash FROM users WHERE id_usuario = $1"
INSERT_USER_RETURNING_SQL = """
INSERT INTO users (email, display_name, birthday, password_hash, last_seen_at)
VALUES ($1, $2, $3, $4, NOW())
RETURNING id_usuario, email, display_name, birthday
"""
SET_PASSWORD_SQL = "UPDATE users SET password_hash = $2, last_seen_at = NOW() WHERE id_usuario = $1"

INSERT_INTERVAL_SQL = """
INSERT INTO intervalos_label (user_id, label, reason, start_ts)
VALUES ($1, $2, $3, $4)
RETURNING id, session_id, label, reason, start_ts
"""
END_INTERVAL_SQL = "UPDATE intervalos_label SET end_ts = $2 WHERE id = $1 RETURNING id, end_ts"

WINDOWS_INSERT_SQL = """
INSERT INTO windows (
  id_usuario, session_id,
  received_at, start_time, end_time, sample_count, sample_rate_hz,
  features, samples_json,
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
  $1, $2,
  $3, $4, $5, $6, $7,
  $8::jsonb, $9::jsonb,
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

# ---------- Utils ----------
_email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def is_window_payload(d: Dict[str, Any]) -> bool:
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def _parse_ts(value):
    """
    Convierte cadenas ISO8601 a UTC.
    - Si la cadena tiene zona horaria (Z, +hh:mm), la respeta y convierte a UTC.
    - Si la cadena NO tiene zona horaria, se asume HORA COLOMBIA (UTC-5) y se convierte a UTC.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            dt_utc = value + timedelta(hours=5)
            return dt_utc.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        s = value.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        dt = None
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
                try:
                    dt = datetime.fromisoformat(s2)
                except Exception:
                    dt = None
        if dt is None:
            return None

        if dt.tzinfo is None:
            dt_utc = dt + timedelta(hours=5)
            return dt_utc.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    return value

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s: return None
    try:
        return date.fromisoformat(s.strip())
    except Exception:
        return None

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

# ---------- Auth helpers ----------
async def register_user(conn: asyncpg.Connection, email: str, display_name: Optional[str], birthday_str: Optional[str], password: str):
    print(f"📝 register_user: email={email}, name={display_name}, birthday={birthday_str}")
    if not _email_re.match(email or ""):
        return {"ok": False, "code": "bad_email", "message": "Correo inválido"}
    if not password or len(password) < 6:
        return {"ok": False, "code": "weak_password", "message": "Contraseña muy corta (mínimo 6)"}
    bday = _parse_date(birthday_str)
    row = await conn.fetchrow(GET_USER_BY_EMAIL_SQL, email)
    if row and row["password_hash"]:
        return {"ok": False, "code": "email_exists", "message": "El correo ya está registrado"}

    pwd_hash = PH.hash(password)
    row = await conn.fetchrow(INSERT_USER_RETURNING_SQL, email, display_name, bday, pwd_hash)
    print(f"✅ register_user OK id={row['id_usuario']}")
    return {
        "ok": True,
        "id_usuario": row["id_usuario"],
        "email": row["email"],
        "display_name": row["display_name"],
        "birthday": birthday_str
    }

async def login_user(conn: asyncpg.Connection, email: str, password: str):
    print(f"🔐 login_user: email={email}")
    if not _email_re.match(email or ""):
        return {"ok": False, "code": "bad_email", "message": "Correo inválido"}
    row = await conn.fetchrow(GET_USER_BY_EMAIL_SQL, email)
    if not row or not row["password_hash"]:
        return {"ok": False, "code": "not_found", "message": "Usuario no existe"}
    try:
        PH.verify(row["password_hash"], password)
    except VerifyMismatchError:
        return {"ok": False, "code": "invalid_credentials", "message": "Credenciales inválidas"}
    await conn.execute("UPDATE users SET last_seen_at = NOW() WHERE id_usuario = $1", row["id_usuario"])
    print(f"✅ login_user OK id={row['id_usuario']}")
    return {
        "ok": True,
        "id_usuario": row["id_usuario"],
        "email": email,
        "display_name": row["display_name"]
    }

async def change_password(conn: asyncpg.Connection, user_id: int, old_pwd: str, new_pwd: str):
    print(f"🛠 change_password: id={user_id}")
    if not new_pwd or len(new_pwd) < 6:
        return {"ok": False, "code": "weak_password", "message": "Contraseña muy corta (mínimo 6)"}
    row = await conn.fetchrow(GET_USER_BY_ID_SQL, user_id)
    if not row or not row["password_hash"]:
        return {"ok": False, "code": "not_found", "message": "Usuario no existe"}
    try:
        PH.verify(row["password_hash"], old_pwd)
    except VerifyMismatchError:
        return {"ok": False, "code": "invalid_credentials", "message": "Contraseña actual incorrecta"}
    pwd_hash = PH.hash(new_pwd)
    await conn.execute(SET_PASSWORD_SQL, user_id, pwd_hash)
    print("✅ change_password OK")
    return {"ok": True}

# ---------- Sessions RPC ----------
async def start_session(conn: asyncpg.Connection, user_id: int, label: str, reason: Optional[str]):
    if not label or not label.strip():
        return {"ok": False, "code": "bad_label", "message": "Label requerido"}
    # reason puede venir fijo como "initial" desde la app
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    row = await conn.fetchrow(INSERT_INTERVAL_SQL, user_id, label.strip(), (reason or "").strip() or None, now)
    return {
        "ok": True,
        "interval_id": row["id"],
        "session_id": row["session_id"],
        "label": row["label"],
        "start_ts": row["start_ts"].isoformat()
    }

async def stop_session(conn: asyncpg.Connection, interval_id: int):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    row = await conn.fetchrow(END_INTERVAL_SQL, interval_id, now)
    if not row:
        return {"ok": False, "code": "not_found", "message": "Sesión no encontrada"}
    return {"ok": True, "interval_id": row["id"], "end_ts": now.isoformat()}

# ---------- DB init ----------
async def init_db():
    if not DATABASE_URL:
        print("❌ DATABASE_URL no está configurada"); raise RuntimeError("DATABASE_URL missing")
    print("🔌 Conectando a Postgres...")
    global POOL
    POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with POOL.acquire() as conn:
        await conn.execute(CREATE_USERS_SQL)
        await conn.execute(CREATE_INTERVALS_SQL)
        await conn.execute(CREATE_WINDOWS_SQL)
        await conn.execute(MIGRATE_SQL)
    print("🗄️  DB lista (tablas/índices/columnas verificados).")

# ---------- Save window ----------
async def save_window(item: Dict[str, Any]) -> int:
    assert POOL is not None
    received_at = datetime.utcnow().replace(tzinfo=timezone.utc)

    def _as_int(v):
        try: return int(v)
        except Exception: return None

    user_id     = _as_int(item.get("id_usuario"))
    interval_id = _as_int(item.get("session_id"))

    start_time   = _parse_ts(item.get("start_time"))
    end_time     = _parse_ts(item.get("end_time"))
    sample_count = _normi(item.get("sample_count"))
    sample_rate  = _normf(item.get("sample_rate_hz"))
    feats        = item.get("features") or {}
    samples      = item.get("samples")

    start_index  = _normi(feats.get("start_index"))
    end_index    = _normi(feats.get("end_index"))
    n_muestras   = _normi(feats.get("n_muestras"))

    # Etiqueta ahora puede venir en features o quedar NULL (el modelo la actualizará)
    etiqueta     = feats.get("etiqueta")
    if isinstance(etiqueta, str):
        etiqueta = etiqueta.strip() or None
    else:
        etiqueta = None

    def f(k): return _normf(feats.get(k))

    args = [
        user_id, interval_id,
        received_at, start_time, end_time, sample_count, sample_rate,
        json.dumps(feats, ensure_ascii=False),
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
        win_id = await conn.fetchval(WINDOWS_INSERT_SQL, *args)
    return win_id

# ---------- WS ----------
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

            if isinstance(data, dict) and "type" in data and isinstance(data["type"], str):
                typ = data["type"]
                try:
                    async with POOL.acquire() as conn:
                        if typ == "register":
                            _email = (data.get("email") or "").strip()
                            _name  = (data.get("display_name") or "").strip() or None
                            _bday  = (data.get("birthday") or "").strip() or None
                            _pwd   = data.get("password") or ""
                            resp = await register_user(conn, _email, _name, _bday, _pwd)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "login":
                            _email = (data.get("email") or "").strip()
                            _pwd   = data.get("password") or ""
                            resp = await login_user(conn, _email, _pwd)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "change_password":
                            try: uid = int(data.get("id_usuario"))
                            except Exception:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_id", "message": "id_usuario inválido"})); continue
                            resp = await change_password(conn, uid, data.get("old_password") or "", data.get("new_password") or "")
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "start_session":
                            try: uid = int(data.get("id_usuario"))
                            except Exception:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_id", "message": "id_usuario inválido"})); continue
                            label  = (data.get("label") or "").strip()
                            reason = (data.get("reason") or "initial").strip()
                            resp = await start_session(conn, uid, label, reason)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "stop_session":
                            try: iid = int(data.get("interval_id"))
                            except Exception:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_interval_id", "message": "interval_id inválido"})); continue
                            resp = await stop_session(conn, iid)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "ping":
                            await websocket.send(json.dumps({"ok": True, "type": "pong"})); continue

                        else:
                            await websocket.send(json.dumps({"ok": False, "code": "unknown_type", "message": "Tipo de RPC desconocido"})); continue
                except Exception as e:
                    print(f"💥 Error en RPC {typ}: {e}")
                    try: await websocket.send(json.dumps({"ok": False, "code": "server_error", "message": str(e)}))
                    except Exception: pass
                    continue

            # Ventanas
            items = data if isinstance(data, list) else [data]
            acks = []
            for item in items:
                if is_window_payload(item):
                    try:
                        win_id = await save_window(item)
                        print("\n📦 Ventana recibida (DB):")
                        print(f"  👤 id_usuario={item.get('id_usuario')}  🧩 session_id={item.get('session_id')}")
                        print(f"  ⏱  {item.get('start_time')} → {item.get('end_time')}")
                        print(f"  🔢  muestras={item.get('sample_count')} fs={item.get('sample_rate_hz')}")
                        print(f"  🧮  features={len((item.get('features') or {}))}")
                        print(f"  🆔  window_id={win_id}")
                        acks.append({"ok": True, "type": "window", "id": win_id})
                    except Exception as db_e:
                        print(f"💥 Error guardando ventana: {db_e}")
                        acks.append({"ok": False, "type": "window", "error": str(db_e)})
                else:
                    acks.append({"ok": True, "type": "unknown"})

            try:
                await websocket.send(json.dumps(acks[0] if len(acks) == 1 else acks, ensure_ascii=False))
            except Exception as e:
                print(f"⚠️ Error enviando ACK: {e}")

    except websockets.ConnectionClosed:
        print(f"❌ Cliente desconectado: {peer}")
    except Exception as e:
        print(f"⚠️ Error en la conexión: {e}")

# ---------- Main ----------
async def main():
    await init_db()
    print(f"🌐 Iniciando WebSocket en 0.0.0.0:{PORT} ...")
    async with websockets.serve(handle_connection, "0.0.0.0", PORT, ping_interval=30, ping_timeout=30):
        print(f"🚀 WS server escuchando en puerto {PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
