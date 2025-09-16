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

# --- stdlib HTTP (sin dependencias) ---
import urllib.request
import urllib.error

DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))
POOL: asyncpg.Pool | None = None
PH = PasswordHasher()

# ---------- Config modelo ----------
PREDICT_BASE = "https://online-server-xgji.onrender.com"
PREDICT_URL = f"{PREDICT_BASE}/predict_by_window"
POLICY_LABELED_URL = f"{PREDICT_BASE}/policy/labeled"
HTTP_TIMEOUT_SECS = 6  # timeout corto para no bloquear

# ---------- Cache en memoria de etiqueta vigente por sesión ----------
# Clave: session_id (grupo)  Valor: str (label vigente)
SESSION_ACTIVITY: Dict[int, str] = {}

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

# -- ATENCIÓN: session_id YA NO ES UNIQUE
CREATE_INTERVALS_SQL = """
CREATE TABLE IF NOT EXISTS intervalos_label (
  id          BIGSERIAL PRIMARY KEY,
  session_id  BIGINT NOT NULL,
  id_usuario  BIGINT REFERENCES users(id_usuario),
  label       TEXT NOT NULL,
  reason      TEXT,
  duracion    INTEGER,
  start_ts    TIMESTAMPTZ NOT NULL,
  end_ts      TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_intervals_created ON intervalos_label (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intervals_user    ON intervalos_label (id_usuario);
CREATE INDEX IF NOT EXISTS idx_intervals_session ON intervalos_label (session_id);
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

  pred_label       TEXT,
  confianza        DOUBLE PRECISION,
  precision        DOUBLE PRECISION,
  actividad        TEXT,

  ax_mean          DOUBLE PRECISION, ax_std DOUBLE PRECISION, ax_min DOUBLE PRECISION, ax_max DOUBLE PRECISION, ax_range DOUBLE PRECISION,
  ay_mean          DOUBLE PRECISION, ay_std DOUBLE PRECISION, ay_min DOUBLE PRECISION, ay_max DOUBLE PRECISION, ay_range DOUBLE PRECISION,
  az_mean          DOUBLE PRECISION, az_std DOUBLE PRECISION, az_min DOUBLE PRECISION, az_max DOUBLE PRECISION, az_range DOUBLE PRECISION,

  gx_mean          DOUBLE PRECISION, gx_std DOUBLE PRECISION, gx_min DOUBLE PRECISION, gx_max DOUBLE PRECISION, gx_range DOUBLE PRECISION,
  gy_mean          DOUBLE PRECISION, gy_std DOUBLE PRECISION, gy_min DOUBLE PRECISION, gy_max DOUBLE PRECISION, gy_range DOUBLE PRECISION,
  gz_mean          DOUBLE PRECISION, gz_std DOUBLE PRECISION, gz_min DOUBLE PRECISION, gz_max DOUBLE PRECISION, gz_range DOUBLE PRECISION,

  pitch_mean       DOUBLE PRECISION, pitch_std DOUBLE PRECISION, pitch_min DOUBLE PRECISION, pitch_max DOUBLE PRECISION, pitch_range DOUBLE PRECISION,
  roll_mean        DOUBLE PRECISION, roll_std DOUBLE PRECISION, roll_min DOUBLE PRECISION, roll_max DOUBLE PRECISION, roll_range DOUBLE PRECISION,

  acc_mag_mean     DOUBLE PRECISION, acc_mag_std DOUBLE PRECISION, acc_mag_min DOUBLE PRECISION, acc_mag_max DOUBLE PRECISION, acc_mag_range DOUBLE PRECISION,

  sl_trained       BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_windows_received_at ON windows (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_windows_etiqueta    ON windows (etiqueta);
CREATE INDEX IF NOT EXISTS idx_windows_start_index ON windows (start_index);
CREATE INDEX IF NOT EXISTS idx_windows_end_index   ON windows (end_index);
CREATE INDEX IF NOT EXISTS idx_windows_user        ON windows (id_usuario);
CREATE INDEX IF NOT EXISTS idx_windows_session     ON windows (session_id);
"""

# MIGRACIÓN DEFENSIVA: elimina UNIQUE en session_id y asegura índices/FKs y nueva columna
MIGRATE_SQL = """
-- USERS
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

-- INTERVALOS_LABEL: renombrar user_id -> id_usuario si existiera
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='intervalos_label' AND column_name='user_id'
  ) THEN
    ALTER TABLE intervalos_label
      RENAME COLUMN user_id TO id_usuario;
  END IF;
END $$;

-- Asegurar columnas
ALTER TABLE intervalos_label
  ADD COLUMN IF NOT EXISTS id_usuario BIGINT,
  ADD COLUMN IF NOT EXISTS reason     TEXT,
  ADD COLUMN IF NOT EXISTS duracion   INTEGER;

-- Asegurar session_id BIGINT (sin UNIQUE)
DO $$
DECLARE
  _data_type TEXT;
BEGIN
  SELECT data_type INTO _data_type
  FROM information_schema.columns
  WHERE table_name='intervalos_label' AND column_name='session_id';

  IF _data_type IS NULL THEN
    ALTER TABLE intervalos_label ADD COLUMN session_id BIGINT;
  ELSIF _data_type <> 'bigint' THEN
    ALTER TABLE intervalos_label ADD COLUMN IF NOT EXISTS session_id_new BIGINT;

    UPDATE intervalos_label
    SET session_id_new = NULLIF(session_id::text, '')::bigint
    WHERE session_id_new IS NULL
      AND session_id IS NOT NULL
      AND session_id::text ~ '^[0-9]+$';

    ALTER TABLE intervalos_label DROP COLUMN session_id;
    ALTER TABLE intervalos_label RENAME COLUMN session_id_new TO session_id;
  END IF;
END $$;

-- Quitar UNIQUE si existía
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'intervalos_label_session_id_key'
  ) THEN
    ALTER TABLE intervalos_label DROP CONSTRAINT intervalos_label_session_id_key;
  END IF;
EXCEPTION WHEN undefined_table THEN NULL;
END $$;

DROP INDEX IF EXISTS intervalos_label_session_id_key;
DROP INDEX IF EXISTS uq_intervalos_label_session_id;

-- Asegurar NOT NULL y un índice normal por session_id
ALTER TABLE intervalos_label
  ALTER COLUMN session_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_intervals_session ON intervalos_label (session_id);

-- FK a users
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_name = 'intervalos_label'
      AND constraint_type = 'FOREIGN KEY'
      AND constraint_name = 'intervalos_label_id_usuario_fkey'
  ) THEN
    ALTER TABLE intervalos_label
      ADD CONSTRAINT intervalos_label_id_usuario_fkey
      FOREIGN KEY (id_usuario) REFERENCES users(id_usuario);
  END IF;
END $$;

-- WINDOWS: asegurar session_id BIGINT y FK a intervalos_label(id)
DO $$
DECLARE
  _data_type2 TEXT;
BEGIN
  SELECT data_type INTO _data_type2
  FROM information_schema.columns
  WHERE table_name='windows' AND column_name='session_id';

  IF _data_type2 IS NULL THEN
    ALTER TABLE windows ADD COLUMN session_id BIGINT;
  ELSIF _data_type2 <> 'bigint' THEN
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
      AND w.session_id::text = i.session_id::text;

    ALTER TABLE windows DROP COLUMN session_id;
    ALTER TABLE windows RENAME COLUMN session_id_new TO session_id;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_name = 'windows'
      AND constraint_type = 'FOREIGN KEY'
      AND constraint_name = 'windows_session_id_fkey'
  ) THEN
    ALTER TABLE windows DROP CONSTRAINT windows_session_id_fkey;
  END IF;
END $$;

ALTER TABLE windows
  ADD CONSTRAINT windows_session_id_fkey
  FOREIGN KEY (session_id) REFERENCES intervalos_label(id)
  ON DELETE SET NULL;

-- WINDOWS: columnas del modelo (por si faltaran)
ALTER TABLE windows
  ADD COLUMN IF NOT EXISTS pred_label  TEXT,
  ADD COLUMN IF NOT EXISTS confianza   DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS precision   DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS actividad   TEXT,
  ADD COLUMN IF NOT EXISTS sl_trained  BOOLEAN NOT NULL DEFAULT FALSE;
"""

GET_USER_BY_EMAIL_SQL = "SELECT id_usuario, email, password_hash, display_name FROM users WHERE email = $1"
GET_USER_BY_ID_SQL    = "SELECT id_usuario, email, password_hash FROM users WHERE id_usuario = $1"
INSERT_USER_RETURNING_SQL = """
INSERT INTO users (email, display_name, birthday, password_hash, last_seen_at)
VALUES ($1, $2, $3, $4, NOW())
RETURNING id_usuario, email, display_name, birthday
"""
SET_PASSWORD_SQL = "UPDATE users SET password_hash = $2, last_seen_at = NOW() WHERE id_usuario = $1"

# Inserción de intervalo con id preasignado; por defecto usamos id como session_id NUEVO
GET_NEXT_INTERVAL_ID_SQL = "SELECT nextval(pg_get_serial_sequence('intervalos_label','id')) AS new_id"

INSERT_INTERVAL_WITH_ID_SQL = """
INSERT INTO intervalos_label (id, session_id, id_usuario, label, reason, start_ts)
VALUES ($1, $1, $2, $3, $4, $5)
RETURNING id, session_id, label, reason, start_ts
"""

# --- Consultas auxiliares para "última fila por session_id" ---
GET_SESSION_ID_BY_INTERVAL_ID_SQL = """
SELECT session_id FROM intervalos_label WHERE id = $1
"""

GET_LATEST_INTERVAL_FOR_SESSION_SQL = """
SELECT id, session_id, reason, label, duracion
FROM intervalos_label
WHERE session_id = $1
ORDER BY id DESC
LIMIT 1
"""

# stop: solo cambia reason a 'finish' en la última fila por session_id (NO toca end_ts)
STOP_BY_SESSION_SQL = """
UPDATE intervalos_label
SET reason = 'finish'
WHERE id = (
  SELECT id FROM intervalos_label
  WHERE session_id = $1
  ORDER BY id DESC
  LIMIT 1
)
RETURNING id, session_id, reason
"""

# ✅ ACTUALIZADO: al aplicar feedback también seteamos end_ts = $4 (UTC del momento de respuesta)
APPLY_FEEDBACK_BY_SESSION_SQL = """
UPDATE intervalos_label
SET label   = $2,
    duracion = $3,
    reason  = 'ok',
    end_ts  = $4
WHERE id = (
  SELECT id FROM intervalos_label
  WHERE session_id = $1
  ORDER BY id DESC
  LIMIT 1
)
RETURNING id, session_id, label, duracion, reason, end_ts
"""

# Para obtener id_usuario del último registro de ese session_id (para notificar policy)
GET_USER_FOR_SESSION_SQL = """
SELECT id_usuario
FROM intervalos_label
WHERE session_id = $1
ORDER BY id DESC
LIMIT 1
"""

# Backfill de ventanas por duración hacia atrás desde "ahora" (UTC)
WINDOWS_BACKFILL_SQL = """
UPDATE windows
SET etiqueta = $1
WHERE session_id = $2
  AND end_time >= (NOW() AT TIME ZONE 'UTC') - ($3::int * INTERVAL '1 second')
  AND end_time <= (NOW() AT TIME ZONE 'UTC');
"""

WINDOWS_INSERT_SQL = """
INSERT INTO windows (
  id_usuario, session_id,
  received_at, start_time, end_time, sample_count, sample_rate_hz,
  features, samples_json,
  start_index, end_index, n_muestras, etiqueta,

  pred_label, confianza, precision, actividad,

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

  $14, $15, $16, $17,

  $18, $19, $20, $21, $22,
  $23, $24, $25, $26, $27,
  $28, $29, $30, $31, $32,

  $33, $34, $35, $36, $37,
  $38, $39, $40, $41, $42,
  $43, $44, $45, $46, $47,

  $48, $49, $50, $51, $52,
  $53, $54, $55, $56, $57,

  $58, $59, $60, $61, $62
)
RETURNING id;
"""

# ---------- Utils ----------
_email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def is_window_payload(d: Dict[str, Any]) -> bool:
    return isinstance(d, dict) and "features" in d and "start_time" in d and "end_time" in d

def _parse_ts(value):
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

# ===== NUEVO: helpers de modo / etiquetas nulas =====
def _is_nullish(v) -> bool:
    if v is None: return True
    s = str(v).strip().lower()
    return s in ("", "null", "none", "na", "sin prediccion")

def _mode_with_null(labels):
    """Devuelve (winner_or_None, counts_dict_including_None).
    Empate: se prefiere una etiqueta NO nula."""
    counts = {}
    for lab in labels:
        key = None if _is_nullish(lab) else str(lab).strip()
        counts[key] = counts.get(key, 0) + 1

    if not counts:
        return None, {}

    winner, maxc = None, -1
    for k, c in counts.items():
        if c > maxc:
            winner, maxc = k, c
        elif c == maxc:
            if winner is None and k is not None:
                winner = k
    return winner, counts

# ---------- Notificaciones HTTP (sin deps) ----------
def _post_predict_sync(win_id: int):
    """Bloqueante: usa urllib.request para POSTear el id. Se ejecuta en hilo."""
    data = json.dumps({"id": win_id}).encode("utf-8")
    req = urllib.request.Request(
        PREDICT_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
            _ = resp.read()
            code = resp.getcode()
            if code != 200:
                print(f"⚠️ Modelo respondió {code}")
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        print(f"⚠️ Error HTTP al notificar modelo: {e.code} {body[:200]}")
    except urllib.error.URLError as e:
        print(f"⚠️ Error de red al notificar modelo: {e.reason}")
    except Exception as e:
        print(f"💥 Error notificando al modelo: {e}")

async def notify_model(win_id: int):
    """Envía el id en background sin bloquear el event loop."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _post_predict_sync, win_id)

def _post_policy_labeled_sync(uid: int, session_id: int, when_dt: Optional[datetime]):
    """Notifica al motor de reglas que el usuario etiquetó (o arrancó)."""
    payload = {"id_usuario": uid, "session_id": session_id}
    if when_dt is not None:
        payload["when"] = when_dt.astimezone(timezone.utc).isoformat()
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        POLICY_LABELED_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
            _ = resp.read()
    except Exception as e:
        print(f"⚠️ Error notificando policy/labeled: {e}")

async def notify_policy_labeled(uid: int, session_id: int, when_dt: Optional[datetime] = None):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _post_policy_labeled_sync, uid, session_id, when_dt)

# ---------- Helpers de etiqueta vigente ----------
async def _db_fetch_latest_label_for_session(conn: asyncpg.Connection, session_id: int) -> Optional[str]:
    row = await conn.fetchrow("""
        SELECT label
        FROM intervalos_label
        WHERE session_id = $1
        ORDER BY id DESC
        LIMIT 1
    """, session_id)
    if row and row["label"]:
        return row["label"]
    return None

async def _get_current_label(session_id: int) -> Optional[str]:
    if session_id in SESSION_ACTIVITY:
        return SESSION_ACTIVITY[session_id]
    assert POOL is not None
    async with POOL.acquire() as conn:
        lab = await _db_fetch_latest_label_for_session(conn, session_id)
        if lab:
            SESSION_ACTIVITY[session_id] = lab
        return lab

async def _resolve_session_id(conn: asyncpg.Connection, maybe_interval_id: Optional[int], maybe_session_id: Optional[int]) -> Optional[int]:
    """Si recibimos session_id directamente, úsalo. Si recibimos interval_id, buscamos su session_id."""
    if maybe_session_id is not None:
        return maybe_session_id
    if maybe_interval_id is not None:
        sid = await conn.fetchval(GET_SESSION_ID_BY_INTERVAL_ID_SQL, maybe_interval_id)
        return int(sid) if sid is not None else None
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

    new_id = await conn.fetchval(GET_NEXT_INTERVAL_ID_SQL)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    row = await conn.fetchrow(
        INSERT_INTERVAL_WITH_ID_SQL,
        new_id,
        user_id,
        label.strip(),
        (reason or "").strip() or None,
        now
    )

    # cachear etiqueta vigente para este session_id
    SESSION_ACTIVITY[row["session_id"]] = row["label"]

    # 🔔 Notificar al motor de políticas que hay etiqueta inicial
    asyncio.create_task(
        notify_policy_labeled(user_id, row["session_id"], row["start_ts"])
    )

    return {
        "ok": True,
        "interval_id": row["id"],        # id de esta fila
        "session_id": row["session_id"], # grupo (puede tener varias filas a futuro)
        "label": row["label"],
        "start_ts": row["start_ts"].isoformat()
    }

# stop por SESSION (no tocamos end_ts, solo reason='finish' en la última fila del grupo)
async def stop_session_by_session(conn: asyncpg.Connection, session_id: int):
    row = await conn.fetchrow(STOP_BY_SESSION_SQL, session_id)
    if not row:
        return {"ok": False, "code": "not_found", "message": "Sesión no encontrada"}
    return {"ok": True, "interval_id": row["id"], "session_id": row["session_id"], "reason": row["reason"]}

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

    id_usuario = _as_int(item.get("id_usuario"))
    interval_id = _as_int(item.get("session_id"))  # FK a intervalos_label.id

    start_time   = _parse_ts(item.get("start_time"))
    end_time     = _parse_ts(item.get("end_time"))
    sample_count = _normi(item.get("sample_count"))
    sample_rate  = _normf(item.get("sample_rate_hz"))
    feats        = item.get("features") or {}
    samples      = item.get("samples")

    start_index  = _normi(feats.get("start_index"))
    end_index    = _normi(feats.get("end_index"))
    n_muestras   = _normi(feats.get("n_muestras"))

    # ❗️NO escribir 'etiqueta' en windows hasta confirmación del usuario.
    etiqueta = None

    pred_label   = None
    confianza    = None
    precision    = None
    actividad    = None

    def f(k): return _normf(feats.get(k))

    args = [
        id_usuario, interval_id,
        received_at, start_time, end_time, sample_count, sample_rate,
        json.dumps(feats, ensure_ascii=False),
        json.dumps(samples, ensure_ascii=False) if samples is not None else None,
        start_index, end_index, n_muestras, etiqueta,

        pred_label, confianza, precision, actividad,

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

    # Lanza notificación al modelo en background (solo id)
    asyncio.create_task(notify_model(win_id))

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
                            _pwd   = (data.get("password") or "")
                            resp = await register_user(conn, _email, _name, _bday, _pwd)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "login":
                            _email = (data.get("email") or "").strip()
                            _pwd   = (data.get("password") or "")
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
                            reason = (data.get("reason") or "").strip()
                            resp = await start_session(conn, uid, label, reason)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "stop_session":
                            # Compatibilidad: aceptar session_id o interval_id
                            session_id = data.get("session_id")
                            interval_id = data.get("interval_id")
                            try:
                                session_id = int(session_id) if session_id is not None else None
                            except Exception:
                                session_id = None
                            try:
                                interval_id = int(interval_id) if interval_id is not None else None
                            except Exception:
                                interval_id = None

                            sid = await _resolve_session_id(conn, interval_id, session_id)
                            if sid is None:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_session", "message": "No se pudo resolver session_id"})); continue

                            resp = await stop_session_by_session(conn, sid)
                            await websocket.send(json.dumps(resp)); continue

                        elif typ == "get_interval_reason":
                            # Compat: aceptar interval_id o session_id; devolver la ÚLTIMA fila del session_id
                            session_id = data.get("session_id")
                            interval_id = data.get("interval_id")
                            try:
                                session_id = int(session_id) if session_id is not None else None
                            except Exception:
                                session_id = None
                            try:
                                interval_id = int(interval_id) if interval_id is not None else None
                            except Exception:
                                interval_id = None

                            sid = await _resolve_session_id(conn, interval_id, session_id)
                            if sid is None:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_session", "message": "No se pudo resolver session_id"})); continue

                            row = await conn.fetchrow(GET_LATEST_INTERVAL_FOR_SESSION_SQL, sid)
                            if not row:
                                await websocket.send(json.dumps({"ok": False, "code": "not_found", "message": "Sesión no encontrada"})); continue

                            await websocket.send(json.dumps({
                                "ok": True,
                                "interval_id": row["id"],
                                "session_id": row["session_id"],
                                "reason": row["reason"],
                                "label": row["label"],
                                "duracion": row["duracion"]
                            })); continue

                        elif typ == "apply_interval_feedback":
                            # Compat: aceptar interval_id o session_id; aplicar sobre la ÚLTIMA fila del session_id
                            new_label = (data.get("label") or "").strip()
                            try:
                                dur = int(data.get("duracion") or 0)
                            except Exception:
                                dur = 0
                            if not new_label:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_label", "message": "Actividad requerida"})); continue

                            session_id = data.get("session_id")
                            interval_id = data.get("interval_id")
                            try:
                                session_id = int(session_id) if session_id is not None else None
                            except Exception:
                                session_id = None
                            try:
                                interval_id = int(interval_id) if interval_id is not None else None
                            except Exception:
                                interval_id = None

                            sid = await _resolve_session_id(conn, interval_id, session_id)
                            if sid is None:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_session", "message": "No se pudo resolver session_id"})); continue

                            # ✅ end_ts = "momento en el que se respondió" en UTC (+00)
                            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

                            row = await conn.fetchrow(APPLY_FEEDBACK_BY_SESSION_SQL, sid, new_label, dur, now_utc)  # ✅ pasa end_ts
                            if not row:
                                await websocket.send(json.dumps({"ok": False, "code": "not_found", "message": "Sesión no encontrada"})); continue

                            # 2) Backfill de ventanas previas (ahora - dur .. ahora)
                            try:
                                await conn.execute(WINDOWS_BACKFILL_SQL, new_label, sid, dur)
                            except Exception as e:
                                print(f"⚠️ Backfill windows falló: {e}")

                            # 3) Actualiza etiqueta vigente en cache para futuras ventanas
                            SESSION_ACTIVITY[sid] = new_label

                            # 4) 🔔 Notificar al motor de políticas que el usuario etiquetó ahora
                            try:
                                uid_for_policy = await conn.fetchval(GET_USER_FOR_SESSION_SQL, sid)
                                if uid_for_policy:
                                    asyncio.create_task(
                                        notify_policy_labeled(int(uid_for_policy), sid, now_utc)
                                    )
                            except Exception as e:
                                print(f"⚠️ policy/labeled tras feedback falló: {e}")

                            await websocket.send(json.dumps({
                                "ok": True,
                                "interval_id": row["id"],
                                "session_id": row["session_id"],
                                "label": row["label"],
                                "duracion": row["duracion"],
                                "reason": row["reason"],
                                "end_ts": row["end_ts"].isoformat() if row["end_ts"] else now_utc.isoformat(),  # útil para el cliente
                            })); continue

                        # ===== NUEVO RPC: modo (actividad más frecuente) en 10 s =====
                        elif typ == "get_predictions_mode":
                            # Params: session_id (o interval_id), horizon_sec (opcional, default 10)
                            session_id = data.get("session_id")
                            interval_id = data.get("interval_id")
                            try:
                                session_id = int(session_id) if session_id is not None else None
                            except Exception:
                                session_id = None
                            try:
                                interval_id = int(interval_id) if interval_id is not None else None
                            except Exception:
                                interval_id = None

                            sid = await _resolve_session_id(conn, interval_id, session_id)
                            if sid is None:
                                await websocket.send(json.dumps({"ok": False, "code": "bad_session", "message": "No se pudo resolver session_id"})); continue

                            try:
                                horizon = int(data.get("horizon_sec", 10))
                            except Exception:
                                horizon = 10
                            if horizon < 1: horizon = 1
                            if horizon > 120: horizon = 120

                            rows = await conn.fetch("""
                                SELECT pred_label, actividad
                                  FROM windows
                                 WHERE session_id = $1
                                   AND end_time >= (NOW() AT TIME ZONE 'UTC') - ($2::int * INTERVAL '1 second')
                                   AND end_time <= (NOW() AT TIME ZONE 'UTC')
                                 ORDER BY end_time DESC
                            """, sid, horizon)

                            pre_labels = [r["pred_label"] for r in rows]
                            sl_labels  = [r["actividad"]  for r in rows]

                            pre_mode, pre_counts = _mode_with_null(pre_labels)
                            sl_mode,  sl_counts  = _mode_with_null(sl_labels)

                            resp = {
                                "ok": True,
                                "session_id": sid,
                                "horizon_sec": horizon,
                                "pred_label_mode": pre_mode,   # puede ser None
                                "actividad_mode":  sl_mode,    # puede ser None
                                "counts": {
                                    "pred_label": { ("" if k is None else k): v for k, v in pre_counts.items() },
                                    "actividad":  { ("" if k is None else k): v for k, v in sl_counts.items() },
                                },
                                "windows_considered": len(rows),
                            }
                            await websocket.send(json.dumps(resp, ensure_ascii=False)); continue

                        elif typ == "ping":
                            await websocket.send(json.dumps({"ok": True, "type": "pong"})); continue

                        else:
                            await websocket.send(json.dumps({"ok": False, "code": "unknown_type", "message": "Tipo de RPC desconocido"})); continue
                except Exception as e:
                    print(f"💥 Error en RPC {typ}: {e}")
                    try: await websocket.send(json.dumps({"ok": False, "code": "server_error", "message": str(e)}))
                    except Exception: pass
                    continue

            # Ventanas (array o objeto)
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
