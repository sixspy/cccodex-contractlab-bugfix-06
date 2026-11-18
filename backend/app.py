
import hashlib, secrets, sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 假缓冲修复径：甲径（同事务双写）；乙径草稿支未启用，禁止混用。
POLICY_DEFAULT_PATH = "甲"
POLICY_DEFAULT_NOTE = "甲径：批次缓冲标志与缓冲行同事务双写，缺一整笔回滚"

@contextmanager
def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # 事务显式开关，双写才能整笔提交/回滚
    conn.execute("PRAGMA busy_timeout=15000")
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def transaction(conn):
    """BEGIN IMMEDIATE：并发写入在库级排队，后到者等前一笔提交。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")

def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")

def today_text() -> str:
    return date.today().isoformat()

def record_event(conn, kind: str, batch_id, actor: str, detail: str, at: str | None = None):
    """审计事件：假缓冲偏了 / 缓冲争用 / 缓冲补行。与业务写入共用事务。"""
    conn.execute(
        "INSERT INTO buffer_events(kind,batch_id,actor,detail,created_at) VALUES (?,?,?,?,?)",
        (kind, batch_id, actor, detail, at or now_text()),
    )

def record_event_separate(kind: str, batch_id, actor: str, detail: str):
    """事务之外也要落下的审计事件（偏离/争用）：独立连接提交。"""
    try:
        with connect() as conn:
            record_event(conn, kind, batch_id, actor, detail)
    except sqlite3.Error:
        pass

def ensure_policy(conn):
    conn.execute(
        "INSERT OR IGNORE INTO buffer_policy(id,path,note,decided_on) VALUES (1,?,?,?)",
        (POLICY_DEFAULT_PATH, POLICY_DEFAULT_NOTE, today_text()),
    )
    return conn.execute("SELECT * FROM buffer_policy WHERE id=1").fetchone()

def policy_row(conn):
    """只读口径：缺行时按默认甲径解释，不因读而写库。"""
    row = conn.execute("SELECT * FROM buffer_policy WHERE id=1").fetchone()
    if row:
        return row
    return {"path": POLICY_DEFAULT_PATH, "note": POLICY_DEFAULT_NOTE, "decided_on": today_text()}

def buffer_snapshot(conn) -> dict:
    """从原始数据推导口径，不看任何自报数字；五个视图共用同一结论（甲径）。"""
    policy = policy_row(conn)
    flag_ids = {r["id"] for r in conn.execute("SELECT id FROM ingress_batches WHERE result='缓冲'")}
    row_ids = {r["batch_id"] for r in conn.execute("SELECT batch_id FROM buffers")}
    row_count = conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    # 甲径只认实打实的缓冲行：标缓冲无行即假缓冲，有行无标志即脏行，一行多挂也算假绿。
    fake = sorted(flag_ids - row_ids)
    orphan = sorted(row_ids - flag_ids)
    counted = row_ids & flag_ids
    aligned = not fake and not orphan and row_count == len(counted)
    return {
        "ingress_total": conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"],
        "passed": conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"],
        "buffered": len(flag_ids),
        "rejected": conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"],
        "buffer_rows": row_count,
        "counted_buffers": len(counted),
        "fake_buffer_batches": fake,
        "orphan_buffer_batches": orphan,
        "aligned": aligned,
        "verdict": VERDICT_GREEN if aligned else VERDICT_FAKE,
        "path": policy["path"],
        "policy_note": policy["note"],
    }

def verdict_payload(snap: dict) -> dict:
    return {
        "aligned": snap["aligned"],
        "verdict": snap["verdict"],
        "fake_buffer_batches": snap["fake_buffer_batches"],
        "orphan_buffer_batches": snap["orphan_buffer_batches"],
        "buffer_rows": snap["buffer_rows"],
        "buffered": snap["buffered"],
        "path": snap["path"],
        "policy_note": snap["policy_note"],
    }

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"
VERDICT_GREEN = "绿"
VERDICT_FAKE = "假缓冲"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffer_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          batch_id INTEGER,
          actor TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffer_policy(
          id INTEGER PRIMARY KEY CHECK (id=1),
          path TEXT NOT NULL,
          note TEXT NOT NULL,
          decided_on TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
        # 选定后立即落入“假缓冲说明”，当天钉死不许换径。
        ensure_policy(conn)
        # 旧库没有唯一约束：补上唯一索引，兜住并发重复补行。
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_buffers_batch ON buffers(batch_id)")
        except sqlite3.IntegrityError:
            record_event(conn, "假缓冲偏了", None, "system", "存量缓冲重复行，唯一索引未建立")
        # 存量库若已有“标缓冲无行”的假缓冲，补记偏离，不再静默。
        fake_ids = [
            r["id"]
            for r in conn.execute(
                "SELECT i.id FROM ingress_batches i LEFT JOIN buffers b ON b.batch_id=i.id"
                " WHERE i.result='缓冲' AND b.batch_id IS NULL"
            )
        ]
        has_marker = conn.execute(
            "SELECT 1 FROM buffer_events WHERE kind='假缓冲偏了' AND detail LIKE '存量假缓冲%' LIMIT 1"
        ).fetchone()
        if fake_ids and not has_marker:
            record_event(conn, "假缓冲偏了", None, "system",
                         f"存量假缓冲待补行:{','.join(str(i) for i in fake_ids)}")

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    actor = require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    now = now_text()
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        if missing and ctr["mode"] == "兼容":
            # 甲径：批次缓冲标志与缓冲行同一事务，缺一则整笔回滚。
            try:
                with connect() as w, transaction(w):
                    cur = w.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                    (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                    bid = cur.lastrowid
                    w.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (bid, "缺字段进双写缓冲"))
                    record_event(w, "缓冲双写", bid, actor, "批次标志+缓冲行同事务")
            except sqlite3.Error as exc:
                # 任一环失败：整笔回滚，并留下“假缓冲偏了”痕迹，不入账。
                record_event_separate("假缓冲偏了", None, actor, f"兼容缺字段双写失败:{exc}")
                raise HTTPException(503, "兼容缓冲双写失败，整笔回滚") from exc
            return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": bid}
        with connect() as w, transaction(w):
            if missing:
                cur = w.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
                bid = cur.lastrowid
                return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": bid}
            cur = w.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
            bid = cur.lastrowid
        return {"result":"通过", "version": ctr["version"], "batch_id": bid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        snap = buffer_snapshot(c)
        items = [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]
    return {"items": items, **verdict_payload(snap)}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        snap = buffer_snapshot(c)
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b LEFT JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        missing = c.execute("""SELECT i.* FROM ingress_batches i LEFT JOIN buffers b ON b.batch_id=i.id
            WHERE i.result='缓冲' AND b.batch_id IS NULL ORDER BY i.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows], "missing_items": [dict(r, batch_id=r["id"]) for r in missing], **verdict_payload(snap)}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        snap = buffer_snapshot(c)
        row_count = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    return {
        "ingress_total": snap["ingress_total"],
        "passed": snap["passed"],
        "buffered": snap["buffered"],
        "rejected": snap["rejected"],
        "buffer_rows": row_count,
        "counted_buffers": snap["counted_buffers"],
        "aligned": snap["aligned"],
        "verdict": snap["verdict"],
        "fake_buffer_batches": snap["fake_buffer_batches"],
        "orphan_buffer_batches": snap["orphan_buffer_batches"],
    }

@app.get("/api/strategy")
def strategy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        snap = buffer_snapshot(c)
        policy = dict(policy_row(c))
        events = [dict(r) for r in c.execute("SELECT * FROM buffer_events ORDER BY id DESC LIMIT 20").fetchall()]
    return {**verdict_payload(snap), "policy": policy, "events": events}

@app.get("/api/review")
def review(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        snap = buffer_snapshot(c)
    return {
        **verdict_payload(snap),
        "card": {
            "title": "缓冲复核卡片",
            "verdict": snap["verdict"],
            "counted_buffers": snap["counted_buffers"],
            "fake_buffer_batches": snap["fake_buffer_batches"],
        },
    }

class SupplementIn(BaseModel):
    batch_id: int

@app.post("/api/buffers/supplement")
def supplement(body: SupplementIn, authorization: str | None = Header(default=None)):
    """值班补行：排队写入；补成记“缓冲补行”，重复补者只记“缓冲争用”。"""
    actor = require_user(authorization)
    with connect() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (body.batch_id,)).fetchone()
    if not batch:
        raise HTTPException(404, "批次不存在")
    if batch["result"] != "缓冲":
        # 拒收不得改写成缓冲；拦下并留痕。
        record_event_separate("假缓冲偏了", body.batch_id, actor, f"拒绝把{batch['result']}改写成缓冲")
        raise HTTPException(409, "拒收批次不得改写为缓冲")
    try:
        with connect() as w, transaction(w):
            existing = w.execute("SELECT id FROM buffers WHERE batch_id=?", (body.batch_id,)).fetchone()
            if existing:
                record_event(w, "缓冲争用", body.batch_id, actor, "后到补行让位，仅记争用")
                return {"batch_id": body.batch_id, "buffer_id": existing["id"], "supplemented": False, "contended": True}
            cur = w.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (body.batch_id, "值班补行"))
            record_event(w, "缓冲补行", body.batch_id, actor, "补行成功")
            return {"batch_id": body.batch_id, "buffer_id": cur.lastrowid, "supplemented": True, "contended": False}
    except sqlite3.IntegrityError:
        # 唯一约束兜底：并发下另一笔已挂行，后到者只记争用。
        record_event_separate("缓冲争用", body.batch_id, actor, "唯一约束拦下重复补行")
        with connect() as c:
            row = c.execute("SELECT id FROM buffers WHERE batch_id=?", (body.batch_id,)).fetchone()
        return {"batch_id": body.batch_id, "buffer_id": row["id"] if row else None, "supplemented": False, "contended": True}

class PolicyIn(BaseModel):
    path: str
    note: str = ""

@app.get("/api/buffer-policy")
def get_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return dict(policy_row(c))

@app.put("/api/buffer-policy")
def put_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    """选定即落入假缓冲说明；当天不许换径，偏离记“假缓冲偏了”。"""
    actor = require_user(authorization)
    with connect() as w:
        policy = policy_row(w)
    if body.path != policy["path"]:
        record_event_separate("假缓冲偏了", None, actor, f"当天试换径 {policy['path']}->{body.path}")
        raise HTTPException(409, "假缓冲修复径已钉死，当天不许换")
    if body.path != POLICY_DEFAULT_PATH:
        record_event_separate("假缓冲偏了", None, actor, f"选用未启用的{body.path}径")
        raise HTTPException(409, "仅启用甲径，乙径草稿支不得混用")
    with connect() as w, transaction(w):
        ensure_policy(w)
        w.execute("UPDATE buffer_policy SET note=? WHERE id=1", (body.note or policy["note"],))
        return dict(w.execute("SELECT * FROM buffer_policy WHERE id=1").fetchone())

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
