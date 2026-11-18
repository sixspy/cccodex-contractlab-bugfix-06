"""假缓冲（fake buffer）修复的回归测试。

口径（钉死甲径，不混用乙径草稿支）：
- 兼容模式缺字段 -> 批次缓冲标志与缓冲行同一事务双写，缺一整笔回滚。
- 有“缓冲”结果而无缓冲行 = 假缓冲，任何视图都不得报绿。
- 拒收不得改写成缓冲。
- 值班补行并发必须排队，后到者只记“缓冲争用”。
"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import app as app_module


@pytest.fixture()
def ctx(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    app_module.init_db()
    client = TestClient(app_module.app, raise_server_exceptions=False)
    tok = client.post(
        "/api/login", json={"username": "admin", "password": "123456"}
    ).json()["token"]
    return client, {"Authorization": f"Bearer {tok}"}


def _enable_compat():
    with app_module.connect() as conn:
        conn.execute(
            "UPDATE contracts SET track='正式', status='启用' WHERE version='v4-shadow'"
        )


def _insert_batch(result="缓冲", detail="缺字段:currency"):
    with app_module.connect() as conn:
        cur = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
            " VALUES (?,?,?,?,?,?)",
            ("订单入站", "order_id,amount", "v4-shadow", result, detail, "2026-11-18T00:00:00"),
        )
        return cur.lastrowid


def _buffer_batch_ids():
    with app_module.connect() as conn:
        return [r["batch_id"] for r in conn.execute("SELECT batch_id FROM buffers")]


def _event_kinds():
    with app_module.connect() as conn:
        return [r["kind"] for r in conn.execute("SELECT kind FROM buffer_events")]


def test_compat_missing_field_repro_fake_buffer_is_red(ctx):
    """先复现：兼容缺字段落缓冲，必须真有行；无行则是假缓冲，对账不得假绿。"""
    client, headers = ctx
    _enable_compat()

    r = client.post(
        "/api/ingress/validate",
        headers=headers,
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "缓冲"
    assert body["batch_id"] in _buffer_batch_ids(), "标缓冲必须同时落缓冲行"

    # 构造旧仓遗留的假缓冲：删掉缓冲行，再用一条脏行把总数配平。
    bid = body["batch_id"]
    with app_module.connect() as conn:
        conn.execute("DELETE FROM buffers WHERE batch_id=?", (bid,))
        orphan = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
            " VALUES (?,?,?,?,?,?)",
            ("订单入站", "x", "v3", "拒收", "缺字段:x", "2026-11-18T00:00:00"),
        ).lastrowid
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (orphan, "脏行"))

    recon = client.get("/api/reconcile", headers=headers).json()
    assert recon["aligned"] is False, "有缓冲结果无行时对账不得假绿"
    assert bid in recon["fake_buffer_batches"]
    assert orphan in recon["orphan_buffer_batches"]

    queue = client.get("/api/buffers", headers=headers).json()
    assert bid in [x["batch_id"] for x in queue["missing_items"]]
    assert queue["aligned"] is False


def test_compat_double_write_failure_rolls_back_whole_batch(ctx):
    """双写失败测：缓冲行写不进去，批次行也必须回滚，并记“假缓冲偏了”。"""
    client, headers = ctx
    _enable_compat()
    with app_module.connect() as conn:
        conn.executescript(
            "CREATE TRIGGER no_buffer_row BEFORE INSERT ON buffers "
            "BEGIN SELECT RAISE(ABORT, 'double write down'); END;"
        )

    r = client.post(
        "/api/ingress/validate",
        headers=headers,
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    assert r.status_code == 503, r.text
    assert "回滚" in r.json()["detail"]

    with app_module.connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'"
        ).fetchone()["c"]
        b = conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    assert (n, b) == (0, 0), "缺一环必须整笔回滚，禁止留下无行的缓冲结果"
    assert "假缓冲偏了" in _event_kinds()


def test_reject_is_never_rewritten_as_buffer(ctx):
    """拒收直接拒收，不得改写为缓冲；补行接口也必须拒绝。"""
    client, headers = ctx
    r = client.post(
        "/api/ingress/validate",
        headers=headers,
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    assert r.json()["result"] == "拒收"
    bid = r.json()["batch_id"]
    assert _buffer_batch_ids() == []

    s = client.post("/api/buffers/supplement", headers=headers, json={"batch_id": bid})
    assert s.status_code == 409
    assert _buffer_batch_ids() == []
    assert "假缓冲偏了" in _event_kinds()

    recon = client.get("/api/reconcile", headers=headers).json()
    assert recon["aligned"] is True
    assert recon["rejected"] == 1 and recon["buffered"] == 0


def test_supplement_concurrency_queues_and_loser_records_contention(ctx):
    """两名值班同时补同一行：排队，成功恰一条，后到者只记“缓冲争用”。"""
    client, headers = ctx
    bid = _insert_batch(result="缓冲")  # 模拟崩溃/遗留：有缓冲标志但无行

    barrier = threading.Barrier(2)

    def worker():
        c = TestClient(app_module.app, raise_server_exceptions=False)
        barrier.wait(timeout=5)
        return c.post(
            "/api/buffers/supplement", headers=headers, json={"batch_id": bid}
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [f.result(timeout=20) for f in [pool.submit(worker) for _ in range(2)]]

    assert [r.status_code for r in results] == [200, 200], [r.text for r in results]
    bodies = [r.json() for r in results]
    assert sum(1 for b in bodies if b["supplemented"]) == 1
    assert sum(1 for b in bodies if b["contended"]) == 1
    assert _buffer_batch_ids().count(bid) == 1, "并发补行不得插出重复行"
    assert _event_kinds().count("缓冲争用") == 1

    recon = client.get("/api/reconcile", headers=headers).json()
    assert recon["aligned"] is True
    assert recon["fake_buffer_batches"] == []


def test_policy_path_is_pinned_and_same_day_switch_blocked(ctx):
    """甲径选定后写入说明；当天不许换径，偏离记“假缓冲偏了”。"""
    client, headers = ctx
    p = client.get("/api/buffer-policy", headers=headers).json()
    assert p["path"] == "甲"

    bad = client.put(
        "/api/buffer-policy",
        headers=headers,
        json={"path": "乙", "note": "换草稿支"},
    )
    assert bad.status_code == 409
    assert client.get("/api/buffer-policy", headers=headers).json()["path"] == "甲"
    assert "假缓冲偏了" in _event_kinds()

    same = client.put(
        "/api/buffer-policy",
        headers=headers,
        json={"path": "甲", "note": "甲径：同事务双写"},
    )
    assert same.status_code == 200


def test_all_five_views_share_one_verdict(ctx):
    """列表、缓冲队列、对账账册、调策略附页、复核卡片同绿同空。"""
    client, headers = ctx
    _enable_compat()
    client.post(
        "/api/ingress/validate",
        headers=headers,
        json={"subject": "订单入站", "payload_fields": "order_id,amount"},
    )
    urls = [
        "/api/ingress",
        "/api/buffers",
        "/api/reconcile",
        "/api/strategy",
        "/api/review",
    ]
    for url in urls:
        body = client.get(url, headers=headers).json()
        assert body["aligned"] is True, url
        assert body["verdict"] == "绿", url

    # 制造假缓冲后，五处必须同时转红。
    first = client.get("/api/buffers", headers=headers).json()["items"][0]["batch_id"]
    with app_module.connect() as conn:
        conn.execute("DELETE FROM buffers WHERE batch_id=?", (first,))
    for url in urls:
        body = client.get(url, headers=headers).json()
        assert body["aligned"] is False, url
        assert body["verdict"] == "假缓冲", url
        assert first in body["fake_buffer_batches"], url
