import os
import sqlite3
import tempfile
import threading

import pytest

_tmpdir = tempfile.mkdtemp(prefix="metrodp-")
os.environ["APP_DB"] = os.path.join(_tmpdir, "test.db")

import app as app_module  # noqa: E402

CSV_V1 = "乘客ID,进站,出站\np1,A,X\np2,A,X\np3,B,X\np4,A,Y\np5,C,Z\n"
CSV_V2 = "乘客ID,进站,出站\np1,A,X\np2,B,Y\n"
# 订正版: 补回漏记的 p6(A→X), 共 6 行
CSV_V3 = "乘客ID,进站,出站\np1,A,X\np2,A,X\np3,B,X\np4,A,Y\np5,C,Z\np6,A,X\n"


@pytest.fixture(autouse=True)
def clean_db():
    conn = sqlite3.connect(os.environ["APP_DB"])
    conn.executescript("DELETE FROM queries; DELETE FROM day_state; DELETE FROM passengers;")
    conn.close()
    yield


@pytest.fixture
def client():
    return app_module.app.test_client()


def upload(client, date, csv_text):
    return client.post("/upload", data={"date": date, "csv": csv_text})


def query(client, date, ins, outs):
    return client.post("/query", json={"date": date, "in_stations": ins,
                                       "out_stations": outs})


def query_rev(client, date, rev, ins, outs):
    return client.post("/query", json={"date": date, "revision": rev,
                                       "in_stations": ins, "out_stations": outs})


def correct(client, date, rev, csv_text):
    return client.post("/correct", data={"date": date, "revision": rev,
                                         "csv": csv_text})


def status(client, date):
    return client.get(f"/api/status?date={date}").get_json()


# ------------------------------------------------------------- 上传与锁定

def test_upload_ok_and_replace_before_lock(client):
    r = upload(client, "2026-11-01", CSV_V1)
    assert r.status_code == 200 and r.get_json()["rows"] == 5
    # 锁定前可整日替换
    r = upload(client, "2026-11-01", CSV_V2)
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    st = status(client, "2026-11-01")
    assert st["rows"] == 2 and not st["locked"]


def test_upload_rejected_after_lock(client):
    upload(client, "2026-11-02", CSV_V1)
    r = query(client, "2026-11-02", ["A"], ["X"])
    assert r.status_code == 200
    r = upload(client, "2026-11-02", CSV_V2)
    assert r.status_code == 409
    assert "锁定" in r.get_json()["error"]
    # 源数据未被改动
    assert status(client, "2026-11-02")["rows"] == 5


def test_duplicate_passenger_rejected(client):
    r = upload(client, "2026-11-03", "乘客ID,进站,出站\np1,A,X\np1,B,Y\n")
    assert r.status_code == 400
    assert "重复" in r.get_json()["error"]


def test_csv_without_header_and_english_header(client):
    r = upload(client, "2026-11-04", "p1,A,X\np2,B,Y\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 2
    r = upload(client, "2026-11-05", "passenger_id,in_station,out_station\np1,A,X\n")
    assert r.status_code == 200 and r.get_json()["rows"] == 1


# ------------------------------------------------------------- 查询与缓存

def test_query_returns_noisy_only(client):
    upload(client, "2026-11-06", CSV_V1)
    r = query(client, "2026-11-06", ["A"], ["X"])
    body = r.get_json()
    assert r.status_code == 200 and body["ok"]
    assert isinstance(body["noisy_count"], float)
    assert "true_count" not in body and "count" not in body
    assert status(client, "2026-11-06")["budget_spent"] == 0.5
    assert status(client, "2026-11-06")["locked"]


def test_same_query_reused_regardless_of_order_and_duplicates(client):
    upload(client, "2026-11-07", CSV_V1)
    r1 = query(client, "2026-11-07", ["A", "B"], ["X", "Y"]).get_json()
    r2 = query(client, "2026-11-07", ["B", "A", "A"], ["Y", "X", "X"]).get_json()
    assert r1["noisy_count"] == r2["noisy_count"]
    assert r2["cached"] is True
    # 第二次未扣费
    assert status(client, "2026-11-07")["budget_spent"] == 0.5


def test_budget_limit_four_queries(client):
    upload(client, "2026-11-08", CSV_V1)
    ok, rejected = 0, 0
    for i in range(6):
        r = query(client, "2026-11-08", [f"S{i}"], ["X"])
        if r.status_code == 200:
            ok += 1
        else:
            rejected += 1
            assert r.status_code == 403
            assert "预算不足" in r.get_json()["error"]
    assert ok == 4 and rejected == 2  # 2 / 0.5 = 4
    assert status(client, "2026-11-08")["budget_spent"] == 2.0
    # 预算用完后, 已缓存的查询仍然返回原值
    r = query(client, "2026-11-08", ["S0"], ["X"])
    assert r.status_code == 200 and r.get_json()["cached"] is True


def test_budget_is_per_date(client):
    upload(client, "2026-11-09", CSV_V1)
    upload(client, "2026-11-10", CSV_V1)
    for _ in range(4):
        assert query(client, "2026-11-09", [f"S{_}"], ["X"]).status_code == 200
    # 另一个日期仍有完整预算
    assert query(client, "2026-11-10", ["S0"], ["X"]).status_code == 200


# ------------------------------------------------------------- 并发

def test_concurrent_distinct_queries_never_overspend(client):
    upload(client, "2026-11-11", CSV_V1)
    results = []

    def worker(i):
        c = app_module.app.test_client()
        results.append(query(c, "2026-11-11", [f"T{i}"], ["X"]).status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(200) == 4
    assert results.count(403) == 8
    assert status(client, "2026-11-11")["budget_spent"] == 2.0


def test_concurrent_identical_query_single_result(client):
    upload(client, "2026-11-12", CSV_V1)
    bodies = []

    def worker():
        c = app_module.app.test_client()
        bodies.append(query(c, "2026-11-12", ["A", "B"], ["X"]).get_json())

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(b["ok"] for b in bodies)
    assert len({b["noisy_count"] for b in bodies}) == 1  # 只有一份结果
    assert status(client, "2026-11-12")["budget_spent"] == 0.5  # 只扣一次
    conn = sqlite3.connect(os.environ["APP_DB"])
    n = conn.execute("SELECT COUNT(*) FROM queries WHERE date='2026-11-12'").fetchone()[0]
    conn.close()
    assert n == 1


# ------------------------------------------------------------- 持久化

def test_restart_keeps_results_and_budget(client):
    upload(client, "2026-11-13", CSV_V1)
    r1 = query(client, "2026-11-13", ["A"], ["X"]).get_json()
    # 模拟重启: 重新初始化数据库连接(文件不变)
    app_module.init_db()
    client2 = app_module.app.test_client()
    r2 = query(client2, "2026-11-13", ["A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == r1["noisy_count"]
    assert status(client2, "2026-11-13")["budget_spent"] == 0.5
    # 重启后上传仍被拒绝
    assert upload(client2, "2026-11-13", CSV_V2).status_code == 409


# ------------------------------------------------------------- 噪声分布

def test_laplace_noise_distribution():
    samples = [app_module.laplace_noise() for _ in range(20000)]
    mean = sum(samples) / len(samples)
    var = sum((s - mean) ** 2 for s in samples) / len(samples)
    assert abs(mean) < 0.1            # 均值 ≈ 0
    assert abs(var - 8.0) < 0.8       # Laplace(0,2) 方差 = 2 * 2^2 = 8


# ------------------------------------------------------------- 整日订正

def test_correct_creates_new_revision_and_switches_source(client):
    upload(client, "2026-12-01", CSV_V1)
    query(client, "2026-12-01", ["A"], ["X"])  # 锁定第 1 版
    r = correct(client, "2026-12-01", 1, CSV_V3)
    body = r.get_json()
    assert r.status_code == 200 and body["revision"] == 2 and body["rows"] == 6
    st = status(client, "2026-12-01")
    assert st["current_revision"] == 2 and st["rows"] == 6
    # 订正后依然锁定, 普通上传仍被拒绝
    assert upload(client, "2026-12-01", CSV_V2).status_code == 409


def test_correct_requires_seen_revision_and_existing_data(client):
    upload(client, "2026-12-02", CSV_V1)
    # 缺修订号 / 非法修订号
    assert client.post("/correct", data={"date": "2026-12-02",
                                         "csv": CSV_V3}).status_code == 400
    assert correct(client, "2026-12-02", "abc", CSV_V3).status_code == 400
    assert correct(client, "2026-12-02", 0, CSV_V3).status_code == 400
    # 所见修订号与当前不一致
    assert correct(client, "2026-12-02", 5, CSV_V3).status_code == 409
    # 当天无源数据不能订正
    assert correct(client, "2026-12-03", 1, CSV_V3).status_code == 409
    st = status(client, "2026-12-02")
    assert st["current_revision"] == 1 and st["rows"] == 5


def test_correct_csv_validation_failure_leaves_state_unchanged(client):
    upload(client, "2026-12-04", CSV_V1)
    query(client, "2026-12-04", ["A"], ["X"])
    # 乘客ID 重复 → 校验失败, 不产生新修订, 源数据与结果不动
    r = correct(client, "2026-12-04", 1, "乘客ID,进站,出站\np1,A,X\np1,B,Y\n")
    assert r.status_code == 400 and "重复" in r.get_json()["error"]
    st = status(client, "2026-12-04")
    assert st["current_revision"] == 1 and st["rows"] == 5
    assert st["budget_spent"] == 0.5


def test_concurrent_corrections_same_revision_only_one_accepted(client):
    upload(client, "2026-12-05", CSV_V1)
    query(client, "2026-12-05", ["A"], ["X"])  # 锁定
    results = []

    def worker():
        c = app_module.app.test_client()
        results.append(correct(c, "2026-12-05", 1, CSV_V3).status_code)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(200) == 1          # 同版并发订正只接受一次
    assert results.count(409) == 5
    st = status(client, "2026-12-05")
    assert st["current_revision"] == 2 and st["rows"] == 6
    # 源数据与修订号不错配: 第 2 版恰好是订正 CSV 的 6 行
    conn = sqlite3.connect(os.environ["APP_DB"])
    n = conn.execute("SELECT COUNT(*) FROM passengers"
                     " WHERE date='2026-12-05' AND revision=2").fetchone()[0]
    conn.close()
    assert n == 6


def test_correction_does_not_refund_budget(client):
    upload(client, "2026-12-06", CSV_V1)
    assert query(client, "2026-12-06", ["A"], ["X"]).status_code == 200  # 0.5
    assert query(client, "2026-12-06", ["B"], ["X"]).status_code == 200  # 1.0
    assert correct(client, "2026-12-06", 1, CSV_V3).status_code == 200
    # 跨修订不退还: 剩余 1.0, 仅够 2 次新查询
    assert query(client, "2026-12-06", ["C"], ["X"]).status_code == 200  # 1.5
    assert query(client, "2026-12-06", ["D"], ["X"]).status_code == 200  # 2.0
    r = query(client, "2026-12-06", ["E"], ["X"])
    assert r.status_code == 403 and "预算不足" in r.get_json()["error"]
    assert status(client, "2026-12-06")["budget_spent"] == 2.0


# ------------------------------------------------------------- 分版查询

def test_old_revision_replay_original_value_and_reject_unpublished(client):
    upload(client, "2026-12-07", CSV_V1)
    n1 = query(client, "2026-12-07", ["A"], ["X"]).get_json()["noisy_count"]
    correct(client, "2026-12-07", 1, CSV_V3)
    # 旧版已发布条件: 原值重查, 不扣费
    r = query_rev(client, "2026-12-07", 1, ["A"], ["X"])
    body = r.get_json()
    assert r.status_code == 200 and body["cached"] is True
    assert body["noisy_count"] == n1 and body["revision"] == 1
    assert status(client, "2026-12-07")["budget_spent"] == 0.5
    # 旧版未发布过的条件: 拒绝
    r = query_rev(client, "2026-12-07", 1, ["B"], ["X"])
    assert r.status_code == 409 and "未发布" in r.get_json()["error"]
    # 不存在的修订号
    assert query_rev(client, "2026-12-07", 3, ["A"], ["X"]).status_code == 409
    assert query_rev(client, "2026-12-07", 0, ["A"], ["X"]).status_code == 400


def test_same_condition_on_current_revision_is_fresh_query(client):
    upload(client, "2026-12-08", CSV_V1)
    query(client, "2026-12-08", ["A"], ["X"])            # 第 1 版, 扣 0.5
    correct(client, "2026-12-08", 1, CSV_V3)
    # 当前版同条件: 重新加噪并扣费
    r = query(client, "2026-12-08", ["A"], ["X"])
    body = r.get_json()
    assert r.status_code == 200 and body["cached"] is False
    assert body["revision"] == 2 and body["current_revision"] == 2
    assert status(client, "2026-12-08")["budget_spent"] == 1.0
    # 当前版内重复同条件: 复用, 不再扣费
    r2 = query(client, "2026-12-08", ["A", "A"], ["X"]).get_json()
    assert r2["cached"] is True and r2["noisy_count"] == body["noisy_count"]
    assert status(client, "2026-12-08")["budget_spent"] == 1.0


def test_old_rows_not_counted_in_new_revision(client, monkeypatch):
    monkeypatch.setattr(app_module, "laplace_noise", lambda *a, **k: 0.0)
    upload(client, "2026-12-09", CSV_V1)                 # A→X: p1, p2
    assert query(client, "2026-12-09", ["A"], ["X"]).get_json()["noisy_count"] == 2.0
    correct(client, "2026-12-09", 1, CSV_V3)             # A→X: p1, p2, p6
    # 新查询只统计当前修订的源数据
    assert query(client, "2026-12-09", ["A"], ["X"]).get_json()["noisy_count"] == 3.0
    # 旧版结果原值保留
    assert query_rev(client, "2026-12-09", 1, ["A"], ["X"]).get_json()["noisy_count"] == 2.0


def test_queries_listed_per_revision(client):
    upload(client, "2026-12-10", CSV_V1)
    query(client, "2026-12-10", ["A"], ["X"])
    correct(client, "2026-12-10", 1, CSV_V3)
    query(client, "2026-12-10", ["B"], ["X"])
    data = client.get("/api/queries?date=2026-12-10").get_json()
    assert data["current_revision"] == 2
    by_rev = {}
    for q in data["queries"]:
        by_rev.setdefault(q["revision"], []).append(q)
    assert set(by_rev) == {1, 2}
    assert by_rev[1][0]["in_stations"] == ["A"]
    assert by_rev[2][0]["in_stations"] == ["B"]


def test_stations_and_status_reflect_current_revision(client):
    upload(client, "2026-12-11", CSV_V1)
    query(client, "2026-12-11", ["A"], ["X"])
    correct(client, "2026-12-11", 1, CSV_V2)  # 第 2 版只剩 A,X,B,Y
    stations = client.get("/api/stations?date=2026-12-11").get_json()["stations"]
    assert stations == ["A", "B", "X", "Y"]   # 旧版独有的 C、Z 不再出现


# ------------------------------------------------------------- 订正后的持久化与迁移

def test_restart_after_correction_consistent(client):
    upload(client, "2026-12-12", CSV_V1)
    n1 = query(client, "2026-12-12", ["A"], ["X"]).get_json()["noisy_count"]
    correct(client, "2026-12-12", 1, CSV_V3)
    n2 = query(client, "2026-12-12", ["B"], ["X"]).get_json()["noisy_count"]
    app_module.init_db()  # 模拟重启
    c2 = app_module.app.test_client()
    st = status(c2, "2026-12-12")
    # 修订号、源数据、预算、结果不得错配
    assert st["current_revision"] == 2 and st["rows"] == 6
    assert st["budget_spent"] == 1.0 and st["locked"]
    assert query_rev(c2, "2026-12-12", 1, ["A"], ["X"]).get_json()["noisy_count"] == n1
    assert query_rev(c2, "2026-12-12", 2, ["B"], ["X"]).get_json()["noisy_count"] == n2
    # 旧版未发布条件重启后仍被拒绝
    assert query_rev(c2, "2026-12-12", 1, ["C"], ["X"]).status_code == 409


def test_interleaved_correct_and_query_single_version(client, monkeypatch):
    """订正与查询交错: 每条成功查询都必须归属某一完整修订.

    噪声置 0 后, 响应的加噪人数必须等于其所属修订源数据的真实人数;
    任何修订号/源数据/结果错配都会体现为人数对不上.
    """
    monkeypatch.setattr(app_module, "laplace_noise", lambda *a, **k: 0.0)
    upload(client, "2026-12-14", CSV_V1)
    query(client, "2026-12-14", ["C"], ["Z"])  # 锁定第 1 版

    conds = [(["A"], ["X"]), (["A"], ["Y"]), (["A"], ["X", "Y"]),
             (["A", "B"], ["X"]), (["B"], ["X"]), (["A"], ["Z"])]
    # 各修订下各条件的真实人数(第 2 版 = CSV_V3, 多出 p6 A→X)
    expected = {1: [2, 1, 3, 3, 1, 0], 2: [3, 1, 4, 4, 1, 0]}
    outcomes = []

    def corrector():
        c = app_module.app.test_client()
        outcomes.append(("correct", correct(c, "2026-12-14", 1, CSV_V3).status_code))

    def querier(i):
        c = app_module.app.test_client()
        ins, outs = conds[i]
        r = query(c, "2026-12-14", ins, outs)
        if r.status_code == 200:
            body = r.get_json()
            outcomes.append(("query", i, body["revision"], body["noisy_count"]))
        else:
            outcomes.append(("query", i, r.status_code, None))

    threads = [threading.Thread(target=corrector)]
    threads += [threading.Thread(target=querier, args=(i,)) for i in range(len(conds))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert ("correct", 200) in outcomes
    successes = [o for o in outcomes if o[0] == "query" and o[3] is not None]
    assert successes, "预算内至少应有部分查询成功"
    for _, i, rev, noisy in successes:
        assert rev in (1, 2)
        assert noisy == expected[rev][i]  # 人数必须匹配其所属修订的源数据
    # 落库结果同样不得错配
    conn = sqlite3.connect(os.environ["APP_DB"])
    rows = conn.execute(
        "SELECT revision, in_key, out_key, noisy_count FROM queries"
        " WHERE date='2026-12-14'").fetchall()
    conn.close()
    import json as _json
    key2idx = {(_json.dumps(conds[i][0]), _json.dumps(conds[i][1])): i
               for i in range(len(conds))}
    for rev, in_key, out_key, noisy in rows:
        if (in_key, out_key) == (_json.dumps(["C"]), _json.dumps(["Z"])):
            assert rev == 1 and noisy == 1.0  # 锁定时的首个查询
        else:
            i = key2idx[(in_key, out_key)]
            assert noisy == expected[rev][i]


def test_migration_from_legacy_schema(tmp_path):
    db = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db)
    conn.executescript("""
    CREATE TABLE passengers (
        date TEXT NOT NULL, passenger_id TEXT NOT NULL,
        in_station TEXT NOT NULL, out_station TEXT NOT NULL,
        PRIMARY KEY (date, passenger_id));
    CREATE TABLE day_state (
        date TEXT PRIMARY KEY, locked INTEGER NOT NULL DEFAULT 0,
        budget_spent REAL NOT NULL DEFAULT 0);
    CREATE TABLE queries (
        date TEXT NOT NULL, in_key TEXT NOT NULL, out_key TEXT NOT NULL,
        in_stations TEXT NOT NULL, out_stations TEXT NOT NULL,
        noisy_count REAL NOT NULL, created_at TEXT NOT NULL,
        PRIMARY KEY (date, in_key, out_key));
    INSERT INTO passengers VALUES ('2026-12-13', 'p1', 'A', 'X');
    INSERT INTO day_state VALUES ('2026-12-13', 1, 0.5);
    INSERT INTO queries VALUES ('2026-12-13', '["A"]', '["X"]', '["A"]', '["X"]',
                                2.5, '2026-12-13T00:00:00+00:00');
    """)
    conn.close()

    app_module.init_db(db)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    # 存量数据与结果全部归第 1 版
    assert conn.execute("SELECT revision FROM passengers").fetchone()["revision"] == 1
    state = conn.execute("SELECT locked, budget_spent, current_revision"
                         " FROM day_state").fetchone()
    assert state["locked"] == 1 and state["budget_spent"] == 0.5
    assert state["current_revision"] == 1
    row = conn.execute("SELECT revision, noisy_count FROM queries").fetchone()
    assert row["revision"] == 1 and row["noisy_count"] == 2.5
    conn.close()
    # 迁移幂等: 再次初始化不报错
    app_module.init_db(db)
