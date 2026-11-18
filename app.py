"""地铁首程客流差分隐私查询服务(整日订正 + 修订管理).

- 按日期上传 CSV(乘客ID, 进站, 出站), 每位乘客仅一行
- 选择进/出站集合查询匹配人数, 结果加 Laplace(0, 2) 噪声(安全随机源)
- 每次新查询扣 0.5 隐私预算, 每日(每个数据日期)总额 2, 跨修订累计、不退还
- 相同 日期 + 修订 + 站点集合(与顺序、重复无关)复用原结果, 不再扣费
- 首个成功查询锁定当天源数据, 之后 /upload 拒绝替换, 只能走整日订正
- 整日订正: 上传完整新 CSV 并携带所见修订号(乐观并发, 同版并发仅接受一次);
  成功后当前修订 +1 并切换源数据, 旧原始行不再参与新查询,
  已发布的带噪结果留在原修订供原值回查; 存量数据与结果归第 1 版
- 查询可携带修订号: 旧修订仅支持已发布条件的原值重查, 未发布过的条件拒绝;
  当前修订的新条件重新加噪并从当日剩余预算扣费
- 扣费 / 保存结果 / 锁定 / 切换修订在同一事务中, 要么全部成功要么全部不生效,
  校验失败、保存失败或重启后不会出现修订号、源数据与结果错配
"""

import csv
import io
import json
import math
import os
import secrets
import sqlite3
from datetime import datetime, timezone

from flask import Flask, g, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("APP_DB", os.path.join(BASE_DIR, "app.db"))

DAILY_BUDGET = 2.0   # 每日隐私预算总额(跨修订累计)
QUERY_COST = 0.5     # 每次新查询的预算开销
NOISE_SCALE = 2.0    # Laplace(0, 2)

app = Flask(__name__)
_rng = secrets.SystemRandom()  # 安全随机源

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS passengers (
        date         TEXT NOT NULL,
        revision     INTEGER NOT NULL,
        passenger_id TEXT NOT NULL,
        in_station   TEXT NOT NULL,
        out_station  TEXT NOT NULL,
        PRIMARY KEY (date, revision, passenger_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS day_state (
        date             TEXT PRIMARY KEY,
        locked           INTEGER NOT NULL DEFAULT 0,
        budget_spent     REAL    NOT NULL DEFAULT 0,
        current_revision INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS queries (
        date         TEXT NOT NULL,
        revision     INTEGER NOT NULL,
        in_key       TEXT NOT NULL,
        out_key      TEXT NOT NULL,
        in_stations  TEXT NOT NULL,
        out_stations TEXT NOT NULL,
        noisy_count  REAL NOT NULL,
        created_at   TEXT NOT NULL,
        PRIMARY KEY (date, revision, in_key, out_key)
    )
    """,
)


def _create_schema(conn):
    for stmt in SCHEMA_STATEMENTS:
        conn.execute(stmt)


def _migrate_if_needed(conn):
    """旧结构(无 revision 列)整体迁移: 存量源数据与已发布结果全部归第 1 版.

    迁移在单一事务内完成, 失败整体回滚, 不会出现半新半旧的结构.
    """
    cols = [r[1] for r in conn.execute("PRAGMA table_info(passengers)")]
    if not cols or "revision" in cols:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("ALTER TABLE passengers RENAME TO passengers_old")
        conn.execute("ALTER TABLE day_state RENAME TO day_state_old")
        conn.execute("ALTER TABLE queries RENAME TO queries_old")
        _create_schema(conn)
        conn.execute(
            "INSERT INTO passengers (date, revision, passenger_id, in_station,"
            " out_station)"
            " SELECT date, 1, passenger_id, in_station, out_station"
            " FROM passengers_old")
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent, current_revision)"
            " SELECT date, locked, budget_spent, 1 FROM day_state_old")
        conn.execute(
            "INSERT INTO queries (date, revision, in_key, out_key, in_stations,"
            " out_stations, noisy_count, created_at)"
            " SELECT date, 1, in_key, out_key, in_stations, out_stations,"
            " noisy_count, created_at FROM queries_old")
        conn.execute("DROP TABLE passengers_old")
        conn.execute("DROP TABLE day_state_old")
        conn.execute("DROP TABLE queries_old")
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def init_db(path=None):
    path = path or DB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        _migrate_if_needed(conn)
        _create_schema(conn)
    finally:
        conn.close()


init_db()


def get_db():
    """每个请求一个连接; autocommit 模式, 事务用显式 BEGIN IMMEDIATE."""
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


# ---------------------------------------------------------------- 工具函数

def laplace_noise(scale=NOISE_SCALE):
    """用安全随机源生成 Laplace(0, scale) 噪声(逆变换采样)."""
    u = _rng.random() - 0.5  # [-0.5, 0.5)
    if u == 0.0:
        return 0.0
    return -scale * math.copysign(1.0, u) * math.log1p(-2.0 * abs(u))


def normalize_date(raw):
    """校验并规范化日期为 YYYY-MM-DD; 非法返回 None."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_revision(raw):
    """解析修订号为正整数; 非法返回 None."""
    if raw is None:
        return None
    try:
        rev = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return rev if rev >= 1 else None


def canon_key(stations):
    """站点集合的规范化表示: 去空白、去重、排序, 与顺序和重复选择无关."""
    return json.dumps(sorted({s.strip() for s in stations if s and s.strip()}),
                      ensure_ascii=False)


def parse_station_list(value):
    """接受 JSON 数组或分隔符(逗号/顿号/分号/空白/换行)分隔的字符串."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        items = [str(v) for v in value]
    else:
        text = str(value)
        for sep in ("，", "、", "；", ";", "\n", "\t", " "):
            text = text.replace(sep, ",")
        items = text.split(",")
    seen, out = set(), []
    for item in items:
        s = item.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


HEADER_ALIASES = {
    "passenger": {"乘客id", "passenger_id", "passengerid", "passenger", "id", "乘客编号"},
    "in": {"进站", "in_station", "instation", "entry", "entry_station", "入站", "进站站点"},
    "out": {"出站", "out_station", "outstation", "exit", "exit_station", "出站站点"},
}


def parse_csv_text(text):
    """解析 CSV 文本, 返回 [(passenger_id, in_station, out_station), ...].

    支持带表头(中/英文列名)或无表头(按 乘客ID,进站,出站 顺序)两种格式.
    乘客ID 重复或字段为空时抛出 ValueError.
    """
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        raise ValueError("CSV 内容为空")

    first = [c.strip().lower() for c in rows[0]]
    col_idx = {}
    for field, aliases in HEADER_ALIASES.items():
        for i, name in enumerate(first):
            if name in aliases:
                col_idx[field] = i
                break

    has_header = len(col_idx) == 3
    if not has_header:
        if col_idx:
            raise ValueError("CSV 表头无法完整识别, 请使用: 乘客ID, 进站, 出站")
        if len(first) < 3:
            raise ValueError("CSV 至少需要 3 列: 乘客ID, 进站, 出站")
        col_idx = {"passenger": 0, "in": 1, "out": 2}

    data_rows = rows[1:] if has_header else rows
    result, seen = [], set()
    for offset, r in enumerate(data_rows):
        lineno = offset + (2 if has_header else 1)
        if max(col_idx.values()) >= len(r):
            raise ValueError(f"第 {lineno} 行列数不足")
        pid = r[col_idx["passenger"]].strip()
        in_s = r[col_idx["in"]].strip()
        out_s = r[col_idx["out"]].strip()
        if not pid or not in_s or not out_s:
            raise ValueError(f"第 {lineno} 行存在空字段")
        if pid in seen:
            raise ValueError(f"乘客ID 重复: {pid}(每位乘客仅允许一行)")
        seen.add(pid)
        result.append((pid, in_s, out_s))
    if not result:
        raise ValueError("CSV 无有效数据行")
    return result


def get_payload():
    if request.is_json:
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            return body
    return request.form


def extract_csv_text(payload):
    """从请求中取出 CSV 文本; 返回 (text, error_response)."""
    file = request.files.get("file")
    if file is not None and file.filename:
        try:
            return file.read().decode("utf-8-sig"), None
        except UnicodeDecodeError:
            return None, (jsonify(ok=False, error="CSV 文件编码必须是 UTF-8"), 400)
    if payload.get("csv"):
        return payload.get("csv"), None
    return None, (jsonify(ok=False, error="请上传 CSV 文件或提供 csv 文本"), 400)


def fetch_state(conn, date):
    return conn.execute(
        "SELECT locked, budget_spent, current_revision FROM day_state"
        " WHERE date = ?", (date,)).fetchone()


# ---------------------------------------------------------------- 页面

@app.get("/")
def index():
    return render_template("index.html",
                           daily_budget=DAILY_BUDGET, query_cost=QUERY_COST)


# ---------------------------------------------------------------- 上传

@app.post("/upload")
def upload():
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    text, err = extract_csv_text(payload)
    if err:
        return err

    try:
        rows = parse_csv_text(text)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400

    conn = get_db()
    conn.execute("BEGIN IMMEDIATE")
    try:
        state = fetch_state(conn, date)
        if state and state["locked"]:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"{date} 的源数据已被首个成功查询锁定,"
                                 " 拒绝替换, 请改用整日订正"), 409
        # 未锁定: 整日替换当前修订
        rev = state["current_revision"] if state else 1
        conn.execute("DELETE FROM passengers WHERE date = ? AND revision = ?",
                     (date, rev))
        conn.executemany(
            "INSERT INTO passengers (date, revision, passenger_id, in_station,"
            " out_station) VALUES (?, ?, ?, ?, ?)",
            [(date, rev, pid, in_s, out_s) for pid, in_s, out_s in rows])
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent, current_revision)"
            " VALUES (?, 0, 0, ?) ON CONFLICT(date) DO NOTHING", (date, rev))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, rows=len(rows), revision=rev,
                   message=f"已替换 {date} 第 {rev} 版源数据, 共 {len(rows)} 行")


# ---------------------------------------------------------------- 整日订正

@app.post("/correct")
def correct():
    """整日订正: 上传完整新 CSV 并携带所见修订号, 成功后当前修订 +1.

    乐观并发控制: 所见修订号与当前修订不一致(含同版并发订正)即拒绝;
    CSV 校验失败不触碰数据库; 新修订行写入与修订号切换在同一事务中.
    """
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    seen_rev = parse_revision(payload.get("revision"))
    if seen_rev is None:
        return jsonify(ok=False, error="请携带所见修订号(正整数)"), 400

    text, err = extract_csv_text(payload)
    if err:
        return err

    try:
        rows = parse_csv_text(text)  # 校验失败: 不写库, 修订号不变
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400

    conn = get_db()
    conn.execute("BEGIN IMMEDIATE")
    try:
        state = fetch_state(conn, date)
        cur = state["current_revision"] if state else 1
        has_data = conn.execute(
            "SELECT 1 FROM passengers WHERE date = ? AND revision = ? LIMIT 1",
            (date, cur)).fetchone()
        if not has_data:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"{date} 尚无源数据, 请先上传首日 CSV"), 409
        if seen_rev != cur:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"修订号已过期: 所见为第 {seen_rev} 版,"
                                 f" 当前已是第 {cur} 版, 请刷新后重试"), 409
        new_rev = cur + 1
        conn.executemany(
            "INSERT INTO passengers (date, revision, passenger_id, in_station,"
            " out_station) VALUES (?, ?, ?, ?, ?)",
            [(date, new_rev, pid, in_s, out_s) for pid, in_s, out_s in rows])
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent, current_revision)"
            " VALUES (?, 0, 0, ?)"
            " ON CONFLICT(date) DO UPDATE"
            " SET current_revision = excluded.current_revision", (date, new_rev))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, revision=new_rev, rows=len(rows),
                   message=f"已生成 {date} 第 {new_rev} 版源数据(共 {len(rows)} 行),"
                           " 旧版原始行不再参与新查询, 已发布结果保留在原修订供回查")


# ---------------------------------------------------------------- 查询

@app.post("/query")
def query():
    payload = get_payload()
    date = normalize_date(payload.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确(应为 YYYY-MM-DD)"), 400

    in_stations = parse_station_list(payload.get("in_stations"))
    out_stations = parse_station_list(payload.get("out_stations"))
    if not in_stations or not out_stations:
        return jsonify(ok=False, error="进站集合与出站集合均不能为空"), 400

    rev_raw = payload.get("revision")
    requested = None
    if rev_raw is not None and str(rev_raw).strip() != "":
        requested = parse_revision(rev_raw)
        if requested is None:
            return jsonify(ok=False, error="修订号必须是正整数"), 400

    in_key, out_key = canon_key(in_stations), canon_key(out_stations)
    conn = get_db()

    # 整个查询(缓存检查、计费、写结果)在同一写事务中, 与并发订正严格串行,
    # 成功查询只可能归属某一完整修订, 不会出现修订号/源数据/结果错配
    conn.execute("BEGIN IMMEDIATE")
    try:
        state = fetch_state(conn, date)
        cur = state["current_revision"] if state else 1
        spent = state["budget_spent"] if state else 0.0
        rev = requested if requested is not None else cur
        if rev > cur:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"第 {rev} 版不存在, {date} 当前修订为第 {cur} 版"), 409

        cached = conn.execute(
            "SELECT noisy_count, in_stations, out_stations FROM queries"
            " WHERE date = ? AND revision = ? AND in_key = ? AND out_key = ?",
            (date, rev, in_key, out_key)).fetchone()
        if cached:
            conn.execute("COMMIT")
            return jsonify(ok=True, date=date, revision=rev, current_revision=cur,
                           in_stations=json.loads(cached["in_stations"]),
                           out_stations=json.loads(cached["out_stations"]),
                           noisy_count=cached["noisy_count"], cached=True,
                           budget_remaining=round(DAILY_BUDGET - spent, 10))

        if rev < cur:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"第 {rev} 版未发布过该条件的查询结果;"
                                 " 历史修订仅支持已发布结果的原值重查, 拒绝新查询"), 409

        # 当前修订的新条件: 重新加噪, 从当日剩余预算扣费(跨修订累计, 不退还)
        if spent + QUERY_COST > DAILY_BUDGET + 1e-9:
            conn.execute("ROLLBACK")
            return jsonify(ok=False,
                           error=f"{date} 当日隐私预算不足"
                                 f"(已用 {spent:g}/{DAILY_BUDGET:g}, 本次需 {QUERY_COST:g}),"
                                 " 不返回新结果"), 403

        placeholders_in = ",".join("?" for _ in in_stations)
        placeholders_out = ",".join("?" for _ in out_stations)
        true_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM passengers"
            f" WHERE date = ? AND revision = ? AND in_station IN ({placeholders_in})"
            f" AND out_station IN ({placeholders_out})",
            [date, rev, *in_stations, *out_stations]).fetchone()["c"]

        noisy_count = true_count + laplace_noise()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO queries (date, revision, in_key, out_key, in_stations,"
            " out_stations, noisy_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (date, rev, in_key, out_key, json.dumps(in_stations, ensure_ascii=False),
             json.dumps(out_stations, ensure_ascii=False), noisy_count, now))
        conn.execute(
            "INSERT INTO day_state (date, locked, budget_spent, current_revision)"
            " VALUES (?, 1, ?, ?)"
            " ON CONFLICT(date) DO UPDATE SET locked = 1,"
            " budget_spent = budget_spent + excluded.budget_spent",
            (date, QUERY_COST, cur))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    return jsonify(ok=True, date=date, revision=rev, current_revision=cur,
                   in_stations=in_stations, out_stations=out_stations,
                   noisy_count=noisy_count, cached=False,
                   budget_remaining=round(DAILY_BUDGET - spent - QUERY_COST, 10))


# ---------------------------------------------------------------- 状态接口

@app.get("/api/stations")
def api_stations():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    state = fetch_state(conn, date)
    cur = state["current_revision"] if state else 1
    rows = conn.execute(
        "SELECT in_station AS s FROM passengers WHERE date = ? AND revision = ?"
        " UNION SELECT out_station FROM passengers WHERE date = ? AND revision = ?"
        " ORDER BY s", (date, cur, date, cur)).fetchall()
    return jsonify(ok=True, date=date, revision=cur, stations=[r["s"] for r in rows])


@app.get("/api/status")
def api_status():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    state = fetch_state(conn, date)
    cur = state["current_revision"] if state else 1
    rows = conn.execute(
        "SELECT COUNT(*) AS c FROM passengers WHERE date = ? AND revision = ?",
        (date, cur)).fetchone()["c"]
    spent = state["budget_spent"] if state else 0.0
    return jsonify(ok=True, date=date,
                   locked=bool(state and state["locked"]),
                   rows=rows,
                   current_revision=cur,
                   budget_spent=spent,
                   budget_remaining=round(DAILY_BUDGET - spent, 10),
                   daily_budget=DAILY_BUDGET, query_cost=QUERY_COST)


@app.get("/api/queries")
def api_queries():
    date = normalize_date(request.args.get("date", ""))
    if not date:
        return jsonify(ok=False, error="日期缺失或格式不正确"), 400
    conn = get_db()
    state = fetch_state(conn, date)
    cur = state["current_revision"] if state else 1
    rows = conn.execute(
        "SELECT revision, in_stations, out_stations, noisy_count, created_at"
        " FROM queries WHERE date = ? ORDER BY revision, created_at, rowid",
        (date,)).fetchall()
    return jsonify(ok=True, date=date, current_revision=cur, queries=[
        {"revision": r["revision"],
         "in_stations": json.loads(r["in_stations"]),
         "out_stations": json.loads(r["out_stations"]),
         "noisy_count": r["noisy_count"],
         "created_at": r["created_at"]} for r in rows])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
