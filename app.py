"""Target Rush leaderboard API.

Postgres is the source of truth. Redis sorted sets serve the three boards
(daily, weekly, all-time). Redis pub/sub + Server-Sent Events push changes to
every open browser, whichever API pod they are connected to. Per-second
metrics are aggregated in Redis so the /dashboard page shows the whole
cluster, not just one pod.
"""
import asyncio
import json
import os
import re
import secrets
import threading
import time
from bisect import bisect_left
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import redis
import redis.asyncio as aioredis
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from psycopg2.pool import ThreadedConnectionPool

# ---------------------------------------------------------------- config
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("CACHE_PORT", "6379"))   # not REDIS_PORT: Kubernetes injects that as "tcp://ip:port"
PG_HOST = os.getenv("PG_HOST", "postgresql")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
DB_POOL = int(os.getenv("DB_POOL", "15"))          # per pod; keep pods x pool < Postgres max_connections (100)
POD = os.getenv("HOSTNAME", "local")

LOADTEST_KEY = os.getenv("LOADTEST_KEY", "")       # empty = load-test bypass disabled
MAX_SCORE = int(os.getenv("MAX_SCORE", "8000"))
MIN_GAME_SECONDS = float(os.getenv("MIN_GAME_SECONDS", "28"))   # game lasts 30 s
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))

NAME_RE = re.compile(r"^[A-Za-z0-9_-]{2,20}$")
BOARDS = ("daily", "weekly", "all")
CHANNEL = "lb_updates"
METRIC_TTL = 900                                   # keep 15 minutes of per-second metrics
BOUNDS_MS = [2, 5, 10, 20, 35, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2000, 5000]
STATIC = Path(__file__).parent / "static"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                socket_connect_timeout=1, socket_timeout=2)
ar = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=1, socket_timeout=2)

# ---------------------------------------------------------------- prometheus
HTTP_REQS = Counter("lb_http_requests_total", "HTTP requests", ["method", "path", "status"])
HTTP_LAT = Histogram("lb_http_request_seconds", "Request latency", ["path"],
                     buckets=[b / 1000 for b in BOUNDS_MS])
BOARD_READS = Counter("lb_board_reads_total", "Board reads by data source", ["source"])
SCORES_SAVED = Counter("lb_scores_saved_total", "Scores accepted")
SSE_CLIENTS = Gauge("lb_sse_clients", "Open live-update connections on this pod")

# ---------------------------------------------------------------- postgres
pool = None
pool_sem = threading.BoundedSemaphore(DB_POOL)     # wait for a connection instead of erroring


def run(sql, args=(), fetch=False):
    with pool_sem:
        conn = pool.getconn()
        try:
            with conn.cursor() as c:
                c.execute(sql, args)
                rows = c.fetchall() if fetch else None
            conn.commit()
            return rows
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)


def init_db():
    global pool
    for _ in range(60):
        try:
            if pool is None:
                pool = ThreadedConnectionPool(
                    1, DB_POOL, host=PG_HOST, port=PG_PORT, dbname=os.getenv("PG_DB"),
                    user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))
            run("CREATE TABLE IF NOT EXISTS scores (id SERIAL PRIMARY KEY, player TEXT, "
                "score INT, created_at TIMESTAMP DEFAULT now())")
            run("CREATE INDEX IF NOT EXISTS idx_player ON scores(player)")
            run("CREATE INDEX IF NOT EXISTS idx_created ON scores(created_at)")
            return
        except Exception as exc:
            print(f"waiting for database: {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("database never became ready")


# ---------------------------------------------------------------- boards
def board_spec(board):
    """Return (redis key, period start as naive UTC, ttl seconds) for a board."""
    now = datetime.now(timezone.utc)
    if board == "daily":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return f"lb:daily:{start:%Y%m%d}", start.replace(tzinfo=None), 3 * 86400
    if board == "weekly":
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = day - timedelta(days=day.weekday())            # Monday 00:00 UTC
        year, week, _ = start.isocalendar()
        return f"lb:weekly:{year}W{week:02d}", start.replace(tzinfo=None), 10 * 86400
    if board == "all":
        return "lb:all", datetime(1970, 1, 1), None
    raise HTTPException(404, "board must be daily, weekly or all")


def db_top(since, limit=10):
    rows = run("SELECT player, MAX(score) AS s FROM scores WHERE created_at >= %s "
               "GROUP BY player ORDER BY s DESC, player LIMIT %s", (since, limit), fetch=True)
    return [{"player": p, "score": int(s)} for p, s in rows]


def ensure_cached(key, since, ttl):
    """True if Redis already had this board; False if we rebuilt it from Postgres."""
    if r.exists(key):
        return True
    marker = f"built:{key}"           # lets a genuinely empty board stay empty without hitting Postgres
    if r.get(marker):
        return True
    rows = run("SELECT player, MAX(score) FROM scores WHERE created_at >= %s GROUP BY player",
               (since,), fetch=True)
    if rows:
        r.zadd(key, {p: int(s) for p, s in rows}, gt=True)
        if ttl:
            r.expire(key, ttl)
    if ttl:
        r.set(marker, 1, ex=ttl)
    else:
        r.set(marker, 1)
    return False


def read_board(board, nocache=False):
    """Return (rows, source) where source is redis | rebuild | postgres."""
    key, since, ttl = board_spec(board)
    if nocache:
        return db_top(since), "postgres"
    try:
        cached = ensure_cached(key, since, ttl)
        data = r.zrevrange(key, 0, 9, withscores=True)
        return [{"player": p, "score": int(s)} for p, s in data], ("redis" if cached else "rebuild")
    except redis.RedisError:           # cache down: degrade to Postgres instead of failing
        return db_top(since), "postgres"


_RANK = {"redis": 0, "rebuild": 1, "postgres": 2}


def get_boards(nocache=False):
    out, worst = {}, "redis"
    for b in BOARDS:
        rows, src = read_board(b, nocache)
        out[b] = rows
        if _RANK[src] > _RANK[worst]:
            worst = src
    return out, worst


# ---------------------------------------------------------------- metrics
async def heartbeat():
    """Each pod stamps the current second so the dashboard can count live pods."""
    while True:
        try:
            sec = f"m:{int(time.time())}"
            pipe = ar.pipeline(transaction=False)
            pipe.hset(sec, f"p:{POD}", 1)
            pipe.expire(sec, METRIC_TTL)
            await pipe.execute()
        except Exception:
            pass
        await asyncio.sleep(2)


async def record(label, method, status, ms, source):
    HTTP_REQS.labels(method, label, str(status)).inc()
    HTTP_LAT.labels(label).observe(ms / 1000)
    if source:
        BOARD_READS.labels(source).inc()
    try:
        sec = f"m:{int(time.time())}"
        pipe = ar.pipeline(transaction=False)
        pipe.hincrby(sec, "n", 1)
        pipe.hincrbyfloat(sec, "lat", ms)
        pipe.hincrby(sec, f"b{bisect_left(BOUNDS_MS, ms)}", 1)
        if status >= 500:
            pipe.hincrby(sec, "err", 1)
        if source:
            pipe.hincrby(sec, {"redis": "hit", "rebuild": "miss", "postgres": "db"}[source], 1)
        if label == "/score" and method == "POST" and status == 200:
            pipe.hincrby(sec, "sc", 1)
        pipe.expire(sec, METRIC_TTL)
        await pipe.execute()
    except Exception:
        pass


def percentile(counts, q):
    total = sum(counts)
    if not total:
        return None
    target, cum = q * total, 0
    for i, c in enumerate(counts):
        cum += c
        if cum >= target:
            return BOUNDS_MS[min(i, len(BOUNDS_MS) - 1)]
    return BOUNDS_MS[-1]


# ---------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(app):
    await asyncio.to_thread(init_db)
    task = asyncio.create_task(heartbeat())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)

_SKIP_EXACT = {"/", "/dashboard"}
_SKIP_PREFIX = ("/health", "/metrics", "/api/metrics", "/stream", "/favicon")


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    path = request.url.path
    if path in _SKIP_EXACT or path.startswith(_SKIP_PREFIX):
        return await call_next(request)
    t0 = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        ms = (time.perf_counter() - t0) * 1000
        route = request.scope.get("route")
        label = getattr(route, "path", None) or "unmatched"
        await record(label, request.method, status, ms, getattr(request.state, "source", None))


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/dashboard")
def dashboard():
    return FileResponse(STATIC / "dashboard.html")


@app.get("/health")
def health():
    return {"status": "ok", "pod": POD}


# ---- game + security -------------------------------------------------------
def check_name(player):
    if not NAME_RE.match(player):
        raise HTTPException(400, "Name must be 2-20 characters: letters, numbers, _ or -")


@app.post("/game/start")
def game_start(player: str):
    """Issue a single-use token. /score only accepts it after a full game has elapsed."""
    check_name(player)
    token = secrets.token_urlsafe(16)
    try:
        r.set(f"tok:{token}", f"{player}|{time.time()}", ex=180)
    except redis.RedisError:
        raise HTTPException(503, "Game service is restarting, try again in a moment")
    return {"token": token}


def verify_token(player, token):
    if not token:
        raise HTTPException(403, "Missing game token")
    try:
        val = r.getdel(f"tok:{token}")
    except redis.RedisError:
        raise HTTPException(503, "Game service is restarting, try again in a moment")
    if not val:
        raise HTTPException(403, "Game token is invalid, expired or already used")
    owner, started = val.split("|")
    if owner != player:
        raise HTTPException(403, "Game token belongs to another player")
    if time.time() - float(started) < MIN_GAME_SECONDS:
        raise HTTPException(403, "Score submitted before the game could have finished")


def check_rate_limit(player):
    try:
        key = f"rl:{player}"
        n = r.incr(key)
        if n == 1:
            r.expire(key, 60)
        if n > RATE_LIMIT_PER_MIN:
            raise HTTPException(429, "Too many submissions, slow down")
    except redis.RedisError:
        pass


@app.post("/score")
def submit(player: str, score: int, token: Optional[str] = None,
           x_load_test_key: Optional[str] = Header(None)):
    check_name(player)
    if not 0 <= score <= MAX_SCORE:
        raise HTTPException(400, f"Score must be between 0 and {MAX_SCORE}")
    trusted = bool(LOADTEST_KEY) and secrets.compare_digest(
        (x_load_test_key or "").encode(), LOADTEST_KEY.encode())
    if not trusted:                    # real players: rate limit + single-use game token
        check_rate_limit(player)
        verify_token(player, token)

    # 1. durable write first
    ts = datetime.now(timezone.utc).replace(tzinfo=None)
    run("INSERT INTO scores(player, score, created_at) VALUES (%s, %s, %s)", (player, score, ts))
    SCORES_SAVED.inc()

    # 2. update the boards and tell every open browser
    results = {}
    try:
        specs = [board_spec(b) for b in BOARDS]
        if r.exists(*[s[0] for s in specs]) != len(specs):      # a board was lost (e.g. Redis restart)
            for key, since, ttl in specs:
                ensure_cached(key, since, ttl)
        pipe = r.pipeline()
        for key, _, _ in specs:
            pipe.zadd(key, {player: score}, gt=True)
            pipe.zrevrank(key, player)
            pipe.zscore(key, player)
        for key, _, ttl in specs:
            if ttl:
                pipe.expire(key, ttl)
        pipe.publish(CHANNEL, player)
        res = pipe.execute()
        for i, b in enumerate(BOARDS):
            rank, best = res[3 * i + 1], res[3 * i + 2]
            if rank is not None:
                results[b] = {"rank": rank + 1, "best": int(best)}
    except redis.RedisError:
        pass                           # score is safe in Postgres; boards rebuild on next read
    return {"ok": True, "results": results}


# ---- reading boards --------------------------------------------------------
@app.get("/top10")
def top10(request: Request, board: str = "all", nocache: int = 0):
    rows, source = read_board(board, bool(nocache))
    request.state.source = source
    return rows


@app.get("/boards")
def boards(request: Request, nocache: int = 0):
    data, source = get_boards(bool(nocache))
    request.state.source = source
    return data


@app.get("/rank/{player}")
def rank(player: str, board: str = "all"):
    key, since, ttl = board_spec(board)
    try:
        ensure_cached(key, since, ttl)
        pos = r.zrevrank(key, player)
        if pos is None:
            raise HTTPException(404, "player not on this board")
        return {"player": player, "board": board, "rank": pos + 1, "score": int(r.zscore(key, player))}
    except redis.RedisError:
        raise HTTPException(503, "ranking unavailable while the cache restarts")


@app.get("/stream")
async def stream(request: Request):
    """Server-Sent Events: a fresh snapshot of all boards whenever any pod saves a score."""
    async def events():
        pubsub = ar.pubsub()
        await pubsub.subscribe(CHANNEL)
        SSE_CLIENTS.inc()
        try:
            dirty, last_sent, last_beat = True, 0.0, time.time()
            while True:
                if await request.is_disconnected():
                    break
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.25)
                if msg:
                    dirty = True
                now = time.time()
                if dirty and now - last_sent >= 0.5:           # at most 2 pushes per second
                    data, _ = await asyncio.to_thread(get_boards)
                    yield f"event: boards\ndata: {json.dumps(data)}\n\n"
                    dirty, last_sent, last_beat = False, now, now
                elif now - last_beat >= 15:                    # keep the router from closing idle streams
                    yield ": keepalive\n\n"
                    last_beat = now
        except Exception:
            pass
        finally:
            SSE_CLIENTS.dec()
            try:
                await pubsub.unsubscribe(CHANNEL)
                await (pubsub.aclose() if hasattr(pubsub, "aclose") else pubsub.close())
            except Exception:
                pass

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---- metrics ---------------------------------------------------------------
@app.get("/metrics")
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/metrics")
async def api_metrics(window: int = 300, step: int = 5):
    """Cluster-wide time series, aggregated from per-second Redis hashes."""
    window, step = max(30, min(window, 900)), max(1, min(step, 60))
    nb = window // step
    end = int(time.time()) - 1                       # last fully elapsed second
    start = end - nb * step + 1
    pipe = ar.pipeline(transaction=False)
    for s in range(start, end + 1):
        pipe.hgetall(f"m:{s}")
    rows = await pipe.execute()

    nclasses = len(BOUNDS_MS) + 1
    buckets = [{"n": 0, "err": 0, "lat": 0.0, "c": [0] * nclasses, "hit": 0, "miss": 0,
                "db": 0, "sc": 0, "pods": set()} for _ in range(nb)]
    for offset, h in enumerate(rows):
        if not h:
            continue
        b = buckets[offset // step]
        for k, v in h.items():
            if k in ("n", "err", "hit", "miss", "db", "sc"):
                b[k] += int(v)
            elif k == "lat":
                b["lat"] += float(v)
            elif k.startswith("p:"):
                b["pods"].add(k)
            elif k[0] == "b" and k[1:].isdigit():
                b["c"][int(k[1:])] += int(v)

    out = {k: [] for k in ("t", "rps", "err_ps", "avg", "p50", "p95", "hit_pct", "scores_ps", "pods")}
    for i, b in enumerate(buckets):
        reads = b["hit"] + b["miss"] + b["db"]
        out["t"].append((start + (i + 1) * step - 1) * 1000)
        out["rps"].append(round(b["n"] / step, 2))
        out["err_ps"].append(round(b["err"] / step, 2))
        out["avg"].append(round(b["lat"] / b["n"], 1) if b["n"] else None)
        out["p50"].append(percentile(b["c"], 0.50))
        out["p95"].append(percentile(b["c"], 0.95))
        out["hit_pct"].append(round(100 * b["hit"] / reads, 1) if reads else None)
        out["scores_ps"].append(round(b["sc"] / step, 2))
        out["pods"].append(len(b["pods"]))
    try:
        out["players"] = await ar.zcard("lb:all")
    except Exception:
        out["players"] = None
    out["step"] = step
    return out
