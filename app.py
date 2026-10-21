"""Target Rush leaderboard API.

Postgres is the source of truth. Redis sorted sets serve the three boards
(daily, weekly, all-time). Redis pub/sub + Server-Sent Events push changes to
every open browser, whichever API pod they are connected to. Per-second
metrics are aggregated in Redis so the /dashboard page shows the whole
cluster, not just one pod.
"""
import asyncio
import json
import math
import os
import re
import secrets
import statistics
import threading
import time
from bisect import bisect_left
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3
import redis
import redis.asyncio as aioredis
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
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

# Replay store: S3-compatible object storage (RustFS on OpenShift, S3 on AWS).
# Without S3_KEY and S3_SECRET, replays fall back to a Postgres table.
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://replay-store:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "replays")
S3_KEY = os.getenv("S3_KEY", "")
S3_SECRET = os.getenv("S3_SECRET", "")
STORE_MODE = "s3" if S3_KEY and S3_SECRET else "postgres"

GAME_MS = 30000                                   # length of a round
MIN_REACTION_MS = int(os.getenv("MIN_REACTION_MS", "150"))   # faster than this is not human
MAX_EVENTS = 500

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
REPLAYS_SAVED = Counter("lb_replays_saved_total", "Round replays stored")
REJECTS = Counter("lb_replay_rejects_total", "Rounds refused by the replay checks", ["reason"])

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
            run("ALTER TABLE scores ADD COLUMN IF NOT EXISTS round_id TEXT")
            run("CREATE TABLE IF NOT EXISTS replays (id SERIAL PRIMARY KEY, player TEXT NOT NULL, "
                "round_id TEXT NOT NULL, ts_ms BIGINT NOT NULL, score INT NOT NULL, body JSONB NOT NULL)")
            run("CREATE INDEX IF NOT EXISTS idx_replays_player ON replays(player, ts_ms)")
            return
        except Exception as exc:
            print(f"waiting for database: {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError("database never became ready")


# ---------------------------------------------------------------- replays
# One JSON file per try, grouped by player:  replays/<player>/<time>_<score>_<round>.json
# Counting a player's files therefore gives their number of tries.
store_ready = STORE_MODE == "postgres"
_stopping = False
s3 = None


def get_s3():
    global s3
    if s3 is None:
        s3 = boto3.client(
            "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          connect_timeout=2, read_timeout=5, retries={"max_attempts": 2}))
    return s3


def init_store():
    """Create the bucket once the object store answers. Runs in the background."""
    global store_ready
    if STORE_MODE != "s3":
        return
    while not _stopping:
        try:
            client = get_s3()
            try:
                client.head_bucket(Bucket=S3_BUCKET)
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                    raise
                client.create_bucket(Bucket=S3_BUCKET)
            store_ready = True
            print("replay store ready", flush=True)
            return
        except Exception as exc:
            print(f"waiting for replay store: {exc}", flush=True)
            time.sleep(5)


def store_put(player, rid, ts_ms, score, doc):
    if STORE_MODE == "s3":
        key = f"replays/{player}/{ts_ms:013d}_{score:05d}_{rid}.json"
        get_s3().put_object(Bucket=S3_BUCKET, Key=key,
                            Body=json.dumps(doc, separators=(",", ":")).encode(),
                            ContentType="application/json")
    else:
        run("INSERT INTO replays(player, round_id, ts_ms, score, body) VALUES (%s, %s, %s, %s, %s)",
            (player, rid, ts_ms, score, json.dumps(doc)))


def _parse_key(key):
    name = key.rsplit("/", 1)[-1]
    if not name.endswith(".json"):
        return None
    parts = name[:-5].split("_")
    if len(parts) != 3 or not (parts[0].isdigit() and parts[1].isdigit()):
        return None
    return {"t": int(parts[0]), "score": int(parts[1]), "id": parts[2], "key": key}


def store_list(player):
    """Every try this player has saved, oldest first."""
    if STORE_MODE == "s3":
        items = []
        pages = get_s3().get_paginator("list_objects_v2").paginate(
            Bucket=S3_BUCKET, Prefix=f"replays/{player}/")
        for page in pages:
            for obj in page.get("Contents", []):
                item = _parse_key(obj["Key"])
                if item:
                    items.append(item)
        return sorted(items, key=lambda i: (i["t"], i["id"]))
    rows = run("SELECT round_id, ts_ms, score FROM replays WHERE player = %s ORDER BY ts_ms, round_id",
               (player,), fetch=True)
    return [{"t": int(t), "score": int(s), "id": rid} for rid, t, s in rows]


def store_get(player, item):
    if STORE_MODE == "s3":
        return json.loads(get_s3().get_object(Bucket=S3_BUCKET, Key=item["key"])["Body"].read())
    rows = run("SELECT body FROM replays WHERE player = %s AND round_id = %s",
               (player, item["id"]), fetch=True)
    if not rows:
        raise KeyError(item["id"])
    body = rows[0][0]
    return body if isinstance(body, dict) else json.loads(body)


def store_count_all():
    """Tries for every player, counted from the stored files (or rows)."""
    counts = {}
    if STORE_MODE == "s3":
        pages = get_s3().get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="replays/")
        for page in pages:
            for obj in page.get("Contents", []):
                parts = obj["Key"].split("/")
                if len(parts) == 3 and _parse_key(obj["Key"]):
                    counts[parts[1]] = counts.get(parts[1], 0) + 1
        return counts
    for player, n in run("SELECT player, COUNT(*) FROM replays GROUP BY player", fetch=True):
        counts[player] = int(n)
    return counts


def ensure_tries():
    """Tries per player sit in a Redis hash, rebuilt from the stored replays if it is lost."""
    if r.exists("tries") or r.get("built:tries"):
        return
    if r.get("tries:skip"):
        raise RuntimeError("replay store recently unavailable")
    try:
        counts = store_count_all()
    except Exception:
        r.set("tries:skip", 1, ex=30)
        raise
    if counts:
        r.hset("tries", mapping=counts)
    r.set("built:tries", 1)


def attach_tries(boards_out):
    names = sorted({row["player"] for rows in boards_out.values() for row in rows})
    tmap = {}
    if names:
        try:
            ensure_tries()
            tmap = {n: int(v) for n, v in zip(names, r.hmget("tries", names)) if v}
        except Exception:
            pass
    for rows in boards_out.values():
        for row in rows:
            row["tries"] = tmap.get(row["player"], 0)


# ---- replay verification: the server replays the events through the game rules ----
def reject(reason, message):
    REJECTS.labels(reason).inc()
    raise HTTPException(400, message)


def verify_replay(body, elapsed_ms):
    """Re-run a round from its recorded events and return (score, events, stats).

    The client's own score is never trusted: it must match what these rules produce.
    Event kinds: s = a target appears, h = it is hit, e = it expires, m = click on empty arena.
    """
    if not isinstance(body, dict) or body.get("v") != 1 or not isinstance(body.get("events"), list):
        reject("format", "The replay is missing or in the wrong format")
    events = body["events"]
    if not 1 <= len(events) <= MAX_EVENTS:
        reject("format", "The replay has too few or too many events")

    score = hits = combo = 0
    active = None                     # (spawn time, life) of the target on screen
    last_t = 0
    last_resolve = None
    reactions, out = [], []
    for ev in events:
        if not isinstance(ev, dict):
            reject("format", "The replay is in the wrong format")
        kind, t = ev.get("k"), ev.get("t")
        if kind not in ("s", "h", "e", "m") or not isinstance(t, int) or isinstance(t, bool):
            reject("format", "The replay is in the wrong format")
        if t < last_t or t > GAME_MS + 500:
            reject("timing", "The replay timeline is out of order or longer than a round")
        last_t = t

        if kind == "s":
            if active:
                reject("rules", "A target appeared while another was still on screen")
            life, x, y = ev.get("life"), ev.get("x"), ev.get("y")
            if life != max(650, 1500 - hits * 10):
                reject("rules", "Target timing does not follow the game rules")
            if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and 0 <= v <= 1 for v in (x, y)):
                reject("format", "A target position is out of range")
            earliest = 340 if last_resolve is None else last_resolve + 140 - 20
            if t < earliest:
                reject("timing", "Targets appeared faster than the game allows")
            active = (t, life)
            out.append({"k": "s", "t": t, "x": round(float(x), 4), "y": round(float(y), 4), "life": life})
        elif kind == "h":
            if not active:
                reject("rules", "A hit was recorded with no target on screen")
            spawn, life = active
            dt = t - spawn
            if dt < MIN_REACTION_MS:
                reject("reaction", "A reaction time was faster than a human can manage")
            if dt > life + 30:
                reject("timing", "A hit was recorded after its target had expired")
            fraction = max(0.0, 1 - dt / life)
            multiplier = min(1 + combo // 3, 5)
            points = (10 + math.floor(10 * fraction + 0.5)) * multiplier
            score += points
            combo += 1
            hits += 1
            reactions.append(dt)
            active, last_resolve = None, t
            out.append({"k": "h", "t": t, "p": points})
        elif kind == "e":
            if not active:
                reject("rules", "An expiry was recorded with no target on screen")
            spawn, life = active
            if t < spawn + life - 20:
                reject("timing", "A target expired earlier than the game allows")
            combo = 0
            active, last_resolve = None, t
            out.append({"k": "e", "t": t})
        else:                                           # "m": clicked the empty arena
            combo = 0
            out.append({"k": "m", "t": t})

    if last_t > elapsed_ms + 750:
        reject("timing", "The replay covers more time than the round actually took")
    if hits >= 12 and statistics.pstdev(reactions) < 10:
        reject("regular", "The reaction times are too regular to be human")
    if body.get("score") != score:
        reject("score_mismatch", "The score does not match the replay")
    if score > MAX_SCORE:
        reject("rules", f"Score must be between 0 and {MAX_SCORE}")
    stats = {"hits": hits, "misses": sum(1 for e in out if e["k"] in ("e", "m")),
             "avg_reaction_ms": round(statistics.mean(reactions)) if reactions else None}
    return score, out, stats


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
    attach_tries(out)                  # how many tries (stored replays) each player has
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
    global _stopping
    await asyncio.to_thread(init_db)
    store_task = asyncio.create_task(asyncio.to_thread(init_store))   # never blocks start-up
    beat = asyncio.create_task(heartbeat())
    yield
    _stopping = True
    beat.cancel()
    store_task.cancel()


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
    return {"status": "ok", "pod": POD, "replay_store": STORE_MODE, "replay_store_ready": store_ready}


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
    elapsed = time.time() - float(started)
    if elapsed < MIN_GAME_SECONDS:
        raise HTTPException(403, "Score submitted before the game could have finished")
    return elapsed * 1000


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
def submit(player: str, score: Optional[int] = None, token: Optional[str] = None,
           replay: Optional[dict] = Body(None), x_load_test_key: Optional[str] = Header(None)):
    check_name(player)
    trusted = bool(LOADTEST_KEY) and secrets.compare_digest(
        (x_load_test_key or "").encode(), LOADTEST_KEY.encode())
    now = datetime.now(timezone.utc)
    ts_ms, ts = int(now.timestamp() * 1000), now.replace(tzinfo=None)
    round_id = None

    if trusted:                        # our own load-test scripts: no game is played, so no replay
        if score is None or not 0 <= score <= MAX_SCORE:
            raise HTTPException(400, f"Score must be between 0 and {MAX_SCORE}")
    else:                              # real players: rate limit, token, then the replay decides the score
        check_rate_limit(player)
        elapsed_ms = verify_token(player, token)
        if replay is None:
            reject("no_replay", "A replay of the round is required with every score")
        score, events, stats = verify_replay(replay, elapsed_ms)
        if not store_ready:
            raise HTTPException(503, "Replay storage is starting, try again in a moment")
        round_id = secrets.token_hex(6)
        doc = {"v": 1, "round_id": round_id, "player": player, "score": score,
               "saved_at": now.isoformat().replace("+00:00", "Z"), "stats": stats, "events": events}
        try:
            store_put(player, round_id, ts_ms, score, doc)     # the replay is saved before the score
        except Exception:
            raise HTTPException(503, "Replay storage is unavailable, so this round was not saved")
        REPLAYS_SAVED.inc()

    # 1. durable write
    run("INSERT INTO scores(player, score, created_at, round_id) VALUES (%s, %s, %s, %s)",
        (player, score, ts, round_id))
    SCORES_SAVED.inc()

    # 2. how many tries this player has, counted from their stored replays.
    #    This must be updated before the boards are published, or viewers get a stale count.
    tries = None
    if round_id:
        try:
            tries = len(store_list(player))
            ensure_tries()
            r.hset("tries", player, tries)
        except Exception:
            pass

    # 3. update the boards and tell every open browser
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

    return {"ok": True, "score": score, "round_id": round_id, "tries": tries, "results": results}


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


# ---- tries and replays ------------------------------------------------------
@app.get("/api/player/{player}")
def api_player(player: str, limit: int = 20):
    """One player's tries, counted from their stored replay files."""
    check_name(player)
    try:
        items = store_list(player)
    except Exception:
        raise HTTPException(503, "Replay storage is unavailable")
    recent = sorted(items, key=lambda i: i["t"], reverse=True)[:max(1, min(limit, 100))]
    return {"player": player, "tries": len(items),
            "best": max((i["score"] for i in items), default=None),
            "rounds": [{"id": i["id"], "time": i["t"], "score": i["score"]} for i in recent]}


@app.get("/api/tries")
def api_tries(limit: int = 100):
    """Number of tries for every player, most active first."""
    try:
        ensure_tries()
        counts = {p: int(n) for p, n in r.hgetall("tries").items()}
    except Exception:
        try:
            counts = store_count_all()
        except Exception:
            raise HTTPException(503, "Replay storage is unavailable")
    rows = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:max(1, min(limit, 500))]
    return [{"player": p, "tries": n} for p, n in rows]


@app.get("/api/replay/{player}")
def api_replay(player: str, board: str = "all", round_id: Optional[str] = Query(None, alias="round")):
    """A player's best round on a board (or one specific round) as stored."""
    check_name(player)
    _, since, _ = board_spec(board)
    since_ms = int(since.replace(tzinfo=timezone.utc).timestamp() * 1000)
    try:
        items = store_list(player)
    except Exception:
        raise HTTPException(503, "Replay storage is unavailable")
    if round_id:
        pool = [i for i in items if i["id"] == round_id]
    else:
        pool = [i for i in items if i["t"] >= since_ms]
    if not pool:
        raise HTTPException(404, "No replay for this player on this board yet")
    pick = max(pool, key=lambda i: (i["score"], -i["t"]))
    try:
        return store_get(player, pick)
    except Exception:
        raise HTTPException(404, "That replay could not be read")


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
