import os
import redis
from fastapi import FastAPI, HTTPException
from psycopg2.pool import ThreadedConnectionPool

app = FastAPI()
r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
pool = ThreadedConnectionPool(
    1, 10,
    host=os.getenv("PG_HOST", "postgresql"),
    dbname=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)

def run(sql, args=(), fetch=False):
    conn = pool.getconn()
    try:
        with conn.cursor() as c:
            c.execute(sql, args)
            rows = c.fetchall() if fetch else None
        conn.commit()
        return rows
    finally:
        pool.putconn(conn)

@app.on_event("startup")
def init():
    run("CREATE TABLE IF NOT EXISTS scores (id SERIAL PRIMARY KEY, player TEXT, "
        "score INT, created_at TIMESTAMP DEFAULT now())")
    run("CREATE INDEX IF NOT EXISTS idx_player ON scores(player)")

def rebuild_cache():
    rows = run("SELECT player, MAX(score) FROM scores GROUP BY player", fetch=True)
    if rows:
        r.zadd("leaderboard", {p: s for p, s in rows})

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/score")
def submit(player: str, score: int):
    if not (0 <= score <= 100000):
        raise HTTPException(400, "invalid score")
    run("INSERT INTO scores(player, score) VALUES (%s, %s)", (player, score))
    r.zadd("leaderboard", {player: score}, gt=True)
    return {"ok": True}

@app.get("/top10")
def top10(nocache: int = 0):
    if nocache:  # reads straight from Postgres (for experiment A)
        rows = run("SELECT player, MAX(score) s FROM scores GROUP BY player "
                   "ORDER BY s DESC LIMIT 10", fetch=True)
        return [{"player": p, "score": s} for p, s in rows]
    if r.zcard("leaderboard") == 0:  # cache empty: rebuild from DB
        rebuild_cache()
    data = r.zrevrange("leaderboard", 0, 9, withscores=True)
    return [{"player": p, "score": int(s)} for p, s in data]

@app.get("/rank/{player}")
def rank(player: str):
    pos = r.zrevrank("leaderboard", player)
    if pos is None:
        raise HTTPException(404, "player not found")
    return {"player": player, "rank": pos + 1, "score": int(r.zscore("leaderboard", player))}