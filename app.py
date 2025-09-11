from fastapi import FastAPI
import redis
import time, psycopg2

app = FastAPI()
r = redis.Redis(host="redis", port=6379)

for i in range(10):
    try:
        conn = psycopg2.connect(
            dbname="demo",
            user="demo",
            password="password",
            host="postgres",
            port=5432,
        )
        cur = conn.cursor()
        break
    except Exception as e:
        print(f"Postgres not ready yet ({e}), retrying in 3s...")
        time.sleep(3)
        
cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT UNIQUE);")
conn.commit()

@app.get("/cache/{key}")
def cache_get(key: str):
    try:
        val = r.get(key)
        return {"key": key, "value": val}
    except Exception as e:
        return {"key": key, "error": str(e)}


@app.post("/cache/{key}/{value}")
def cache_set(key: str, value: str):
    try:
        r.set(key, value)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/users/{name}")
def create_user(name: str):
    try:
        cur.execute("INSERT INTO users (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (name,))
        row = cur.fetchone()
        conn.commit()
        return {"id": row[0] if row else None, "name": name}
    except Exception as e:
        return {"error": str(e)}

@app.get("/users")
def list_users():
    try:
        cur.execute("SELECT id, name FROM users;")
        rows = cur.fetchall()
        return [{"id": r[0], "name": r[1]} for r in rows]
    except Exception as e:
        return {"error": str(e)}    

@app.get("/")
def root():
    return {"message": "Hello from Bootcamp Day 3"}

