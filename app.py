from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import re, subprocess, os, sys, jwt, psycopg2, requests, yt_dlp
from datetime import datetime, timezone

app = Flask(__name__)
app.logger.setLevel("INFO")

CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

# -----------------------------
# CONFIG
# -----------------------------
DB_URL = os.environ.get("SUPABASE_DB_URL")
JWKS_URL = os.environ.get("SUPABASE_JWKS_URL")
JWT_ISSUER = os.environ.get("SUPABASE_JWT_ISSUER")
JWT_AUDIENCE = os.environ.get("SUPABASE_JWT_AUDIENCE", "authenticated")
QUOTA_PER_HOUR = 30

jwks_cache = None
db = None

# -----------------------------
# INIT HELPERS
# -----------------------------
def get_db():
    global db
    if db is None or db.closed != 0:
        app.logger.info("🔌 Connecting to Supabase DB")
        db = psycopg2.connect(DB_URL)
        db.autocommit = True
    return db


def get_jwks():
    global jwks_cache
    if jwks_cache is None:
        app.logger.info("🔑 Fetching JWKS")
        jwks_cache = requests.get(JWKS_URL, timeout=5).json()
    return jwks_cache


def verify_jwt_and_get_user():
    auth = request.headers.get("Authorization", "")
    app.logger.info(f"🔐 Authorization header present={bool(auth)}")

    if not auth.startswith("Bearer "):
        return None

    token = auth.split(" ", 1)[1]

    try:
        header = jwt.get_unverified_header(token)
        jwks = get_jwks()
        key = next(k for k in jwks["keys"] if k["kid"] == header["kid"])

        payload = jwt.decode(
            token,
            jwt.algorithms.RSAAlgorithm.from_jwk(key),
            algorithms=["RS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
        app.logger.info(f"✅ JWT OK user_id={payload['sub']}")
        return payload["sub"]
    except Exception as e:
        app.logger.error(f"❌ JWT verification failed: {e}")
        return None


def increment_usage(user_id):
    app.logger.info(f"📊 Increment usage for {user_id}")
    with get_db().cursor() as cur:
        cur.execute("""
            insert into api_usage (user_id, hour_bucket, count)
            values (%s, date_trunc('hour', now()), 1)
            on conflict (user_id, hour_bucket)
            do update set count = api_usage.count + 1
            returning count;
        """, (user_id,))
        count = cur.fetchone()[0]
        app.logger.info(f"📈 Current count={count}")
        return count


def is_valid_tiktok_url(url):
    return bool(re.search(r"(vm\.tiktok\.com|tiktok\.com)", url))

# -----------------------------
# STREAM
# -----------------------------
@app.route("/tiktok/stream", methods=["POST", "OPTIONS"])
def tiktok_stream():
    app.logger.info("➡️ /tiktok/stream called")

    if request.method == "OPTIONS":
        return "", 200

    user_id = verify_jwt_and_get_user()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    count = increment_usage(user_id)
    if count > QUOTA_PER_HOUR:
        app.logger.warning("⛔ Quota exceeded")
        return jsonify({"error": "Quota exceeded"}), 429

    data = request.get_json(silent=True)
    app.logger.info(f"📦 Payload={data}")

    if not data or "url" not in data:
        return jsonify({"error": "Missing url"}), 400

    url = data["url"]
    if not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid TikTok URL"}), 400

    def generate():
        app.logger.info("🎬 Starting yt-dlp stream")
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "-f", "bv*[ext=mp4]/b[ext=mp4]",
            "-o", "-", "--quiet", url,
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            while True:
                chunk = process.stdout.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            stderr = process.stderr.read().decode()
            process.wait()
            if process.returncode != 0:
                app.logger.error(f"yt-dlp error: {stderr}")
            else:
                app.logger.info("✅ Stream completed")

    return Response(
        stream_with_context(generate()),
        content_type="video/mp4",
        headers={"Cache-Control": "no-store"},
    )


@app.route("/health")
def health():
    return {"status": "ok"}











