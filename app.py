from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import re
import subprocess
import tempfile
import os
import sys
import shutil
import yt_dlp
import psycopg2
from psycopg2 import pool
import jwt
from jwt import PyJWKClient
from datetime import datetime, timezone

app = Flask(__name__)
app.logger.setLevel("INFO")

# -----------------------------
# CORS
# -----------------------------
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response

# -----------------------------
# CONFIG
# -----------------------------
DB_URL = os.environ.get("SUPABASE_DB_URL")
JWKS_URL = os.environ.get("SUPABASE_JWKS_URL")
JWT_ISSUER = os.environ.get("SUPABASE_JWT_ISSUER")
JWT_AUDIENCE = os.environ.get("SUPABASE_JWT_AUDIENCE", "authenticated")
QUOTA_PER_HOUR = 30

# -----------------------------
# GLOBALS
# -----------------------------
db_pool = None
jwk_client = None

# -----------------------------
# DATABASE POOL
# -----------------------------
def get_db_pool():
    global db_pool
    if db_pool is None:
        app.logger.info("🔌 Initializing psycopg2 pool")
        # Free plan safe: 2 machines × 10 connections → 20 max
        db_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, dsn=DB_URL)
    return db_pool

def get_conn():
    p = get_db_pool()
    return p.getconn()

def release_conn(conn):
    get_db_pool().putconn(conn)

# -----------------------------
# JWKS
# -----------------------------
def get_jwk_client():
    global jwk_client
    if jwk_client is None:
        app.logger.info("🔑 Initializing PyJWKClient")
        jwk_client = PyJWKClient(JWKS_URL)
    return jwk_client

# -----------------------------
# AUTH
# -----------------------------
def verify_jwt_and_get_user():
    auth = request.headers.get("Authorization", "")
    app.logger.info(f"🔐 Authorization header present={bool(auth)}")
    if not auth.startswith("Bearer "):
        app.logger.warning("❌ Missing Bearer token")
        return None
    token = auth.split(" ", 1)[1]
    try:
        signing_key = get_jwk_client().get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["ES256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
        user_id = payload.get("sub")
        app.logger.info(f"✅ JWT verified user_id={user_id}")
        return user_id
    except Exception as e:
        app.logger.error(f"❌ JWT verification failed: {e}")
        return None

# -----------------------------
# QUOTA
# -----------------------------
def increment_usage(user_id):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO api_usage (user_id, hour_bucket, count)
                VALUES (%s, date_trunc('hour', now()), 1)
                ON CONFLICT (user_id, hour_bucket)
                DO UPDATE SET count = api_usage.count + 1
                RETURNING count;
            """, (user_id,))
            count = cur.fetchone()[0]
            app.logger.info(f"📈 Current usage count={count}")
            return count
    finally:
        release_conn(conn)

# -----------------------------
# UTILS
# -----------------------------
def is_valid_tiktok_url(url: str) -> bool:
    return bool(re.search(r"(vm\.tiktok\.com|tiktok\.com)", url))

def extract_info_and_filesize(url: str):
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "nocheckcertificate": True,
        "user_agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
            "Mobile/15E148 Safari/604.1"
        ),
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    filesize = info.get("filesize") or info.get("filesize_approx")
    return info, filesize

# -----------------------------
# STREAM VIDEO
# -----------------------------
@app.route("/tiktok/stream", methods=["POST", "OPTIONS"])
def tiktok_stream():
    if request.method == "OPTIONS":
        return "", 200

    user_id = verify_jwt_and_get_user()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    count = increment_usage(user_id)
    if count > QUOTA_PER_HOUR:
        reset_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return jsonify({"error": "Quota exceeded", "limit": QUOTA_PER_HOUR, "reset_at": reset_at.isoformat()}), 429

    data = request.get_json(silent=True)
    url = data.get("url") if data else None
    if not url or not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid or missing URL"}), 400

    try:
        info, filesize = extract_info_and_filesize(url)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not filesize:
        return jsonify({"error": "Unable to determine file size"}), 500

    def generate():
        app.logger.info("🎬 Streaming video")
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "-f", "bv*[ext=mp4][watermark!=true]/b[ext=mp4]",
            "-o", "-", "--merge-output-format", "mp4",
            "--no-part", "--quiet",
            url
        ]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1024*1024)
        try:
            while True:
                chunk = process.stdout.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            stderr = process.stderr.read().decode()
            process.stdout.close()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                app.logger.error(f"❌ yt-dlp error: {stderr}")

    return Response(
        stream_with_context(generate()),
        content_type="video/mp4",
        headers={
            "Content-Disposition": "attachment; filename=tiktok.mp4",
            "Content-Length": str(filesize),
            "Cache-Control": "no-store",
            "Accept-Ranges": "none",
        }
    )

# -----------------------------
# STREAM AUDIO MP3
# -----------------------------
@app.route("/tiktok/mp3", methods=["POST", "OPTIONS"])
def tiktok_mp3():
    if request.method == "OPTIONS":
        return "", 200

    user_id = verify_jwt_and_get_user()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    count = increment_usage(user_id)
    if count > QUOTA_PER_HOUR:
        reset_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return jsonify({"error": "Quota exceeded", "limit": QUOTA_PER_HOUR, "reset_at": reset_at.isoformat()}), 429

    data = request.get_json(silent=True)
    url = data.get("url").strip() if data else None
    if not url or not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid or missing URL"}), 400

    temp_dir = tempfile.mkdtemp(prefix="tiktok_mp3_")
    video_path = os.path.join(temp_dir, "video.mp4")
    audio_path = os.path.join(temp_dir, "audio.mp3")

    try:
        # Télécharger vidéo
        subprocess.run([
            sys.executable, "-m", "yt_dlp",
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "--no-part", "--no-playlist", "--quiet",
            "-o", video_path, url
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)

        if not os.path.exists(video_path) or os.path.getsize(video_path) < 1024:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({"error": "Downloaded video is empty"}), 500

        # Extraire audio MP3
        subprocess.run([
            "ffmpeg", "-y", "-i", video_path,
            "-vn", "-acodec", "libmp3lame", "-ab", "192k",
            audio_path
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)

        if not os.path.exists(audio_path) or os.path.getsize(audio_path) < 1024:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return jsonify({"error": "MP3 extraction failed"}), 409

        mp3_size = os.path.getsize(audio_path)

        def generate():
            try:
                with open(audio_path, "rb") as f:
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        yield chunk
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        return Response(
            stream_with_context(generate()),
            content_type="audio/mpeg",
            headers={
                "Content-Disposition": "attachment; filename=tiktok_audio.mp3",
                "Content-Length": str(mp3_size),
                "Cache-Control": "no-store",
                "Accept-Ranges": "none",
            }
        )

    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return jsonify({"error": "Video download or MP3 encoding failed", "details": e.stderr.decode(errors="ignore")}), 500

# -----------------------------
# HEALTH
# -----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)
















