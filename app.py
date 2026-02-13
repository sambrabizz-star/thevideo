from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import re
import subprocess
import os
import sys
import jwt
import psycopg2
import requests
import yt_dlp
from datetime import datetime, timezone

app = Flask(__name__)

# -----------------------------
# CORS GLOBAL (web + flutter web)
# -----------------------------
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response

# -----------------------------
# CONFIG (Fly.io secrets)
# -----------------------------
DB_URL = os.environ["SUPABASE_DB_URL"]
JWKS_URL = os.environ["SUPABASE_JWKS_URL"]
JWT_ISSUER = os.environ["SUPABASE_JWT_ISSUER"]
JWT_AUDIENCE = os.environ.get("SUPABASE_JWT_AUDIENCE", "authenticated")
QUOTA_PER_HOUR = 30

jwks = requests.get(JWKS_URL, timeout=5).json()

db = psycopg2.connect(DB_URL)
db.autocommit = True

# -----------------------------
# Utils
# -----------------------------
def is_valid_tiktok_url(url: str) -> bool:
    return bool(re.search(r"(vm\.tiktok\.com|tiktok\.com)", url))


def verify_jwt_and_get_user():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None

    token = auth.split(" ", 1)[1]

    try:
        header = jwt.get_unverified_header(token)
        key = next(k for k in jwks["keys"] if k["kid"] == header["kid"])

        payload = jwt.decode(
            token,
            jwt.algorithms.RSAAlgorithm.from_jwk(key),
            algorithms=["RS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
        return payload["sub"]
    except Exception:
        return None


def increment_usage(user_id):
    with db.cursor() as cur:
        cur.execute("""
            insert into api_usage (user_id, hour_bucket, count)
            values (%s, date_trunc('hour', now()), 1)
            on conflict (user_id, hour_bucket)
            do update set count = api_usage.count + 1
            returning count;
        """, (user_id,))
        return cur.fetchone()[0]


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
# STREAM
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
        reset_at = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        return jsonify({
            "error": "Quota exceeded",
            "limit": QUOTA_PER_HOUR,
            "reset_at": reset_at.isoformat()
        }), 429

    data = request.get_json()
    if not data or "url" not in data:
        return jsonify({"error": "Missing url"}), 400

    url = data["url"]
    if not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid TikTok URL"}), 400

    try:
        info, filesize = extract_info_and_filesize(url)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not filesize:
        return jsonify({"error": "Unable to determine file size"}), 500

    def generate():
        cmd = [
            sys.executable,
            "-m", "yt_dlp",
            "-f", "bv*[ext=mp4][watermark!=true]/b[ext=mp4]",
            "-o", "-",
            "--merge-output-format", "mp4",
            "--no-part",
            "--quiet",
            url,
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1024 * 1024,
        )

        try:
            while True:
                chunk = process.stdout.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            process.stdout.close()
            stderr = process.stderr.read().decode()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                app.logger.error(stderr)

    return Response(
        stream_with_context(generate()),
        content_type="video/mp4",
        headers={
            "Content-Disposition": "attachment; filename=tiktok.mp4",
            "Content-Length": str(filesize),
            "Cache-Control": "no-store",
            "Accept-Ranges": "none",
        },
    )

# -----------------------------
# INFO
# -----------------------------
@app.route("/tiktok/info", methods=["POST", "OPTIONS"])
def tiktok_info():
    if request.method == "OPTIONS":
        return "", 200

    user_id = verify_jwt_and_get_user()
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data or "url" not in data:
        return jsonify({"error": "Missing url"}), 400

    url = data["url"]
    if not is_valid_tiktok_url(url):
        return jsonify({"error": "Invalid TikTok URL"}), 400

    try:
        info, filesize = extract_info_and_filesize(url)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "title": info.get("title"),
        "duration": info.get("duration"),
        "filesize": filesize,
    })

# -----------------------------
# HEALTH
# -----------------------------
@app.route("/health", methods=["GET", "OPTIONS"])
def health():
    if request.method == "OPTIONS":
        return "", 200
    return jsonify({"status": "ok"})

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)










