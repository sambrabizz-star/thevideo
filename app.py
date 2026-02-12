from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
import re
import subprocess
import os
import sys
import yt_dlp

app = Flask(__name__)

# -----------------------------
# CORS GLOBAL
# -----------------------------
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

@app.after_request
def add_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    return response

# -----------------------------
# Utils
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
# STREAM ENDPOINT
# -----------------------------
@app.route("/tiktok/stream", methods=["POST", "OPTIONS"])
def tiktok_stream():
    if request.method == "OPTIONS":
        return "", 200

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
# Metadata endpoint (OPTIONNEL)
# -----------------------------
@app.route("/tiktok/info", methods=["POST", "OPTIONS"])
def tiktok_info():
    if request.method == "OPTIONS":
        return "", 200

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
# Healthcheck
# -----------------------------
@app.route("/health", methods=["GET", "OPTIONS"])
def health():
    if request.method == "OPTIONS":
        return "", 200
    return jsonify({"status": "ok"})

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)









