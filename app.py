"""
video-service — Worker Python isolé pour StreamLoader
──────────────────────────────────────────────────────
Rôle UNIQUE : parler à yt-dlp (analyse, téléchargement, progression, fichier).
Appelé exclusivement par le backend Node (server.js) via le header
`X-Service-Secret` — aucune notion d'utilisateur, d'auth JWT ou de paiement
ici : tout ça reste dans Node.

Routes :
  GET  /                       → info service
  GET  /health                 → health-check
  POST /admin/update-ytdlp     → force la mise à jour de yt-dlp (cron externe)
  POST /analyze                → { url } -> infos vidéo + formats + sous-titres disponibles
  POST /download/start         → { url, format, title, sublang } -> { jobId }
  GET  /progress/<job_id>      → SSE de progression du téléchargement
  GET  /file/<job_id>          → stream + suppression du fichier une fois livré
"""

import os
import re
import json
import time
import uuid
import queue
import logging
import threading
import subprocess
import shutil
from urllib.parse import quote

from flask import Flask, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("video-service")

PREFERRED_SUBTITLES = [
    "fr",
    "en",
    "es",
    "pt",
    "pt-PT",
    "de",
    "it",
    "ar",
    "hi",
    "zh-Hans",
    "zh-Hant",
]

# ── Remplacement de FFMPEG ──────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FFMPEG_DIR = os.path.join(BASE_DIR, "ffmpeg")
os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ["PATH"]

# Vérification rapide que ffmpeg est accessible (sera aussi testée dans /health)
FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None
if not FFMPEG_AVAILABLE:
    logger.warning("ffmpeg introuvable dans le PATH, l'embed des sous-titres sera désactivé.")
else:
    logger.info("ffmpeg trouvé : %s", shutil.which("ffmpeg"))

# ── Anti-blocage YouTube ────────────────────────────────────────
YTDLP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
YTDLP_ANTI_BLOCK_ARGS = [
    "--user-agent", YTDLP_UA,
    "--extractor-args", "youtube:player_client=android,web",
]

# ── Configuration ───────────────────────────────────────────────
SERVICE_SECRET = os.environ.get("SERVICE_SECRET")
CENTRAL_URL = os.environ.get("CENTRAL_URL")
PORT = int(os.environ.get("PORT", 5100))
ENV = os.environ.get("ENV", "development")

TMP_DIR = os.environ.get(
    "TMP_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")
)
os.makedirs(TMP_DIR, exist_ok=True)

JOB_TTL_SECONDS = 30 * 60
ANALYZE_TIMEOUT = 60

if not SERVICE_SECRET:
    raise RuntimeError("SERVICE_SECRET manquant dans .env")
if not CENTRAL_URL:
    raise RuntimeError("CENTRAL_URL manquant dans .env")


# ── Mise à jour auto de yt-dlp (cron in-process) ───────────────
def update_ytdlp():
    logger.info("[yt-dlp] Vérification des mises à jour...")
    try:
        result = subprocess.run(
            ["pip", "install", "--upgrade", "yt-dlp", "--break-system-packages"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            logger.info(
                "[yt-dlp] Mis à jour : %s", result.stdout.strip().splitlines()[-1]
            )
        else:
            logger.warning("[yt-dlp] Échec de la mise à jour : %s", result.stderr.strip())
    except Exception as e:
        logger.error("[yt-dlp] Erreur pendant la mise à jour : %s", e)


scheduler = BackgroundScheduler()
update_ytdlp()
scheduler.add_job(update_ytdlp, "cron", hour=4, minute=0)
scheduler.start()


app = Flask(__name__)

CORS(
    app,
    origins="*",
    supports_credentials=True,
    methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Service-Secret"],
)


# ── Auth interne par secret partagé ──────────────────────────────
def verify_service_secret(req):
    return req.headers.get("X-Service-Secret") == SERVICE_SECRET


def require_secret(fn):
    def wrapper(*args, **kwargs):
        if not verify_service_secret(request):
            return jsonify({"detail": "Non autorisé"}), 401
        return fn(*args, **kwargs)

    wrapper.__name__ = fn.__name__
    return wrapper


# ─────────────────────────────────────────────────────────────────
#  yt-dlp — parsing (porté depuis server.js)
# ─────────────────────────────────────────────────────────────────

def parse_ytdlp_error(stderr: str = "") -> str:
    s = stderr.lower()
    if "private video" in s or "video is private" in s:
        return "Cette vidéo est privée et inaccessible."
    if "age-restricted" in s or "age restriction" in s or "confirm your age" in s:
        return "Cette vidéo est réservée aux adultes (restriction d'âge)."
    if "not available in your country" in s or "geo-blocked" in s:
        return "Vidéo non disponible dans ta région."
    if "this video is unavailable" in s or "video unavailable" in s:
        return "Vidéo indisponible (supprimée ou retirée)."
    if "unsupported url" in s or "no supported" in s:
        return "URL non reconnue ou site non supporté."
    if "copyright" in s or "has been removed" in s:
        return "Vidéo retirée pour violation de droits d'auteur."
    if "sign in" in s or "login required" in s:
        return "Cette vidéo requiert une connexion au compte de la plateforme."
    if "members-only" in s or "paid content" in s:
        return "Contenu réservé aux membres / payant."
    if "429" in s or "too many requests" in s:
        return "Trop de requêtes envoyées à la plateforme (429). Réessaie dans quelques minutes."
    if "network" in s or "connection refused" in s:
        return "Erreur réseau. Réessaie dans quelques instants."
    if "http error 403" in s:
        return "Accès refusé par la plateforme (erreur 403)."
    if "http error 404" in s:
        return "Vidéo introuvable (erreur 404). Vérifie l'URL."
    return "Impossible d'analyser cette vidéo. Vérifie l'URL et réessaie."


PROGRESS_RE = re.compile(
    r"\[download\]\s+([\d.]+)%\s+of\s+([\d.]+\s*\S+)\s+at\s+([\d.]+\s*\S+/s)"
    r"(?:\s+ETA\s+([\d:]+))?"
)


def parse_ytdlp_progress(line: str):
    m = PROGRESS_RE.search(line)
    if not m:
        return None
    return {
        "percent": float(m.group(1)),
        "total": m.group(2).strip(),
        "speed": m.group(3).strip(),
        "eta": m.group(4),
    }


def is_valid_url(url: str) -> bool:
    return bool(re.match(r"^https?://", url or "", re.IGNORECASE))


SAFE_TITLE_RE = re.compile(r"[^a-zA-Z0-9\s\-_àâäéèêëîïôöùûüç]")


def sanitize_title(title: str) -> str:
    cleaned = SAFE_TITLE_RE.sub("", title or "video").strip()
    return cleaned[:80] or "video"


SUBLANG_RE = re.compile(r"^[a-zA-Z-]{2,8}$")


def is_valid_sublang(sublang) -> bool:
    return bool(sublang) and bool(SUBLANG_RE.match(sublang))

# ─────────────────────────────────────────────────────────────────
#  STORE DES JOBS
# ─────────────────────────────────────────────────────────────────
jobs = {}
jobs_lock = threading.Lock()

sse_queues = {}
sse_lock = threading.Lock()


def sse_emit(job_id, data):
    with sse_lock:
        subscribers = list(sse_queues.get(job_id, []))
    for q in subscribers:
        q.put(data)


def sse_close(job_id):
    with sse_lock:
        subscribers = sse_queues.pop(job_id, [])
    for q in subscribers:
        q.put(None)


def cleanup_loop():
    while True:
        time.sleep(10 * 60)
        now = time.time()
        with jobs_lock:
            expired = [
                jid for jid, j in jobs.items()
                if now - j["created_at"] > JOB_TTL_SECONDS
            ]
            for jid in expired:
                j = jobs.pop(jid)
                fp = j.get("filepath")
                if fp and os.path.exists(fp):
                    try:
                        os.remove(fp)
                    except OSError:
                        pass
        if expired:
            logger.info("[cleanup] %d job(s) expiré(s) nettoyé(s)", len(expired))


threading.Thread(target=cleanup_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────
#  FONCTION D'EMBED DE SOUS-TITRES AVEC FFMPEG
# ─────────────────────────────────────────────────────────────────
def embed_subtitles_ffmpeg(video_path, subtitle_files, title, ext):
    """
    Intègre les fichiers de sous-titres dans la vidéo MP4.
    Retourne True si l'opération a réussi.
    """
    if not subtitle_files or not FFMPEG_AVAILABLE:
        return False

    tmp_out = video_path + ".tmp.mp4"
    cmd = ["ffmpeg", "-y", "-i", video_path]

    # Ajouter chaque fichier de sous-titres en entrée
    for sf in subtitle_files:
        cmd += ["-i", sf]

    # On copie la vidéo et l'audio tels quels
    cmd += ["-map", "0:v", "-map", "0:a?", "-c:v", "copy", "-c:a", "copy"]

    # Pour chaque fichier de sous-titres, on mappe le flux et on définit la langue
    for idx, sf in enumerate(subtitle_files):
        # Deviner la langue à partir du nom du fichier (ex: video.fr.srt)
        lang = "und"
        basename = os.path.splitext(os.path.basename(sf))[0]
        parts = basename.split(".")
        for part in reversed(parts):
            if re.match(r"^[a-zA-Z]{2,3}$", part):
                lang = part
                break
        # idx+1 car le premier fichier d'entrée est à l'index 1 (0 = vidéo)
        cmd += [
            "-map", f"{idx+1}:s",
            f"-metadata:s:s:{idx}", f"language={lang}",
            f"-disposition:s:{idx}", "default",
            f"-c:s:{idx}", "mov_text",
        ]
    cmd += [tmp_out]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0 and os.path.exists(tmp_out):
            os.replace(tmp_out, video_path)
            return True
        else:
            logger.error("ffmpeg embed error: %s", result.stderr)
            if os.path.exists(tmp_out):
                os.remove(tmp_out)
            return False
    except Exception as e:
        logger.exception("Exception during ffmpeg embed: %s", e)
        if os.path.exists(tmp_out):
            os.remove(tmp_out)
        return False


# ─────────────────────────────────────────────────────────────────
#  ROUTES DE BASE
# ─────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return jsonify({"message": "video-service up", "env": ENV})


@app.get("/health")
def health():
    try:
        v = subprocess.run(
            ["yt-dlp", "--version"], capture_output=True, text=True, timeout=10
        )
        ytdlp_version = v.stdout.strip() if v.returncode == 0 else "❌ non installé"
    except Exception:
        ytdlp_version = "❌ non installé"

    ffmpeg_ok = False
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        ffmpeg_ok = r.returncode == 0
    except Exception:
        pass

    with jobs_lock:
        active_jobs = len(jobs)

    return jsonify({
        "status": "ok",
        "ytdlp": ytdlp_version,
        "activeJobs": active_jobs,
        "ffmpeg_available": ffmpeg_ok,
    })


@app.post("/admin/update-ytdlp")
@require_secret
def trigger_update():
    update_ytdlp()
    return jsonify({"success": True})


# ─────────────────────────────────────────────────────────────────
#  ANALYSE
# ─────────────────────────────────────────────────────────────────
@app.post("/analyze")
@require_secret
def analyze():
    body = request.get_json(silent=True) or {}
    url = body.get("url") or request.args.get("url")

    if not url:
        return jsonify({"error": "URL manquante"}), 400
    if not is_valid_url(url):
        return jsonify({"error": "URL invalide"}), 400

    try:
        proc = subprocess.run(
            [
                "yt-dlp", "--dump-json", "--no-warnings", "--no-playlist",
                *YTDLP_ANTI_BLOCK_ARGS,
                url,
            ],
            capture_output=True,
            text=True,
            timeout=ANALYZE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return jsonify({"error": "yt-dlp a mis trop de temps à répondre."}), 504
    except FileNotFoundError:
        return jsonify({"error": "yt-dlp introuvable sur le serveur."}), 500

    if proc.returncode != 0:
        logger.error(
            "[analyze] yt-dlp a échoué (code=%s) pour url=%s\nSTDERR:\n%s",
            proc.returncode, url, proc.stderr,
        )
        return jsonify({"error": parse_ytdlp_error(proc.stderr)}), 400

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return jsonify({"error": "Réponse yt-dlp invalide. Réessaie."}), 502

    formats = []
    for f in (data.get("formats") or [])[:40]:
        formats.append({
            "id": f.get("format_id"),
            "ext": f.get("ext") or "?",
            "height": f.get("height"),
            "width": f.get("width"),
            "fps": f.get("fps"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "vcodec": f.get("vcodec") if f.get("vcodec") != "none" else None,
            "acodec": f.get("acodec") if f.get("acodec") != "none" else None,
            "tbr": f.get("tbr"),
        })

    # Extraire les langues de sous-titres (manuels + auto), porté depuis server.js
    manual_subs = list((data.get("subtitles") or {}).keys())
    auto_subs = list((data.get("automatic_captions") or {}).keys())
    all_subs = sorted(set(manual_subs + auto_subs))

    selected = []
    # 1. langue originale si elle existe
    for lang in all_subs:
        if lang.endswith("-orig"):
            selected.append(lang)

    # 2. langues prioritaires
    for lang in PREFERRED_SUBTITLES:
        if lang in all_subs and lang not in selected:
            selected.append(lang)

    # 3. compléter jusqu'à 10
    for lang in all_subs:
        if lang not in selected:
            selected.append(lang)
        if len(selected) >= 10:
            break

    return jsonify({
        "success": True,
        "data": {
            "title": data.get("title") or "Sans titre",
            "duration": data.get("duration"),
            "uploader": data.get("uploader") or data.get("channel") or "Inconnu",
            "thumbnail": data.get("thumbnail"),
            "subtitles": selected,
            "extractor": data.get("extractor_key") or "?",
            "webpage": data.get("webpage_url") or url,
            "formats": formats,
        },
    })

# ─────────────────────────────────────────────────────────────────
#  DÉMARRAGE D'UN TÉLÉCHARGEMENT
# ─────────────────────────────────────────────────────────────────
@app.post("/download/start")
@require_secret
def download_start():
    body = request.get_json(silent=True) or {}
    url = body.get("url")
    fmt = body.get("format", "bestvideo+bestaudio/best")
    title = body.get("title", "video")
    sublang = body.get("sublang")

    if not url:
        return jsonify({"error": "URL manquante"}), 400
    if not is_valid_url(url):
        return jsonify({"error": "URL invalide"}), 400

    job_id = uuid.uuid4().hex
    is_audio = "bestaudio" in fmt and "bestvideo" not in fmt
    ext = "mp3" if is_audio else "mp4"
    safe_title = sanitize_title(title)
    filepath = os.path.join(TMP_DIR, f"{job_id}.{ext}")

    with jobs_lock:
        jobs[job_id] = {
            "status": "starting",
            "percent": 0,
            "speed": None,
            "eta": None,
            "total": None,
            "filepath": filepath,
            "title": safe_title,
            "ext": ext,
            "created_at": time.time(),
            "error": None,
        }

    # Arguments de téléchargement principal (SANS --ignore-errors)
    args = [
        "yt-dlp",
        "-f", fmt,
        "-o", filepath,
        "--newline",
        "--progress",
        "--no-warnings",
        "--no-playlist",
    ]
    if is_audio:
        args += ["--extract-audio", "--audio-format", "mp3", "--audio-quality", "0"]
    else:
        args += ["--merge-output-format", "mp4"]

    args += YTDLP_ANTI_BLOCK_ARGS
    args.append(url)

    def run_job():
        # --- Étape 1 : Téléchargement vidéo/audio ---
        try:
            proc = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = "yt-dlp introuvable sur le serveur."
            sse_emit(job_id, {"type": "error", "message": "yt-dlp introuvable sur le serveur."})
            sse_close(job_id)
            return

        stderr_lines = []

        def drain_stderr():
            for line in proc.stderr:
                stderr_lines.append(line)

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()

        for line in proc.stdout:
            progress = parse_ytdlp_progress(line)
            if progress:
                with jobs_lock:
                    j = jobs.get(job_id)
                    if j:
                        j.update(progress)
                        j["status"] = "downloading"
                sse_emit(job_id, {"type": "progress", **progress})

        code = proc.wait()
        stderr_thread.join(timeout=5)
        stderr_output = "".join(stderr_lines)

        if code != 0 or not os.path.exists(filepath):
            # Échec du téléchargement principal
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = parse_ytdlp_error(stderr_output)
            sse_emit(job_id, {"type": "error", "message": parse_ytdlp_error(stderr_output)})
            sse_close(job_id)
            return

        # --- Étape 2 : Sous-titres (uniquement si vidéo et langue valide) ---
        subs_embedded = False
        if not is_audio and is_valid_sublang(sublang) and FFMPEG_AVAILABLE:
            sub_output_dir = os.path.join(TMP_DIR, f"{job_id}_subs")
            os.makedirs(sub_output_dir, exist_ok=True)
            sub_args = [
                "yt-dlp",
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs", sublang,
                "-o", os.path.join(sub_output_dir, "%(title)s.%(ext)s"),
                "--no-warnings",
                "--no-playlist",
                *YTDLP_ANTI_BLOCK_ARGS,
                url
            ]
            try:
                sub_proc = subprocess.run(
                    sub_args,
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if sub_proc.returncode != 0:
                    logger.warning(
                        "[download] Sous-titres non récupérés pour %s : %s",
                        job_id, sub_proc.stderr.strip()
                    )
                else:
                    # Récupérer tous les fichiers de sous-titres dans le dossier
                    subtitle_files = []
                    for fname in os.listdir(sub_output_dir):
                        if any(fname.endswith(ext) for ext in ('.vtt', '.srt', '.ass')):
                            subtitle_files.append(os.path.join(sub_output_dir, fname))
                    if subtitle_files:
                        subs_embedded = embed_subtitles_ffmpeg(filepath, subtitle_files, safe_title, ext)
                        if subs_embedded:
                            logger.info("[download] Sous-titres intégrés avec succès pour %s", job_id)
                        else:
                            logger.error("[download] Échec de l'intégration des sous-titres pour %s", job_id)
                    else:
                        logger.warning("[download] Aucun fichier de sous-titres trouvé pour %s", job_id)
            except Exception as e:
                logger.error("[download] Erreur pendant la récupération des sous-titres : %s", e)
            finally:
                try:
                    shutil.rmtree(sub_output_dir, ignore_errors=True)
                except Exception:
                    pass

        # --- Finalisation ---
        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["status"] = "done"
                j["percent"] = 100
                j["subs_embedded"] = subs_embedded

        sse_emit(job_id, {
            "type": "done",
            "jobId": job_id,
            "title": safe_title,
            "ext": ext,
            "subs": subs_embedded,
        })
        sse_close(job_id)

    threading.Thread(target=run_job, daemon=True).start()
    return jsonify({"success": True, "jobId": job_id})

# ─────────────────────────────────────────────────────────────────
#  PROGRESSION (Server-Sent Events)
# ─────────────────────────────────────────────────────────────────
@app.get("/progress/<job_id>")
@require_secret
def progress(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        job_snapshot = dict(job) if job else None

    if not job_snapshot:
        return jsonify({"error": "Job introuvable"}), 404

    if job_snapshot["status"] == "done":
        def gen_done():
            yield "data: " + json.dumps({
                "type": "done", "jobId": job_id,
                "title": job_snapshot["title"], "ext": job_snapshot["ext"],
            }) + "\n\n"
        return Response(gen_done(), mimetype="text/event-stream")

    if job_snapshot["status"] == "error":
        def gen_err():
            yield "data: " + json.dumps({
                "type": "error", "message": job_snapshot["error"],
            }) + "\n\n"
        return Response(gen_err(), mimetype="text/event-stream")

    q = queue.Queue()
    with sse_lock:
        sse_queues.setdefault(job_id, []).append(q)

    def gen():
        yield "data: " + json.dumps({
            "type": "progress",
            "percent": job_snapshot["percent"],
            "speed": job_snapshot["speed"],
            "eta": job_snapshot["eta"],
            "total": job_snapshot["total"],
        }) + "\n\n"

        try:
            while True:
                try:
                    item = q.get(timeout=20)
                except queue.Empty:
                    yield ": heartbeat\n\n"
                    continue
                if item is None:
                    break
                yield "data: " + json.dumps(item) + "\n\n"
                if item.get("type") in ("done", "error"):
                    break
        finally:
            with sse_lock:
                subscribers = sse_queues.get(job_id)
                if subscribers and q in subscribers:
                    subscribers.remove(q)

    headers = {
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(stream_with_context(gen()), mimetype="text/event-stream", headers=headers)

# ─────────────────────────────────────────────────────────────────
#  LIVRAISON DU FICHIER (usage unique, supprimé après envoi)
# ─────────────────────────────────────────────────────────────────
@app.get("/file/<job_id>")
@require_secret
def get_file(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        job_snapshot = dict(job) if job else None

    if not job_snapshot or job_snapshot["status"] != "done":
        return jsonify({"error": "Fichier non disponible ou expiré"}), 404

    filepath = job_snapshot["filepath"]
    if not os.path.exists(filepath):
        return jsonify({"error": "Fichier expiré. Relance le téléchargement."}), 404

    mime = "audio/mpeg" if job_snapshot["ext"] == "mp3" else "video/mp4"
    filename = f"{job_snapshot['title']}.{job_snapshot['ext']}"
    encoded = quote(filename)
    filesize = os.path.getsize(filepath)

    def generate():
        try:
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
        finally:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except OSError:
                pass
            with jobs_lock:
                jobs.pop(job_id, None)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{encoded}',
        "Content-Type": mime,
        "Content-Length": str(filesize),
    }
    return Response(generate(), headers=headers)

# ─────────────────────────────────────────────────────────────────
#  ARRÊT PROPRE
# ─────────────────────────────────────────────────────────────────
def cleanup_all_tmp_files():
    with jobs_lock:
        for j in jobs.values():
            fp = j.get("filepath")
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except OSError:
                    pass


# ── Point d'entrée ────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        app.run(
            host="0.0.0.0",
            port=PORT,
            debug=(ENV == "development"),
            threaded=True,
        )
    finally:
        cleanup_all_tmp_files()