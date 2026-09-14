"""
video-service — Worker Python isolé pour StreamLoader
──────────────────────────────────────────────────────
Rôle UNIQUE : parler à yt-dlp/ffmpeg (analyse, téléchargement, sous-titres,
progression, fichier). Appelé exclusivement par le backend Node via le
header `X-Service-Secret` — aucune notion d'utilisateur, d'auth JWT, de
paiement ou de quota ici : tout ça reste dans Node (le Node est celui qui
décide QUI a le droit de demander quoi ; ce service exécute simplement la
demande, avec ses propres garde-fous techniques : SSRF, concurrence, DoS).

Routes :
  GET  /                       → info service
  GET  /health                 → health-check
  POST /admin/update-ytdlp     → force la mise à jour de yt-dlp (cron externe)
  POST /analyze                → { url } -> infos vidéo + formats + sous-titres
  POST /download/start         → { url, quality|format, title, sublang,
                                    trim, startTime, endTime } -> { jobId }
  GET  /progress/<job_id>      → SSE de progression du téléchargement
  GET  /file/<job_id>          → stream + suppression du fichier une fois livré
"""

import os
import re
import json
import time
import uuid
import queue
import shutil
import socket
import ipaddress
import logging
import threading
import subprocess
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from flask import Flask, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("video-service")

PREFERRED_SUBTITLES = [
    "fr", "en", "es", "pt", "pt-PT", "de", "it", "ar", "hi", "zh-Hans", "zh-Hant",
]

# ── ffmpeg ──────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FFMPEG_DIR = os.path.join(BASE_DIR, "ffmpeg")
os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ["PATH"]

# ── User-Agent commun à toutes les invocations yt-dlp ────────────
# ⚠️ IMPORTANT : on n'impose plus de --extractor-args youtube:player_client=...
# Le client par défaut de yt-dlp expose la pleine gamme de résolutions (jusqu'en
# 4K), alors que forcer le client "android" plafonnait à 360p/720p et produisait
# des vidéos floues — porté depuis la dernière version de server.js.
YTDLP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ── Configuration ───────────────────────────────────────────────
SERVICE_SECRET = os.environ.get("SERVICE_SECRET")
CENTRAL_URL = os.environ.get("CENTRAL_URL")
PORT = int(os.environ.get("PORT", 5100))
ENV = os.environ.get("ENV", "development")

TMP_DIR = os.environ.get(
    "TMP_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")
)
os.makedirs(TMP_DIR, exist_ok=True)

JOB_TTL_SECONDS = 15 * 60
ANALYZE_TIMEOUT = 60
# Filet de sécurité DoS : pas plus de MAX_CONCURRENT_JOBS téléchargements
# yt-dlp/ffmpeg en même temps, tous appelants confondus.
MAX_CONCURRENT_JOBS = 12
# Un job ne doit jamais rester bloqué indéfiniment.
JOB_WATCHDOG_SECONDS = 15 * 60

TRIM_TIME_RE = re.compile(r"^\d{1,2}(:\d{2}){1,2}$")

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
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            logger.info("[yt-dlp] Mis à jour : %s", result.stdout.strip().splitlines()[-1])
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
#  PROTECTION SSRF — porté depuis server.js
# ─────────────────────────────────────────────────────────────────
# yt-dlp effectue une vraie requête HTTP sortante depuis CE serveur vers
# l'URL fournie par le client : sans ce filtre, n'importe qui pourrait
# sonder le réseau interne ou les endpoints de métadonnées cloud
# (169.254.169.254 AWS/GCP/Azure) via /analyze ou /download/start.

def is_private_or_reserved_ip(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return True  # format inconnu : on bloque par prudence
    return (
        addr.is_private or addr.is_loopback or addr.is_link_local
        or addr.is_multicast or addr.is_reserved or addr.is_unspecified
    )


def assert_public_http_url(raw_url: str):
    parsed = urlparse(raw_url or "")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("URL invalide")
    try:
        infos = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror:
        raise ValueError("URL invalide")
    for info in infos:
        ip = info[4][0]
        if is_private_or_reserved_ip(ip):
            raise ValueError("URL invalide")
    return parsed


def is_valid_url(url: str) -> bool:
    try:
        assert_public_http_url(url)
        return True
    except ValueError:
        return False


# ─────────────────────────────────────────────────────────────────
#  yt-dlp — parsing (porté depuis server.js, enrichi 429/sous-titres)
# ─────────────────────────────────────────────────────────────────

def parse_ytdlp_error(stderr: str = "") -> str:
    s = stderr.lower()
    if "http error 429" in s or "too many requests" in s:
        return "La plateforme limite temporairement les requêtes (erreur 429). Patiente quelques minutes puis réessaie."
    if "unable to download video subtitles" in s or "video subtitles for" in s:
        return "Les sous-titres de cette langue sont indisponibles pour cette vidéo."
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
DOWNLOADING_FORMATS_RE = re.compile(r"Downloading (\d+) format\(s?\)(?::\s*(\S+))?", re.IGNORECASE)
DESTINATION_RE = re.compile(r"\[download\]\s+Destination:", re.IGNORECASE)


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


def parse_time_to_seconds(t: str):
    parts = str(t).strip().split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    return None


SAFE_TITLE_RE = re.compile(r"[^a-zA-Z0-9\s\-_àâäéèêëîïôöùûüç]")


def sanitize_title(title: str) -> str:
    cleaned = SAFE_TITLE_RE.sub("", title or "video").strip()
    return cleaned[:80] or "video"


SUBLANG_RE = re.compile(r"^[a-zA-Z-]{2,8}$")


def is_valid_sublang(sublang) -> bool:
    return bool(sublang) and bool(SUBLANG_RE.match(sublang))


# ── Qualité vidéo → sélecteur de format yt-dlp — porté depuis server.js ──

def get_target_height(quality: str) -> int:
    return {
        "4k": 2160, "2160p": 2160, "1440p": 1440, "1080p": 1080,
        "720p": 720, "480p": 480, "360p": 360,
    }.get(quality, 1080)


def get_video_format_selector(target_height: int) -> str:
    # 1. H.264 MP4 + audio AAC → lisible partout, copiable sans ré-encodage.
    # 2. N'importe quel MP4 + M4A → remux rapide.
    # 3. Meilleurs flux séparés quelconques.
    # 4. Meilleur flux progressif <= hauteur cible.
    h = target_height
    return "/".join([
        f"bestvideo[height<={h}][ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]",
        f"bestvideo[height<={h}][ext=mp4]+bestaudio[ext=m4a]",
        f"bestvideo[height<={h}]+bestaudio",
        f"best[height<={h}]",
    ])


FORMAT_STRING_RE = re.compile(r"^[\w+,\-\[\]<>=./: ]+$")


def is_valid_format_string(fmt: str) -> bool:
    # Un sélecteur de format yt-dlp valide n'utilise qu'un jeu de caractères
    # restreint. Revalidé ici en défense en profondeur, même si Node valide
    # déjà côté appelant — c'est CE service qui exécute réellement le
    # subprocess avec cette valeur.
    return bool(fmt) and len(fmt) <= 150 and bool(FORMAT_STRING_RE.match(fmt))


# ─────────────────────────────────────────────────────────────────
#  ffprobe / ffmpeg — finalisation (porté depuis server.js)
# ─────────────────────────────────────────────────────────────────

def probe_streams(filepath: str):
    proc = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "stream=index,codec_type,codec_name,width,height,profile",
            "-of", "json", filepath,
        ],
        capture_output=True, text=True, timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or "ffprobe indisponible.")
    streams = json.loads(proc.stdout).get("streams") or []
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    return video, audio


def finalize_video(filepath: str, target_height: int, subtitle_path: str = None, subtitle_lang: str = "und"):
    """
    Finalise le fichier téléchargé en UNE passe ffmpeg :
    - copie la vidéo H.264 si déjà au bon format (sinon ré-encodage H.264),
    - copie l'audio AAC si déjà compatible (sinon ré-encodage AAC),
    - embarque un fichier de sous-titres .srt en piste mov_text si fourni,
    - applique +faststart pour un démarrage immédiat.
    Le fast-path (copie) prend quelques secondes au lieu d'un long ré-encodage.
    """
    converted_path = f"{filepath}.converted.mp4"
    video, audio = probe_streams(filepath)

    source_height = int((video or {}).get("height") or 0)
    video_is_h264 = bool(video) and video.get("codec_name") == "h264"
    audio_is_aac = (not audio) or audio.get("codec_name") == "aac"
    need_scale = source_height > 0 and source_height > target_height

    args = ["ffmpeg", "-y", "-i", filepath]

    if subtitle_path:
        args += ["-i", subtitle_path]
        args += ["-map", "0:v:0", "-map", "0:a:0?", "-map", "1:0"]

    if video_is_h264 and not need_scale:
        args += ["-c:v", "copy"]
    else:
        if need_scale:
            args += ["-vf", f"scale=-2:{target_height}"]
        args += [
            "-c:v", "libx264", "-profile:v", "high", "-level", "5.1",
            "-crf", "20", "-pix_fmt", "yuv420p", "-preset", "veryfast",
        ]

    if audio:
        args += ["-c:a", "copy"] if audio_is_aac else ["-c:a", "aac", "-b:a", "192k"]

    if subtitle_path:
        args += ["-c:s", "mov_text", "-metadata:s:s:0", f"language={subtitle_lang}"]

    args += ["-movflags", "+faststart", converted_path]

    proc = subprocess.run(args, capture_output=True, text=True, timeout=1800)
    if proc.returncode != 0 or not os.path.exists(converted_path):
        try:
            os.remove(converted_path)
        except OSError:
            pass
        raise RuntimeError((proc.stderr or "")[-500:])

    os.remove(filepath)
    os.rename(converted_path, filepath)


def transcode_audio(filepath: str):
    converted_path = f"{filepath}.converted.mp3"
    proc = subprocess.run(
        ["ffmpeg", "-y", "-i", filepath, "-vn", "-c:a", "libmp3lame", "-q:a", "2", converted_path],
        capture_output=True, text=True, timeout=1800,
    )
    if proc.returncode != 0 or not os.path.exists(converted_path):
        try:
            os.remove(converted_path)
        except OSError:
            pass
        raise RuntimeError((proc.stderr or "")[-500:])
    os.remove(filepath)
    os.rename(converted_path, filepath)


# ── Pochette + métadonnées ID3 — pour l'endpoint /download/music ────────
MAX_COVER_BYTES = 5 * 1024 * 1024  # 5 Mo, largement suffisant pour une cover


def download_cover_image(url: str, dest_path: str):
    """Télécharge une image de couverture, avec la même protection SSRF que
    pour les vidéos (même si l'URL provient typiquement d'une API tierce de
    confiance comme Shazam, c'est une URL externe fournie dans un body de
    requête — défense en profondeur)."""
    assert_public_http_url(url)
    req = Request(url, headers={"User-Agent": YTDLP_UA})
    with urlopen(req, timeout=10) as resp:
        data = resp.read(MAX_COVER_BYTES + 1)
        if len(data) > MAX_COVER_BYTES:
            raise ValueError("Image de couverture trop volumineuse")
        with open(dest_path, "wb") as f:
            f.write(data)


def finalize_audio_with_metadata(filepath: str, title: str = None, artist: str = None, cover_path: str = None):
    """
    Ré-encode l'audio en MP3 en embarquant les métadonnées ID3 (titre/artiste)
    et, si fournie, une pochette en tant que piste vidéo attachée (norme ID3
    standard pour l'art de couverture). Porté depuis bot.js (transcodeAudio).
    """
    converted_path = f"{filepath}.converted.mp3"
    args = ["ffmpeg", "-y", "-i", filepath]

    if cover_path:
        args += ["-i", cover_path, "-map", "0:a", "-map", "1:0"]
    else:
        # Préserve une éventuelle miniature déjà embarquée par yt-dlp.
        args += ["-map", "0:a", "-map", "0:v?"]

    args += ["-c:a", "libmp3lame", "-q:a", "2", "-c:v", "copy", "-map_metadata", "0", "-id3v2_version", "3"]

    if cover_path:
        args += ["-metadata:s:v", "title=Album cover", "-metadata:s:v", "comment=Cover (front)"]
    if title:
        args += ["-metadata", f"title={title}"]
    if artist:
        args += ["-metadata", f"artist={artist}"]

    args.append(converted_path)

    proc = subprocess.run(args, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0 or not os.path.exists(converted_path):
        try:
            os.remove(converted_path)
        except OSError:
            pass
        raise RuntimeError((proc.stderr or "")[-500:])

    os.remove(filepath)
    os.rename(converted_path, filepath)


# ─────────────────────────────────────────────────────────────────
#  Sous-titres — téléchargement robuste (porté depuis server.js)
# ─────────────────────────────────────────────────────────────────
# YouTube limite fortement son endpoint de sous-titres (429 fréquent,
# surtout pour les légendes auto-traduites). On télécharge donc le fichier
# de sous-titres SÉPARÉMENT de la vidéo (jamais via --embed-subs, trop
# fragile — c'était la cause de notre bug "sous-titres non fusionnés"),
# avec plusieurs tentatives, puis on l'embarque nous-mêmes via ffmpeg.

SUBTITLE_MAX_ATTEMPTS = 5
SUBTITLE_RETRY_DELAYS = [1.5, 3, 6, 10]
MIN_SUBTITLE_BYTES = 32


def cleanup_subtitle_files(out_base: str):
    dir_ = os.path.dirname(out_base)
    prefix = os.path.basename(out_base) + "."
    try:
        for name in os.listdir(dir_):
            if name.startswith(prefix):
                try:
                    os.remove(os.path.join(dir_, name))
                except OSError:
                    pass
    except OSError:
        pass


def find_subtitle_file(out_base: str, lang: str):
    dir_ = os.path.dirname(out_base)
    base = os.path.basename(out_base)
    try:
        names = os.listdir(dir_)
    except OSError:
        return None

    srt_files = [n for n in names if n.startswith(base + ".") and n.lower().endswith(".srt")]
    chosen = next((n for n in srt_files if n == f"{base}.{lang}.srt"), None)
    if not chosen:
        chosen = next((n for n in srt_files if n.startswith(f"{base}.{lang}-")), None)
    if not chosen and srt_files:
        chosen = srt_files[0]
    if not chosen:
        return None

    full = os.path.join(dir_, chosen)
    try:
        if os.path.getsize(full) < MIN_SUBTITLE_BYTES:
            return None
    except OSError:
        return None
    return full


def is_subtitle_error_retryable(stderr: str) -> bool:
    s = (stderr or "").lower()
    if "no subtitles" in s or "there are no subtitles" in s:
        return False
    if "requested languages" in s and "not available" in s:
        return False
    return True


def attempt_subtitle_download(url: str, lang: str, out_base: str):
    cleanup_subtitle_files(out_base)
    try:
        proc = subprocess.run(
            [
                "yt-dlp", "--skip-download",
                "--write-subs", "--write-auto-subs",
                "--sub-langs", lang,
                "--sub-format", "srt/vtt/best",
                "--convert-subs", "srt",
                "-o", out_base,
                "--no-playlist",
                "--retries", "5",
                "--fragment-retries", "5",
                "--extractor-retries", "2",
                "--sleep-subtitles", "1",
                "--user-agent", YTDLP_UA,
                url,
            ],
            capture_output=True, text=True, timeout=90,
        )
    except subprocess.TimeoutExpired:
        return None, True, "timeout"

    file = find_subtitle_file(out_base, lang)
    retryable = not file and proc.returncode != 0 and is_subtitle_error_retryable(proc.stderr)
    return file, retryable, proc.stderr


def download_subtitle_file(url: str, lang: str, out_base: str):
    last_error = ""
    for attempt in range(1, SUBTITLE_MAX_ATTEMPTS + 1):
        file, retryable, stderr = attempt_subtitle_download(url, lang, out_base)
        if file:
            if attempt > 1:
                logger.info("[subtitles] '%s' récupéré à la tentative %d/%d", lang, attempt, SUBTITLE_MAX_ATTEMPTS)
            return file
        lines = [l for l in (stderr or "").strip().splitlines() if l]
        last_error = lines[-1] if lines else ""
        if not retryable:
            break
        if attempt < SUBTITLE_MAX_ATTEMPTS:
            pause = SUBTITLE_RETRY_DELAYS[attempt - 1] if attempt - 1 < len(SUBTITLE_RETRY_DELAYS) else SUBTITLE_RETRY_DELAYS[-1]
            logger.warning("[subtitles] '%s' indisponible (tentative %d/%d) — nouvel essai dans %ss", lang, attempt, SUBTITLE_MAX_ATTEMPTS, pause)
            time.sleep(pause)
    if last_error:
        logger.warning("[subtitles] '%s' non embarqué : %s", lang, last_error)
    return None


# ─────────────────────────────────────────────────────────────────
#  Nettoyage disque — porté depuis server.js
# ─────────────────────────────────────────────────────────────────
# Balaie TMP_DIR sur le disque (pas seulement la Map en mémoire) et
# supprime tout fichier plus vieux que max_age_seconds. Rattrape les
# fichiers orphelins qu'un job perdu (crash serveur) laisserait sinon
# indéfiniment sur le disque.

def sweep_stale_tmp_files(max_age_seconds: float):
    try:
        entries = os.listdir(TMP_DIR)
    except OSError:
        return
    now = time.time()
    for name in entries:
        full = os.path.join(TMP_DIR, name)
        try:
            if now - os.path.getmtime(full) > max_age_seconds:
                if os.path.isdir(full):
                    shutil.rmtree(full, ignore_errors=True)
                else:
                    os.remove(full)
        except OSError:
            pass


sweep_stale_tmp_files(20 * 60)


# ─────────────────────────────────────────────────────────────────
#  STORE DES JOBS (en mémoire)
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
            expired = [jid for jid, j in jobs.items() if now - j["created_at"] > JOB_TTL_SECONDS]
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
        sweep_stale_tmp_files(20 * 60)


threading.Thread(target=cleanup_loop, daemon=True).start()


# ─────────────────────────────────────────────────────────────────
#  ROUTES DE BASE
# ─────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return jsonify({"message": "video-service up", "env": ENV})


@app.get("/health")
def health():
    try:
        v = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True, timeout=10)
        ytdlp_version = v.stdout.strip() if v.returncode == 0 else "❌ non installé"
    except Exception:
        ytdlp_version = "❌ non installé"

    with jobs_lock:
        active_jobs = len(jobs)

    return jsonify({"status": "ok", "ytdlp": ytdlp_version, "activeJobs": active_jobs})


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
    try:
        assert_public_http_url(url)
    except ValueError:
        return jsonify({"error": "URL invalide"}), 400

    try:
        proc = subprocess.run(
            ["yt-dlp", "--dump-single-json", "--no-warnings", "--no-playlist",
             "--user-agent", YTDLP_UA, url],
            capture_output=True, text=True, timeout=ANALYZE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return jsonify({"error": "yt-dlp a mis trop de temps à répondre."}), 504
    except FileNotFoundError:
        return jsonify({"error": "yt-dlp introuvable sur le serveur."}), 500

    if proc.returncode != 0:
        logger.error("[analyze] yt-dlp a échoué (code=%s) pour url=%s\nSTDERR:\n%s", proc.returncode, url, proc.stderr)
        return jsonify({"error": parse_ytdlp_error(proc.stderr)}), 400

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return jsonify({"error": "Réponse yt-dlp invalide. Réessaie."}), 502

    # On écarte les storyboards (vcodec "images" / ext mhtml), sans intérêt et
    # qui poussaient les vrais formats 4K/1440p hors de la limite.
    raw_formats = [f for f in (data.get("formats") or []) if f.get("vcodec") != "images" and f.get("ext") != "mhtml"]
    formats = []
    for f in raw_formats[:80]:
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

    manual_subs = list((data.get("subtitles") or {}).keys())
    auto_subs = list((data.get("automatic_captions") or {}).keys())
    all_subs = sorted(set(manual_subs + auto_subs))

    selected = []
    for lang in all_subs:
        if lang.endswith("-orig"):
            selected.append(lang)
    for lang in PREFERRED_SUBTITLES:
        if lang in all_subs and lang not in selected:
            selected.append(lang)
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
#  DÉMARRAGE D'UN TÉLÉCHARGEMENT (job en arrière-plan)
# ─────────────────────────────────────────────────────────────────

@app.post("/download/start")
@require_secret
def download_start():
    body = request.get_json(silent=True) or {}
    url = body.get("url")
    quality = body.get("quality", "1080p")
    fmt = body.get("format")
    title = body.get("title", "video")
    sublang = body.get("sublang")
    trim = body.get("trim")
    start_time = body.get("startTime")
    end_time = body.get("endTime")

    if not url:
        return jsonify({"error": "URL manquante"}), 400
    try:
        assert_public_http_url(url)
    except ValueError:
        return jsonify({"error": "URL invalide"}), 400

    with jobs_lock:
        current_jobs = len(jobs)
    if current_jobs >= MAX_CONCURRENT_JOBS:
        return jsonify({
            "error": "Le serveur est actuellement très sollicité. Réessaie dans quelques instants.",
            "code": "SERVER_BUSY",
        }), 503

    is_audio = quality in ("mp3", "audio") or (fmt and "bestaudio" in fmt and "bestvideo" not in fmt)

    # ── Découpage vidéo sur mesure ──────────────────────────────────
    trim_sections = None
    if trim in (True, "true"):
        if not (start_time and TRIM_TIME_RE.match(str(start_time)) and end_time and TRIM_TIME_RE.match(str(end_time))):
            return jsonify({"error": "Format de temps invalide. Utilise MM:SS (ex : 00:10).", "code": "INVALID_TRIM_TIME"}), 400
        start_sec = parse_time_to_seconds(start_time)
        end_sec = parse_time_to_seconds(end_time)
        if start_sec is None or end_sec is None or end_sec <= start_sec:
            return jsonify({"error": "L'heure de fin doit être supérieure à l'heure de début.", "code": "INVALID_TRIM_TIME"}), 400
        trim_sections = f"*{start_time}-{end_time}"

    if fmt and not is_valid_format_string(fmt):
        return jsonify({"error": "Format de téléchargement invalide."}), 400

    normalized_sublang = None
    if sublang:
        normalized_sublang = str(sublang).lower()
        if not is_valid_sublang(normalized_sublang):
            logger.warning("[download] sublang '%s' invalide — sous-titres ignorés", normalized_sublang)
            normalized_sublang = None

    job_id = uuid.uuid4().hex
    ext = "mp3" if is_audio else "mp4"
    safe_title = sanitize_title(title)
    filepath = os.path.join(TMP_DIR, f"{job_id}.{ext}")
    target_height = get_target_height(quality)

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

    format_selector = "bestaudio/best" if is_audio else (fmt if fmt else get_video_format_selector(target_height))

    args = [
        "yt-dlp",
        "-f", format_selector,
        "-o", filepath,
        "--newline", "--progress",
        "--no-warnings", "--no-playlist", "--no-mtime",
    ]
    if is_audio:
        args += ["--extract-audio", "--audio-format", "mp3", "--audio-quality", "0"]
    else:
        args += ["--merge-output-format", "mp4"]

    if trim_sections:
        args += ["--download-sections", trim_sections, "--force-keyframes-at-cuts"]

    args += ["--user-agent", YTDLP_UA, url]

    def run_job():
        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        except FileNotFoundError:
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = "yt-dlp introuvable sur le serveur."
            sse_emit(job_id, {"type": "error", "message": "yt-dlp introuvable sur le serveur."})
            sse_close(job_id)
            return

        # Watchdog : tue le process si le job traîne trop longtemps.
        watchdog_fired = threading.Event()

        def watchdog():
            if not watchdog_fired.wait(JOB_WATCHDOG_SECONDS):
                watchdog_fired.set()
                try:
                    proc.kill()
                except Exception:
                    pass
                with jobs_lock:
                    j = jobs.get(job_id)
                    if j and j["status"] not in ("done", "error"):
                        j["status"] = "error"
                        j["error"] = "Délai dépassé : le téléchargement a pris trop de temps. Réessaie."
                sse_emit(job_id, {"type": "error", "message": "Délai dépassé : le téléchargement a pris trop de temps. Réessaie."})
                sse_close(job_id)

        watchdog_thread = threading.Thread(target=watchdog, daemon=True)
        watchdog_thread.start()

        # Drain stderr en parallèle (évite un deadlock si ffmpeg/yt-dlp
        # produit beaucoup de sortie stderr pendant que stdout attend).
        stderr_lines = []

        def drain_stderr():
            for line in proc.stderr:
                stderr_lines.append(line)

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()

        # Progression agrégée sur l'ensemble des flux (vidéo puis audio) :
        # yt-dlp redémarre son compteur à 0% par flux, on reconstitue une
        # progression monotone 0→99%.
        expected_streams = 1
        stream_index = 0
        best_percent = 0.0

        for line in proc.stdout:
            info = DOWNLOADING_FORMATS_RE.search(line)
            if info:
                spec = info.group(2) or ""
                plus_count = spec.count("+")
                expected_streams = max(1, int(info.group(1)), plus_count + 1)
                continue
            if DESTINATION_RE.search(line):
                stream_index += 1
                expected_streams = max(expected_streams, stream_index)
                continue

            progress = parse_ytdlp_progress(line)
            if progress:
                streams = max(expected_streams, stream_index, 1)
                started_index = max(stream_index, 1)
                overall = min(99.0, ((started_index - 1) + progress["percent"] / 100) / streams * 100)
                best_percent = max(best_percent, overall)
                with jobs_lock:
                    j = jobs.get(job_id)
                    if j:
                        j["percent"] = best_percent
                        j["speed"] = progress["speed"]
                        j["eta"] = progress["eta"]
                        j["total"] = progress["total"]
                        j["status"] = "downloading"
                sse_emit(job_id, {
                    "type": "progress", "percent": best_percent,
                    "stream": started_index, "streams": streams,
                    "total": progress["total"], "speed": progress["speed"], "eta": progress["eta"],
                })

        code = proc.wait()
        watchdog_fired.set()
        stderr_thread.join(timeout=5)
        stderr_output = "".join(stderr_lines)

        if code != 0 or not os.path.exists(filepath):
            logger.error("[download] yt-dlp a échoué (code=%s, job=%s)\nCommande: %s\nSTDERR:\n%s", code, job_id, " ".join(args), stderr_output)
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = parse_ytdlp_error(stderr_output)
            sse_emit(job_id, {"type": "error", "message": parse_ytdlp_error(stderr_output)})
            sse_close(job_id)
            return

        # ── Finalisation : sous-titres (best-effort) + remux/ré-encodage ──
        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["status"] = "converting"
                j["percent"] = 99
        sse_emit(job_id, {
            "type": "processing", "percent": 99,
            "message": "Récupération des sous-titres et finalisation…" if normalized_sublang else "Finalisation et optimisation de la qualité…",
        })

        subtitle_path = None
        try:
            if not is_audio and normalized_sublang:
                subtitle_path = download_subtitle_file(url, normalized_sublang, os.path.join(TMP_DIR, f"{job_id}_sub"))
                if not subtitle_path:
                    logger.warning("[download] %s : sous-titres '%s' indisponibles — vidéo sans sous-titres.", job_id, normalized_sublang)

            if is_audio:
                transcode_audio(filepath)
            else:
                finalize_video(filepath, target_height, subtitle_path, normalized_sublang or "und")
        except Exception as e:
            logger.error("[download] finalisation échouée job=%s : %s", job_id, e)
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = f"Conversion vidéo impossible : {e}"
            sse_emit(job_id, {"type": "error", "message": f"Conversion vidéo impossible : {e}"})
            sse_close(job_id)
            return
        finally:
            if subtitle_path:
                cleanup_subtitle_files(os.path.join(TMP_DIR, f"{job_id}_sub"))

        with jobs_lock:
            j = jobs.get(job_id)
            if j is None:
                return
            j["status"] = "done"
            j["percent"] = 100

        sse_emit(job_id, {"type": "done", "jobId": job_id, "title": safe_title, "ext": ext})
        sse_close(job_id)

    threading.Thread(target=run_job, daemon=True).start()

    return jsonify({"success": True, "jobId": job_id})


# ─────────────────────────────────────────────────────────────────
#  RECHERCHE MUSICALE DÉDIÉE (job en arrière-plan)
# ─────────────────────────────────────────────────────────────────
# Endpoint séparé de /download/start, volontairement : ici on sait déjà
# qu'on veut un MP3 avec métadonnées/pochette embarquées à partir d'une
# recherche texte (ytsearch1:...), jamais une URL arbitraire fournie par
# l'utilisateur final — pas de logique conditionnelle mêlée à la route
# générique.

QUERY_MAX_LENGTH = 200


@app.post("/download/music")
@require_secret
def download_music():
    body = request.get_json(silent=True) or {}
    query = (body.get("query") or "").strip()
    title = (body.get("title") or "audio").strip()
    artist = body.get("artist")
    cover_url = body.get("coverUrl")

    if not query:
        return jsonify({"error": "query manquant"}), 400
    if len(query) > QUERY_MAX_LENGTH:
        return jsonify({"error": "query trop long"}), 400

    with jobs_lock:
        current_jobs = len(jobs)
    if current_jobs >= MAX_CONCURRENT_JOBS:
        return jsonify({
            "error": "Le serveur est actuellement très sollicité. Réessaie dans quelques instants.",
            "code": "SERVER_BUSY",
        }), 503

    # ytsearch1:... n'est jamais une adresse réseau arbitraire fournie par
    # l'utilisateur — yt-dlp résout toujours cette syntaxe contre l'endpoint
    # de recherche YouTube lui-même, jamais contre une destination choisie
    # par l'appelant. Pas de vérification SSRF nécessaire ici (contrairement
    # à /analyze et /download/start qui reçoivent une vraie URL).
    search_url = f"ytsearch1:{query}"

    job_id = uuid.uuid4().hex
    safe_title = sanitize_title(title)
    filepath = os.path.join(TMP_DIR, f"{job_id}.mp3")

    with jobs_lock:
        jobs[job_id] = {
            "status": "starting", "percent": 0, "speed": None, "eta": None, "total": None,
            "filepath": filepath, "title": safe_title, "ext": "mp3",
            "created_at": time.time(), "error": None,
        }

    args = [
        "yt-dlp",
        "-f", "bestaudio/best",
        "-o", filepath,
        "--newline", "--progress",
        "--no-warnings", "--no-playlist", "--no-mtime",
        "--extract-audio", "--audio-format", "mp3", "--audio-quality", "0",
        "--user-agent", YTDLP_UA,
        search_url,
    ]

    def run_job():
        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
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
            logger.error("[music] yt-dlp a échoué (code=%s, job=%s) query=%r\nSTDERR:\n%s", code, job_id, query, stderr_output)
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = parse_ytdlp_error(stderr_output)
            sse_emit(job_id, {"type": "error", "message": parse_ytdlp_error(stderr_output)})
            sse_close(job_id)
            return

        with jobs_lock:
            j = jobs.get(job_id)
            if j:
                j["status"] = "converting"
                j["percent"] = 99
        sse_emit(job_id, {"type": "processing", "percent": 99, "message": "Ajout des métadonnées et de la pochette…"})

        cover_path = None
        try:
            if cover_url:
                cover_path = os.path.join(TMP_DIR, f"{job_id}_cover.jpg")
                try:
                    download_cover_image(cover_url, cover_path)
                except Exception as e:
                    logger.warning("[music] pochette non récupérée job=%s : %s", job_id, e)
                    cover_path = None

            finalize_audio_with_metadata(filepath, title=title, artist=artist, cover_path=cover_path)
        except Exception as e:
            logger.error("[music] finalisation échouée job=%s : %s", job_id, e)
            with jobs_lock:
                j = jobs.get(job_id)
                if j:
                    j["status"] = "error"
                    j["error"] = f"Finalisation audio impossible : {e}"
            sse_emit(job_id, {"type": "error", "message": f"Finalisation audio impossible : {e}"})
            sse_close(job_id)
            return
        finally:
            if cover_path and os.path.exists(cover_path):
                try:
                    os.remove(cover_path)
                except OSError:
                    pass

        with jobs_lock:
            j = jobs.get(job_id)
            if j is None:
                return
            j["status"] = "done"
            j["percent"] = 100

        sse_emit(job_id, {"type": "done", "jobId": job_id, "title": safe_title, "ext": "mp3"})
        sse_close(job_id)

    threading.Thread(target=run_job, daemon=True).start()

    return jsonify({"success": True, "jobId": job_id})

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
            yield "data: " + json.dumps({"type": "done", "jobId": job_id, "title": job_snapshot["title"], "ext": job_snapshot["ext"]}) + "\n\n"
        return Response(gen_done(), mimetype="text/event-stream")

    if job_snapshot["status"] == "error":
        def gen_err():
            yield "data: " + json.dumps({"type": "error", "message": job_snapshot["error"]}) + "\n\n"
        return Response(gen_err(), mimetype="text/event-stream")

    q = queue.Queue()
    with sse_lock:
        sse_queues.setdefault(job_id, []).append(q)

    def gen():
        yield "data: " + json.dumps({
            "type": "progress",
            "percent": job_snapshot["percent"], "speed": job_snapshot["speed"],
            "eta": job_snapshot["eta"], "total": job_snapshot["total"],
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

    headers = {"Cache-Control": "no-cache, no-transform", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
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


def cleanup_all_tmp_files():
    with jobs_lock:
        for j in jobs.values():
            fp = j.get("filepath")
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except OSError:
                    pass


if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=PORT, debug=(ENV == "development"), threaded=True)
    finally:
        cleanup_all_tmp_files()