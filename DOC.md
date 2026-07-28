# video-service — Documentation d'exploitation

Service interne Python/Flask dont le seul rôle est de piloter `yt-dlp`
(analyse de vidéos, téléchargement, suivi de progression, livraison de
fichier). Il n'est **jamais** exposé directement aux utilisateurs finaux :
il n'est appelé que par le backend Node (`server.js`), en machine-à-machine,
via un secret partagé.

```
Navigateur ──▶ Node (server.js) ──▶ video-service (Flask) ──▶ yt-dlp
              auth / paiement /      analyse / download /
              rate-limit / SQLite    progress / file
```

---

## 1. Configuration

Variables d'environnement requises (fichier `.env`) :

| Variable         | Obligatoire              | Description                                                                                                                                           |
| ---------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SERVICE_SECRET` | ✅                       | Secret partagé avec Node. Toutes les routes (sauf `/` et `/health`) le vérifient via le header `X-Service-Secret`.                                    |
| `CENTRAL_URL`    | ✅                       | URL du backend Node. Réservé à un usage futur (webhook, CORS ciblé).                                                                                  |
| `PORT`           | non (def. `5100`)        | Port d'écoute du service.                                                                                                                             |
| `ENV`            | non (def. `development`) | `development` active `debug=True` sur Flask ; à mettre à `production` en prod.                                                                        |
| `TMP_DIR`        | non (def. `./tmp`)       | Dossier de stockage des fichiers en cours/terminés. **Doit être sur un disque persistant** si l'hébergeur redémarre le conteneur entre deux requêtes. |

Le service refuse de démarrer si `SERVICE_SECRET` ou `CENTRAL_URL` sont
absents (`RuntimeError` au boot).

---

## 2. Authentification

Toutes les routes métier exigent le header :

```
X-Service-Secret: <valeur de SERVICE_SECRET>
```

Absent ou incorrect → `401 { "detail": "Non autorisé" }`.

Il n'y a **aucune notion d'utilisateur** dans ce service : Node doit avoir
déjà authentifié/rate-limité l'appelant avant de relayer la requête ici.

---

## 3. Référence des routes

### `GET /`

Ping basique, pas d'auth requise.

```json
{ "message": "video-service up", "env": "production" }
```

### `GET /health`

Health-check, pas d'auth requise (à appeler par ton monitoring / load balancer).

```json
{ "status": "ok", "ytdlp": "2024.12.06", "activeJobs": 2 }
```

### `POST /admin/update-ytdlp`

Force une mise à jour immédiate de `yt-dlp` (en plus du cron interne, quotidien
à 4h). Utile si l'hébergeur fait du scale-to-zero et que le cron in-process
n'a pas eu l'occasion de tourner.

**Auth requise.**

```bash
curl -X POST https://video-service.internal/admin/update-ytdlp \
  -H "X-Service-Secret: $SERVICE_SECRET"
```

```json
{ "success": true }
```

---

### `POST /analyze`

Analyse une URL vidéo sans la télécharger (`yt-dlp --dump-json`).

**Auth requise.** Body JSON :

```json
{ "url": "https://www.youtube.com/watch?v=..." }
```

**Réponse `200`** :

```json
{
  "success": true,
  "data": {
    "title": "Titre de la vidéo",
    "duration": 245,
    "uploader": "Nom de la chaîne",
    "thumbnail": "https://...",
    "extractor": "Youtube",
    "webpage": "https://...",
    "formats": [
      {
        "id": "137",
        "ext": "mp4",
        "height": 1080,
        "width": 1920,
        "fps": 30,
        "filesize": 52428800,
        "vcodec": "avc1.640028",
        "acodec": null,
        "tbr": 4500
      }
    ]
  }
}
```

**Erreurs possibles (toutes en `400` sauf mention contraire)** :

| Message                                                          | Cause                            |
| ---------------------------------------------------------------- | -------------------------------- |
| `URL manquante`                                                  | body sans `url`                  |
| `URL invalide`                                                   | ne commence pas par `http(s)://` |
| `Cette vidéo est privée et inaccessible.`                        | vidéo privée                     |
| `Cette vidéo est réservée aux adultes (restriction d'âge).`      | âge                              |
| `Vidéo non disponible dans ta région.`                           | geoblocking                      |
| `Vidéo indisponible (supprimée ou retirée).`                     | supprimée                        |
| `URL non reconnue ou site non supporté.`                         | site non géré par yt-dlp         |
| `Vidéo retirée pour violation de droits d'auteur.`               | copyright                        |
| `Cette vidéo requiert une connexion au compte de la plateforme.` | login requis                     |
| `Contenu réservé aux membres / payant.`                          | contenu payant                   |
| `Accès refusé par la plateforme (erreur 403).`                   | HTTP 403                         |
| `Vidéo introuvable (erreur 404). Vérifie l'URL.`                 | HTTP 404                         |
| `yt-dlp a mis trop de temps à répondre.`                         | `504`, timeout 60s dépassé       |
| `yt-dlp introuvable sur le serveur.`                             | `500`, binaire absent            |
| `Réponse yt-dlp invalide. Réessaie.`                             | `502`, JSON illisible            |

---

### `POST /download/start`

Démarre un téléchargement en tâche de fond (thread dédié). Répond
immédiatement, le suivi se fait ensuite via `/progress/:jobId`.

**Auth requise.** Body JSON :

```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "format": "bestvideo+bestaudio/best",
  "title": "Nom du fichier souhaité"
}
```

- `format` : optionnel, défaut `bestvideo+bestaudio/best`. Si la valeur
  contient `bestaudio` sans `bestvideo`, le job extrait l'audio en MP3
  (`--extract-audio --audio-format mp3 --audio-quality 0`), sinon MP4.
- `title` : optionnel, défaut `video`. Nettoyé automatiquement (caractères
  spéciaux retirés, 80 caractères max) — c'est ce nom nettoyé qui sert de
  nom de fichier final.

**Réponse `200`** :

```json
{ "success": true, "jobId": "3f9a1c2b8e4d4a2f9b6c7d8e9f0a1b2c" }
```

**Erreurs** : `400 URL manquante` / `400 URL invalide`.

> Le job n'échoue jamais à ce stade — les erreurs de téléchargement
> apparaissent plus tard via le SSE ou la route `/progress`.

---

### `GET /progress/<job_id>` (Server-Sent Events)

Flux SSE de progression. **Auth requise** (header `X-Service-Secret`,
compatible avec `EventSource` seulement si tu passes par un proxy Node qui
ajoute le header — un `EventSource` navigateur ne permet pas de headers
custom, voir §5).

Comportement :

- Si le job est déjà `done` ou `error` au moment de l'appel → un seul
  événement puis fermeture immédiate.
- Sinon → un événement `progress` immédiat (snapshot), puis un événement par
  mise à jour, plus un heartbeat (`: heartbeat`) toutes les 20s tant qu'il
  n'y a pas de nouvelle donnée.
- La connexion se ferme d'elle-même dès qu'un événement `done` ou `error`
  est envoyé.

**Types d'événements** (`data: {...}\n\n`) :

```jsonc
// progression
{ "type": "progress", "percent": 45.3, "total": "78.50MiB", "speed": "1.23MiB/s", "eta": "00:45" }

// terminé
{ "type": "done", "jobId": "3f9a1c2b...", "title": "Nom du fichier", "ext": "mp4" }

// erreur
{ "type": "error", "message": "Cette vidéo est privée et inaccessible." }
```

`404 { "error": "Job introuvable" }` si le `job_id` n'existe pas (jamais
créé, ou déjà nettoyé après 30 min / après livraison du fichier).

---

### `GET /file/<job_id>`

Télécharge le fichier terminé. **Usage unique** : le fichier est supprimé
du disque et le job retiré de la mémoire juste après l'envoi complet (que
ce soit un succès ou une déconnexion du client en cours de route).

**Auth requise.**

Réponse : flux binaire (`Content-Type: video/mp4` ou `audio/mpeg`), avec
`Content-Disposition: attachment; filename="..."`.

Erreurs :

- `404 { "error": "Fichier non disponible ou expiré" }` — job absent ou pas
  encore `done`.
- `404 { "error": "Fichier expiré. Relance le téléchargement." }` — job
  `done` mais fichier disparu du disque (nettoyage TTL, redémarrage du
  conteneur, etc.).

---

## 4. Cycle de vie d'un job

```
starting ──▶ downloading ──▶ done ──▶ (supprimé après GET /file)
   │              │
   └──────────────┴──────────▶ error (fichier jamais créé ou supprimé)
```

- Un job est **automatiquement nettoyé après 30 minutes** s'il n'a jamais
  été récupéré via `/file` (thread de nettoyage, vérifié toutes les 10 min).
- Après un `GET /file` réussi, le job est retiré immédiatement — pas besoin
  d'attendre le TTL.
- Les jobs vivent **en mémoire du process** : un redémarrage du service
  (déploiement, crash, scale-to-zero) fait perdre tous les jobs en cours.
  Node doit gérer ce cas côté frontend (message « le téléchargement a
  expiré, relance-le »).

---

## 5. Intégration côté Node

Remplacer, dans `server.js` :

| Ancien (spawn local)                               | Nouveau (appel HTTP)                                     |
| -------------------------------------------------- | -------------------------------------------------------- |
| `ytdlpInfo(url)`                                   | `POST {VIDEO_SERVICE_URL}/analyze`                       |
| `spawn('yt-dlp', args)` dans `/api/download/start` | `POST {VIDEO_SERVICE_URL}/download/start`                |
| SSE local (`sseEmit`/`sseClients`)                 | proxy SSE vers `GET {VIDEO_SERVICE_URL}/progress/:jobId` |
| `fs.createReadStream` dans `/api/file/:jobId`      | proxy stream vers `GET {VIDEO_SERVICE_URL}/file/:jobId`  |

Exemple d'appel `analyze` :

```javascript
const r = await axios.post(
  `${process.env.VIDEO_SERVICE_URL}/analyze`,
  { url },
  {
    headers: { "X-Service-Secret": process.env.SERVICE_SECRET },
    timeout: 65_000,
  },
);
```

**Important pour le SSE** : `EventSource` (natif navigateur) ne permet pas
d'ajouter de headers custom. Deux options :

1. **Proxy SSE dans Node** : le frontend appelle `GET /api/progress/:jobId`
   sur Node (sans besoin d'auth spéciale, juste sa session normale), Node
   ouvre lui-même une requête vers `{VIDEO_SERVICE_URL}/progress/:jobId`
   avec le header secret, et relaie chaque `data:` reçu vers le client.
   C'est l'option la plus propre et la plus sûre (le service Python reste
   totalement invisible depuis l'extérieur).
2. **Exposition publique du service Python** avec un token à usage unique
   dans l'URL au lieu du header (plus de travail, à éviter sauf besoin
   spécifique).

→ Recommandation : option 1.

---

## 6. Déploiement en production

Le serveur de dev Flask (`app.run(...)`) **ne doit pas** servir de
production : une connexion SSE ouverte peut bloquer le traitement d'autres
requêtes selon le modèle de concurrence.

Lancer plutôt avec Gunicorn et des workers threadés (indispensable pour le
SSE) :

```bash
gunicorn --worker-class gthread --threads 8 --workers 2 \
  --timeout 120 --bind 0.0.0.0:$PORT video_service:app
```

- `--timeout 120` : évite que Gunicorn tue un worker pendant un
  téléchargement long ou une connexion SSE maintenue ouverte.
- `--worker-class gthread` : nécessaire, `sync` (par défaut) ne gère pas
  bien les connexions longues.
- Si l'hébergeur fait du scale-to-zero, préférer un cron **externe**
  (Render Cron Job, GitHub Action planifiée) qui appelle
  `POST /admin/update-ytdlp` plutôt que de compter sur le
  `BackgroundScheduler` in-process, qui ne tourne que si le service est
  éveillé à l'heure prévue (4h du matin).

Points de vigilance disque :

- `TMP_DIR` doit être un volume **persistant** entre requêtes tant qu'un
  job est en cours (sinon un fichier en cours de téléchargement disparaît
  si le conteneur redémarre).
- Prévoir une alerte disque : un service qui reçoit beaucoup de
  téléchargements simultanés sans clients qui viennent chercher le fichier
  peut accumuler jusqu'à 30 min de fichiers non réclamés avant le nettoyage
  automatique.

---

## 7. Codes d'erreur HTTP — résumé

| Code  | Signification                                                             |
| ----- | ------------------------------------------------------------------------- |
| `400` | Requête invalide (URL manquante/invalide, vidéo inaccessible côté yt-dlp) |
| `401` | `X-Service-Secret` manquant ou incorrect                                  |
| `404` | Job ou fichier introuvable/expiré                                         |
| `500` | `yt-dlp` introuvable sur le serveur (problème d'installation)             |
| `502` | Réponse `yt-dlp` illisible (JSON corrompu)                                |
| `504` | Timeout de l'analyse (60s)                                                |

---

## 8. Limitations connues

- Pas de persistance des jobs (perdus au redémarrage du process).
- Pas de limite de taille de téléchargement — à gérer côté Node
  (rate-limiting déjà en place) ou en ajoutant un contrôle de format/durée
  avant d'appeler `/download/start`.
- Un seul process = les jobs concurrents partagent le même CPU/bande
  passante ; à surveiller si le volume augmente (envisager une file
  d'attente type Redis/RQ si besoin de scaler horizontalement).
