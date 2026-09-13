bash

cat > /mnt/user-data/outputs/VIDEO_SERVICE.md << 'MDEOF'
# video-service — Documentation d'exploitation

Service interne Python/Flask dont le seul rôle est de piloter `yt-dlp` et
`ffmpeg` (analyse de vidéos, téléchargement, sous-titres, suivi de
progression, livraison de fichier). Il n'est **jamais** exposé directement
aux utilisateurs finaux : il n'est appelé que par le backend Node
(`server.js`), en machine-à-machine, via un secret partagé.

Il est **totalement agnostique du plan de l'utilisateur** (Free/Premium) :
aucune notion d'utilisateur, d'auth JWT, de paiement ou de quota n'existe
ici. Node décide QUI a le droit de demander QUOI (quelle qualité, combien
de sous-titres par jour, etc.) et envoie déjà les paramètres autorisés à ce
service, qui se contente de les exécuter. Voir §9 pour le détail de cette
répartition.

```
Navigateur ──▶ Node (server.js) ──▶ video-service (Flask) ──▶ yt-dlp / ffmpeg
              auth / paiement /      analyse / download /
              rate-limit / quotas /  progress / file / ssrf /
              plan Free ou Premium   concurrence / watchdog
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

**Binaires système requis** (installés hors pip, doivent être sur le
`PATH` ou dans `./ffmpeg/`) : `yt-dlp`, `ffmpeg`, **`ffprobe`**. `ffprobe`
est utilisé pour inspecter les flux du fichier téléchargé avant
finalisation (§3, `POST /download/start`) — son absence fait échouer
**tous** les téléchargements vidéo (pas seulement ceux avec sous-titres).

---

## 2. Authentification

Toutes les routes métier exigent le header :

```
X-Service-Secret: <valeur de SERVICE_SECRET>
```

Absent ou incorrect → `401 { "detail": "Non autorisé" }`.

Il n'y a **aucune notion d'utilisateur** dans ce service : Node doit avoir
déjà authentifié/rate-limité/autorisé l'appelant avant de relayer la
requête ici.

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

Analyse une URL vidéo sans la télécharger (`yt-dlp --dump-single-json`).

**Auth requise.** Body JSON :

```json
{ "url": "https://www.youtube.com/watch?v=..." }
```

**Protection SSRF** : l'URL est résolue en IP et rejetée si elle pointe vers
une adresse privée/réservée (loopback, RFC1918, link-local — dont
`169.254.169.254`, endpoint de métadonnées cloud AWS/GCP/Azure). Sans ce
filtre, n'importe qui pourrait sonder le réseau interne du serveur via cette
route. Renvoie `400 URL invalide` si la résolution échoue ou pointe en
interne.

**Réponse `200`** :

```json
{
  "success": true,
  "data": {
    "title": "Titre de la vidéo",
    "duration": 245,
    "uploader": "Nom de la chaîne",
    "thumbnail": "https://...",
    "subtitles": ["fr", "en", "es-orig"],
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

- `subtitles` : jusqu'à 10 codes de langue, triés par pertinence — langue
  originale (`-orig`) en premier, puis une liste de langues préférentielles
  (`fr, en, es, pt, pt-PT, de, it, ar, hi, zh-Hans, zh-Hant`), puis le reste
  jusqu'à 10. Combine sous-titres manuels et auto-générés.
- `formats` : jusqu'à 80 entrées, storyboards (`vcodec: images`, `ext: mhtml`)
  systématiquement exclus (sinon ils saturaient la limite et masquaient les
  vrais formats 4K/1440p).

**Erreurs possibles (toutes en `400` sauf mention contraire)** :

| Message                                                                          | Cause                                     |
| --------------------------------------------------------------------------------- | ------------------------------------------ |
| `URL manquante`                                                                  | body sans `url`                            |
| `URL invalide`                                                                   | protocole non http(s), ou IP privée (SSRF) |
| `Cette vidéo est privée et inaccessible.`                                       | vidéo privée                               |
| `Cette vidéo est réservée aux adultes (restriction d'âge).`                     | âge                                         |
| `Vidéo non disponible dans ta région.`                                          | geoblocking                                |
| `Vidéo indisponible (supprimée ou retirée).`                                    | supprimée                                  |
| `URL non reconnue ou site non supporté.`                                        | site non géré par yt-dlp                   |
| `Vidéo retirée pour violation de droits d'auteur.`                              | copyright                                  |
| `Cette vidéo requiert une connexion au compte de la plateforme.`                | login requis                               |
| `Contenu réservé aux membres / payant.`                                         | contenu payant                             |
| `La plateforme limite temporairement les requêtes (erreur 429)...`              | rate-limit YouTube côté plateforme         |
| `Accès refusé par la plateforme (erreur 403).`                                  | HTTP 403                                   |
| `Vidéo introuvable (erreur 404). Vérifie l'URL.`                                | HTTP 404                                   |
| `yt-dlp a mis trop de temps à répondre.`                                        | `504`, timeout 60s dépassé                 |
| `yt-dlp introuvable sur le serveur.`                                            | `500`, binaire absent                      |
| `Réponse yt-dlp invalide. Réessaie.`                                            | `502`, JSON illisible                      |

---

### `POST /download/start`

Démarre un téléchargement en tâche de fond (thread dédié). Répond
immédiatement, le suivi se fait ensuite via `/progress/:jobId`.

**Auth requise.** Body JSON :

```json
{
  "url": "https://www.youtube.com/watch?v=...",
  "quality": "1080p",
  "format": null,
  "title": "Nom du fichier souhaité",
  "sublang": "fr",
  "trim": false,
  "startTime": null,
  "endTime": null
}
```

| Champ       | Défaut                       | Description                                                                                                                                                    |
| ----------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`       | —                              | **Requis.** Validée + protection SSRF.                                                                                                                        |
| `quality`   | `"1080p"`                     | `4k` \| `2160p` \| `1440p` \| `1080p` \| `720p` \| `480p` \| `360p` \| `mp3`/`audio` (extraction audio). Traduit en sélecteur de format yt-dlp privilégiant H.264/AAC (compatible partout, copiable sans ré-encodage). |
| `format`    | `null`                        | Override brut du sélecteur yt-dlp (prioritaire sur `quality`). Validé par regex (`^[\w+,\-\[\]<>=./: ]+$`, 150 car. max) — défense en profondeur, même si Node valide déjà côté appelant. |
| `title`     | `"video"`                     | Nettoyé automatiquement (caractères spéciaux retirés, 80 car. max) — nom de fichier final.                                                                    |
| `sublang`   | `null`                        | Code de langue (2-8 car., lettres/tirets). Invalide → ignoré silencieusement (log `warning`), le téléchargement continue sans sous-titres.                    |
| `trim`      | `false`                       | `true` pour découper la vidéo. Nécessite `startTime`/`endTime`.                                                                                                |
| `startTime` / `endTime` | `null`            | Format `MM:SS` ou `H:MM:SS` (ex: `"00:10"`, `"1:32:07"`). `endTime` doit être strictement supérieur à `startTime`.                                            |

**Filet anti-DoS** : si `MAX_CONCURRENT_JOBS` (12 par défaut) jobs tournent
déjà, tous appelants confondus, la requête est rejetée immédiatement :

```json
{ "error": "Le serveur est actuellement très sollicité. Réessaie dans quelques instants.", "code": "SERVER_BUSY" }
```
→ `503`

**Réponse `200`** :

```json
{ "success": true, "jobId": "3f9a1c2b8e4d4a2f9b6c7d8e9f0a1b2c" }
```

**Erreurs** :

| Code | Cas |
|---|---|
| `400` | `URL manquante` / `URL invalide` (SSRF inclus) |
| `400` | `Format de temps invalide. Utilise MM:SS (ex : 00:10).` — `code: INVALID_TRIM_TIME` |
| `400` | `L'heure de fin doit être supérieure à l'heure de début.` — `code: INVALID_TRIM_TIME` |
| `400` | `Format de téléchargement invalide.` — `format` ne passe pas la validation regex |
| `503` | `SERVER_BUSY` — concurrence maximale atteinte |

> Une fois le job accepté (`200`), il n'échoue plus à ce stade — les
> erreurs de téléchargement/finalisation apparaissent plus tard via le SSE
> ou la route `/progress`.

**Déroulé interne d'un job vidéo (non-audio)** :
1. Téléchargement yt-dlp (vidéo + audio), progression agrégée multi-flux
   (voir §4).
2. **Watchdog** : si le job dépasse 15 minutes, il est tué et marqué en
   erreur (`Délai dépassé : le téléchargement a pris trop de temps. Réessaie.`).
3. Si `sublang` fourni : téléchargement du fichier de sous-titres **séparé**
   de la vidéo, avec jusqu'à 5 tentatives en cas de 429 (fréquent sur
   l'endpoint de sous-titres YouTube, en particulier les légendes
   auto-traduites). Best-effort : un échec n'annule jamais la vidéo,
   simplement livrée sans sous-titres.
4. Finalisation ffmpeg (`finalize_video`) : copie H.264/AAC si déjà
   compatible (rapide), sinon ré-encodage ; embed du `.srt` en piste
   `mov_text` si récupéré à l'étape 3 ; `+faststart` pour lecture immédiate.

Pour l'audio (`quality: "mp3"` ou `"audio"`), l'étape 4 devient un
ré-encodage direct en MP3 (`libmp3lame`), pas de sous-titres.

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
// progression (agrégée sur l'ensemble des flux vidéo+audio, monotone 0→99%)
{ "type": "progress", "percent": 45.3, "stream": 1, "streams": 2, "total": "78.50MiB", "speed": "1.23MiB/s", "eta": "00:45" }

// finalisation en cours (après le téléchargement, pendant embed sous-titres/ré-encodage)
{ "type": "processing", "percent": 99, "message": "Récupération des sous-titres et finalisation…" }

// terminé
{ "type": "done", "jobId": "3f9a1c2b...", "title": "Nom du fichier", "ext": "mp4" }

// erreur
{ "type": "error", "message": "Cette vidéo est privée et inaccessible." }
```

`404 { "error": "Job introuvable" }` si le `job_id` n'existe pas (jamais
créé, ou déjà nettoyé après 15 min / après livraison du fichier).

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
starting ──▶ downloading ──▶ converting ──▶ done ──▶ (supprimé après GET /file)
   │              │               │
   └──────────────┴───────────────┴──────────▶ error (fichier jamais créé, supprimé, ou finalisation échouée)
```

- `converting` correspond à l'étape de finalisation ffmpeg (embed
  sous-titres / ré-encodage / remux) — le SSE émet un événement `processing`
  pendant cette phase, avec `percent: 99`.
- Un job est **automatiquement nettoyé après 15 minutes** s'il n'a jamais
  été récupéré via `/file` (thread de nettoyage, vérifié toutes les 10 min).
  Ce même thread balaie aussi le **disque** (`TMP_DIR`) au-delà de la Map en
  mémoire, pour rattraper les fichiers orphelins laissés par un crash/
  redémarrage précédent.
- Après un `GET /file` réussi, le job est retiré immédiatement — pas besoin
  d'attendre le TTL.
- Un **watchdog** tue tout job dépassant 15 minutes de traitement actif
  (indépendant du TTL de nettoyage post-job).
- Les jobs vivent **en mémoire du process** : un redémarrage du service
  (déploiement, crash, scale-to-zero) fait perdre tous les jobs en cours.
  Node doit gérer ce cas côté frontend (message « le téléchargement a
  expiré, relance-le »).

---

## 5. Intégration côté Node

Remplacer, dans `server.js` :

| Ancien (spawn local)                               | Nouveau (appel HTTP)                                     |
| --------------------------------------------------- | ---------------------------------------------------------- |
| `ytdlpInfo(url)`                                     | `POST {VIDEO_SERVICE_URL}/analyze`                         |
| `spawn('yt-dlp', args)` dans `/api/download/start`   | `POST {VIDEO_SERVICE_URL}/download/start`                  |
| SSE local (`sseEmit`/`sseClients`)                   | proxy SSE vers `GET {VIDEO_SERVICE_URL}/progress/:jobId`   |
| `fs.createReadStream` dans `/api/file/:jobId`        | proxy stream vers `GET {VIDEO_SERVICE_URL}/file/:jobId`    |

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
  --timeout 120 --bind 0.0.0.0:$PORT main:app
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
  peut accumuler jusqu'à 15 min de fichiers non réclamés avant le nettoyage
  automatique — le balayage disque périodique (§4) réduit ce risque mais ne
  l'élimine pas en cas de pic soutenu.

---

## 7. Codes d'erreur HTTP — résumé

| Code  | Signification                                                             |
| ----- | ---------------------------------------------------------------------------- |
| `400` | Requête invalide (URL manquante/invalide/SSRF, trim invalide, format invalide, vidéo inaccessible côté yt-dlp) |
| `401` | `X-Service-Secret` manquant ou incorrect                                     |
| `404` | Job ou fichier introuvable/expiré                                            |
| `500` | `yt-dlp` introuvable sur le serveur (problème d'installation)                |
| `502` | Réponse `yt-dlp` illisible (JSON corrompu)                                   |
| `503` | `SERVER_BUSY` — trop de jobs concurrents (`MAX_CONCURRENT_JOBS` atteint)     |
| `504` | Timeout de l'analyse (60s)                                                   |

---

## 8. Limitations connues

- Pas de persistance des jobs (perdus au redémarrage du process).
- Pas de limite de **taille** de téléchargement — seule la durée vidéo (côté
  Node, avant l'appel) et la limite de concurrence globale (§3) protègent le
  disque/la bande passante.
- Un seul process = les jobs concurrents partagent le même CPU/bande
  passante ; à surveiller si le volume augmente (envisager une file
  d'attente type Redis/RQ si besoin de scaler horizontalement).
- `MAX_CONCURRENT_JOBS` est une limite **globale**, sans distinction
  Free/Premium — voir §9 pour la discussion sur une éventuelle priorisation.

---

## 9. Répartition Free vs Premium

Ce service ignore totalement l'existence de plans tarifaires. **Toute la
logique d'autorisation vit dans Node** (via la table `users.plan` en base),
qui choisit les valeurs à envoyer dans le body de `/download/start` selon
le plan de l'utilisateur. Le `video-service` exécute fidèlement ce qu'on
lui demande, sans re-vérifier "est-ce que cet utilisateur a le droit ?" —
ce n'est pas son rôle et il n'a pas l'information pour le faire.

### Ce que Node doit décider AVANT d'appeler `/download/start`

| Paramètre                | Free                                                                 | Premium                                  |
| ------------------------- | --------------------------------------------------------------------- | ------------------------------------------ |
| `quality`                 | Plafonnée à `1080p` — rejeter la requête utilisateur (`403 PREMIUM_REQUIRED`) si `4k`/`2160p`/`1440p` demandé, **avant** d'appeler ce service. | Libre, y compris `4k`/`2160p`/`1440p`.     |
| `format` (override brut)  | Toujours `null` — ne jamais transmettre un `format` fourni par un utilisateur Free (même s'il en envoie un, l'ignorer côté Node). | Peut être transmis tel quel si fourni.     |
| `sublang`                 | Autorisé, mais Node doit vérifier un quota journalier (ex: 5/jour) en base **avant** d'appeler ce service ; au-delà, répondre `429 SUBTITLE_LIMIT_REACHED` sans jamais atteindre `/download/start`. | Illimité, pas de vérification de quota.    |
| `trim`/`startTime`/`endTime` | Autorisé avec un nombre d'essais limité (ex: 3 au total, compteur en base) ; au-delà, `403 TRIM_LIMIT_REACHED`. Nécessite un compte (pas anonyme). | Illimité.                                  |
| Durée de la vidéo         | Node doit appeler `/analyze` **d'abord**, lire `data.duration`, et rejeter (`403 DURATION_LIMIT_REACHED`) si elle dépasse un plafond (ex: 3h) avant même de proposer le téléchargement. | Pas de plafond de durée.                   |
| Téléchargements/jour      | Quota journalier (ex: 30/jour) vérifié en base par Node avant l'appel ; au-delà, `429 DAILY_LIMIT_REACHED`. | Illimité.                                  |

### Pourquoi cette répartition et pas l'inverse

- **Le `video-service` reste réutilisable** pour n'importe quel produit
  (site web actuel, bot Telegram, une future appli mobile) sans jamais
  dupliquer la logique de plan à plusieurs endroits — un seul endroit
  (Node + DB) connaît la vérité sur qui est Premium.
- **Défense en profondeur minimale côté service** : la validation regex sur
  `format` (§3) empêche un Node compromis ou buggé d'envoyer une chaîne
  pathologique, mais ce n'est PAS un contrôle d'autorisation — juste un
  garde-fou anti-abus technique (temps de résolution yt-dlp anormal).
  Le service ne doit jamais devenir le lieu où on ajoute "if plan ===
  premium" : ça romprait l'agnosticisme voulu.

### Concurrence : Free et Premium se partagent le même pool

`MAX_CONCURRENT_JOBS` (§8) ne distingue pas les plans — un utilisateur Free
peut occuper un des 12 slots au même titre qu'un Premium. C'est une
limitation connue et acceptée pour l'instant. Si le volume grossit et que
les utilisateurs Premium doivent être priorisés, deux pistes possibles
**à implémenter plus tard, pas maintenant** :

1. Réserver N slots exclusivement aux requêtes marquées `priority: true`
   dans le body de `/download/start` (nouveau champ, toujours décidé par
   Node selon `req.user.plan`), avec une file d'attente FIFO séparée pour
   le reste.
2. Faire tourner deux instances du `video-service` (une pour Premium, une
   pour Free) derrière un routage Node basé sur le plan — plus lourd
   opérationnellement mais isole complètement les ressources.

Aucune des deux n'est implémentée actuellement ; `MAX_CONCURRENT_JOBS` est
un plafond global partagé.
MDEOF
echo "written: $(wc -l < /mnt/user-data/outputs/VIDEO_SERVICE.md) lines"
Output

written: 498 lines