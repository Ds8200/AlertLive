# 🚀 RocketIL — התרעות בזמן אמת

מערכת ניטור התרעות בזמן אמת מ-API של [rocketil.live](https://rocketil.live), המציגה נתונים על מפה אינטראקטיבית עם עדכונים חיים דרך WebSocket.

---

## תוכן עניינים

- [סקירה כללית](#סקירה-כללית)
- [ארכיטקטורה](#ארכיטקטורה)
- [מבנה תיקיות](#מבנה-תיקיות)
- [דרישות מערכת](#דרישות-מערכת)
- [הרצה מקומית](#הרצה-מקומית)
- [הרצה עם Docker](#הרצה-עם-docker)
- [משתני סביבה](#משתני-סביבה)
- [API Endpoints](#api-endpoints)
- [פיצ'רים של הדשבורד](#פיצרים-של-הדשבורד)
- [מפת צבעים](#מפת-צבעים)
- [הרחבה עתידית](#הרחבה-עתידית)

---

## סקירה כללית

השרת מבצע polling אוטומטי ל-API של rocketil כל מספר שניות, מנקה ומאמת כל התרעה, ומשדר אותה בזמן אמת לכל הלקוחות המחוברים דרך WebSocket. הדשבורד מציג את ההתרעות על מפת ישראל אינטראקטיבית ובסיידבר עם לוג.

```
rocketil API ──► poller (asyncio) ──► broadcaster ──► WebSocket ──► dashboard (דפדפן)
```

**אין Kafka, אין Redis, אין תורי הודעות** — FastAPI לבדו מספיק לעומס של מערכת התרעות.

---

## ארכיטקטורה

| שכבה | קובץ | תפקיד |
|------|------|--------|
| שרת | `app/main.py` | FastAPI app, lifespan, ניתוב |
| הגדרות | `app/core/config.py` | משתני סביבה (pydantic-settings) |
| Poller | `app/core/poller.py` | לולאת asyncio — polling לAPI כל N שניות |
| Fetcher | `app/services/fetcher.py` | קריאת HTTP ל-rocketil עם httpx |
| Processor | `app/services/processor.py` | ניקוי, פיצול payload_json, ולידציה |
| Broadcaster | `app/services/broadcaster.py` | ניהול חיבורי WebSocket ושידור לכולם |
| מודל | `app/models/alert.py` | Pydantic Alert — ולידציה ו-serialization |
| Routes | `app/api/routes/ws.py` | נקודת קצה WebSocket (`/ws`) |
| Routes | `app/api/routes/health.py` | בדיקת תקינות (`/health`) |
| Frontend | `app/templates/dashboard.html` | דשבורד מלא — מפה + לוג + התרעות |

---

## מבנה תיקיות

```
rocketil_v2/
├── app/
│   ├── main.py                  # נקודת כניסה — FastAPI + lifespan
│   ├── core/
│   │   ├── config.py            # הגדרות מ-.env
│   │   └── poller.py            # לולאת polling רקע
│   ├── api/
│   │   └── routes/
│   │       ├── ws.py            # /ws — WebSocket endpoint
│   │       └── health.py        # /health — בדיקת תקינות
│   ├── services/
│   │   ├── fetcher.py           # HTTP async לrocketil API
│   │   ├── processor.py         # עיבוד וניקוי נתונים
│   │   └── broadcaster.py       # ConnectionManager — שידור לכל הלקוחות
│   ├── models/
│   │   └── alert.py             # מודל Alert עם pydantic
│   └── templates/
│       └── dashboard.html       # הדשבורד המלא (מוגש מ-FastAPI)
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env                         # (לא בגיט) — משתני סביבה
```

---

## דרישות מערכת

- Python 3.12+
- או Docker

---

## הרצה מקומית

### 1. שכפל והיכנס לתיקייה

```bash
git clone <repo-url>
cd rocketil_v2
```

### 2. צור סביבה וירטואלית

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux / macOS:
source venv/bin/activate
```

### 3. התקן תלויות

```bash
pip install -r requirements.txt
```

### 4. הגדרות (אופציונלי)

צור קובץ `.env` בתיקייה הראשית (כל ערך אופציונלי — יש ברירות מחדל):

```env
API_URL=https://api.rocketil.live/api/alerts
POLL_INTERVAL=5
HOST=0.0.0.0
PORT=8000
```

### 5. הפעל את השרת

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 6. פתח את הדשבורד

```
http://localhost:8000
```

---

## הרצה עם Docker

### build והרצה בפקודה אחת

```bash
docker compose up --build
```

### או בנפרד

```bash
# בנייה
docker build -t rocketil .

# הרצה
docker run -p 8000:8000 rocketil

# עם קובץ .env
docker run -p 8000:8000 --env-file .env rocketil
```

הדשבורד יהיה זמין בכתובת: `http://localhost:8000`

---

## משתני סביבה

| משתנה | ברירת מחדל | תיאור |
|-------|-----------|-------|
| `API_URL` | `https://api.rocketil.live/api/alerts` | כתובת ה-API של rocketil |
| `POLL_INTERVAL` | `5` | מרווח בשניות בין כל polling |
| `HOST` | `0.0.0.0` | כתובת האזנה של השרת |
| `PORT` | `8000` | פורט השרת |

---

## API Endpoints

| Method | נתיב | תיאור |
|--------|------|-------|
| `GET` | `/` | מגיש את `dashboard.html` |
| `WS` | `/ws` | WebSocket — מקבל התרעות בזמן אמת |
| `GET` | `/health` | תקינות השרת ומספר לקוחות מחוברים |

### תגובת `/health`

```json
{
  "status": "ok",
  "clients": 3
}
```

### מבנה הודעת WebSocket (JSON)

```json
{
  "event_id": "134182577110000000",
  "alert_id": "mmv59je4-k7rj9w1",
  "type": "rockets",
  "severity": "warn",
  "region_name": "אשקלון",
  "oref_city": "אשקלון",
  "lat": 31.6688,
  "lng": 34.5743,
  "timestamp": "2026-03-18T14:22:00+03:00"
}
```

---

## פיצ'רים של הדשבורד

### מפה
- מפה אינטראקטיבית של ישראל (Leaflet)
- נקודות צבעוניות לפי רמת האיום
- **דה-דופ לפי אזור** — מוצג סמן אחד בלבד לכל אזור, מתעדכן עם כל התרעה חדשה
- נקודות פעילות מבצעות אנימציית פולס
- לחיצה על כרטיס בסיידבר → מעוף למפה לנקודה
- **8 סוגי מפות** להחלפה: כהה, OSM (עברית), בהיר, רחובות, טופוגרפי, לווין, לווין+שמות, Stadia

### לוג התרעות
- רשימה כרונולוגית של כל ההתרעות
- כרטיס לכל התרעה עם עיר, סוג, חומרה וזמן
- מקסימום 100 כרטיסים — הישנים מוסרים אוטומטית
- כפתור "נקה הכל"

### מיקום המשתמש
- בכל טעינת דף מתבקש מיקום GPS אוטומטית
- **סמן pin כחול** על המפה עם popup המציג קואורדינטות מדויקות ודיוק GPS
- **התרעת קרבה** — אם מגיעה התרעה במרחק עד 50 ק"מ מהמיקום:
  - נפתח חלונית מהבהבת עם פרטי ההתרעה ומרחק
  - **אזעקה** מנגנת דרך Web Audio API (סירן) — פעם אחת בלבד עד לחיצת "הבנתי"

### חיבור
- חיבור WebSocket עם reconnect אוטומטי כל 3 שניות בניתוק
- אינדיקטור חיבור חי בהדר
- עדכון זמן אמת ללא צורך ברענון

### ריספונסיביות
- מותאם לדסקטופ, טאבלט ופלאפון
- במובייל: מפה למעלה, לוג למטה, הדר מתקפל לשתי שורות

---

## מפת צבעים

| צבע | סוג איום |
|-----|---------|
| 🔴 אדום | טילים, רקטות, כלי טיס עוין, פיגוע, חדירה, `critical` |
| 🟠 כתום | `warn` / `warning` |
| 🟡 צהוב | `info` |
| 🟢 ירוק | האירוע הסתיים (`update`) |
| 🟣 סגול | רעידת אדמה, צונאמי, חומרים מסוכנים |
| 🔵 כחול | ברירת מחדל |

---

## הרחבה עתידית

| צורך | פתרון מוצע |
|------|-----------|
| כמה instances של השרת | החלף `broadcaster.py` ב-Redis pub/sub |
| שמירת היסטוריה | הוסף שכבת DB (SQLite / PostgreSQL עם SQLAlchemy) |
| אימות משתמשים | JWT middleware ב-FastAPI |
| התראות Push | Web Push API + service worker |
| רדיוס התרעה מותאם אישית | הוסף שדה קלט בדשבורד ושמור ב-localStorage |
