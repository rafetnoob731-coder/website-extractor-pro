# Website Extractor Pro v3.0

Enterprise-grade web extraction engine built with Flask. Extracts full websites including HTML, CSS, JavaScript, images, and fonts — packaged as a downloadable ZIP.

## Features

- **Full Website Extraction** — Crawls pages up to configurable depth
- **Smart Authentication** — GET, POST, and Cookie-based login support
- **Asset Downloading** — Automatically downloads CSS, JS, images, fonts
- **Concurrent Crawling** — Multi-threaded extraction for speed
- **ZIP Packaging** — One-click download with metadata
- **RESTful API** — Full JSON API for programmatic use
- **Real-time Progress** — Web UI with live status updates
- **Dark/Light Theme** — Modern glassmorphism UI
- **Extraction History** — SQLite-backed persistent history

## Quick Start

```bash
pip install -r requirements.txt
python app.py
```

Open http://localhost:5000 in your browser.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/extract` | Start a new extraction |
| GET | `/api/status/:id` | Get extraction status |
| GET | `/api/download/:id` | Download extracted ZIP |
| POST | `/api/cancel/:id` | Cancel running extraction |
| GET | `/api/history` | List past extractions |
| GET | `/api/info` | Server and config info |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | auto | Flask secret key |
| `TEMP_DIR` | temp_extractions | Working directory |
| `MAX_PAGES` | 100 | Max pages per extraction |
| `MAX_DEPTH` | 5 | Max crawl depth |
| `PORT` | 5000 | Server port |
| `LOG_LEVEL` | INFO | Logging level |

## Deployment

### Render

Create a new Web Service on Render, connect your repo, and Render will automatically use the `render.yaml` configuration.

### Manual

```bash
gunicorn app:app --workers 4 --bind 0.0.0.0:$PORT
```

## License

MIT
