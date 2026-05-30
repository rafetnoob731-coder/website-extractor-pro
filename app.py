#!/usr/bin/env python3
"""
Website Extractor Pro v3.0
Enterprise-grade web extraction engine with advanced features.
"""

import os
import re
import json
import time
import uuid
import zlib
import hashlib
import logging
import logging.handlers
import asyncio
import shutil
import zipfile
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Tuple, Set, Any, Callable
from urllib.parse import urljoin, urlparse, urlencode
from dataclasses import dataclass, field, asdict
from enum import Enum
from functools import wraps
from io import BytesIO

import requests
from bs4 import BeautifulSoup, Comment
from flask import Flask, render_template, request, send_file, jsonify, session, g
from flask_cors import CORS
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from cachetools import TTLCache


# ============================================================================
# Configuration
# ============================================================================

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32).hex())
    TEMP_DIR = os.environ.get('TEMP_DIR', 'temp_extractions')
    MAX_EXTRACTIONS = int(os.environ.get('MAX_EXTRACTIONS', '50'))
    MAX_PAGES = int(os.environ.get('MAX_PAGES', '100'))
    MAX_DEPTH = int(os.environ.get('MAX_DEPTH', '5'))
    REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', '60'))
    CLEANUP_INTERVAL = int(os.environ.get('CLEANUP_INTERVAL', '300'))
    MAX_FILE_SIZE = int(os.environ.get('MAX_FILE_SIZE', str(50 * 1024 * 1024)))
    ENABLE_API_AUTH = os.environ.get('ENABLE_API_AUTH', 'false').lower() == 'true'
    API_KEYS = set(os.environ.get('API_KEYS', '').split(',')) if os.environ.get('API_KEYS') else set()
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
    USER_AGENT = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )


# ============================================================================
# Logging Setup
# ============================================================================

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            'extractor.log', maxBytes=10*1024*1024, backupCount=5
        ),
    ]
)
logger = logging.getLogger('extractor')


# ============================================================================
# Flask Application
# ============================================================================

app = Flask(__name__)
app.secret_key = Config.SECRET_KEY
app.config['MAX_CONTENT_LENGTH'] = Config.MAX_FILE_SIZE
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
CORS(app, resources={r"/api/*": {"origins": "*"}})

os.makedirs(Config.TEMP_DIR, exist_ok=True)


# ============================================================================
# Database
# ============================================================================

class Database:
    def __init__(self, db_path: str = 'extractions.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS extractions (
                    id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    pages INTEGER DEFAULT 0,
                    total_size INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    metadata TEXT DEFAULT '{}'
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    extraction_id TEXT,
                    url TEXT NOT NULL,
                    filename TEXT,
                    status TEXT DEFAULT 'pending',
                    size INTEGER DEFAULT 0,
                    depth INTEGER DEFAULT 0,
                    error TEXT,
                    FOREIGN KEY (extraction_id) REFERENCES extractions(id)
                )
            ''')
            conn.commit()

    def create_extraction(self, extraction_id: str, url: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR IGNORE INTO extractions (id, url) VALUES (?, ?)',
                (extraction_id, url)
            )
            conn.commit()

    def update_extraction(self, extraction_id: str, **kwargs: Any) -> None:
        if not kwargs:
            return
        sets = ', '.join(f'{k} = ?' for k in kwargs)
        values = list(kwargs.values())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                f'UPDATE extractions SET {sets} WHERE id = ?',
                [*values, extraction_id]
            )
            conn.commit()

    def get_extraction(self, extraction_id: str) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                'SELECT * FROM extractions WHERE id = ?', (extraction_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_extractions(self, limit: int = 20) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                'SELECT * FROM extractions ORDER BY created_at DESC LIMIT ?',
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]


db = Database()


# ============================================================================
# Custom Exceptions
# ============================================================================

class ExtractionError(Exception):
    pass


class LoginError(ExtractionError):
    pass


class TimeoutError(ExtractionError):
    pass


class MaxPagesReached(ExtractionError):
    pass


class RateLimitError(ExtractionError):
    pass


# ============================================================================
# Models
# ============================================================================

@dataclass
class PageInfo:
    url: str
    filename: str
    size: int = 0
    depth: int = 0
    content_type: str = 'text/html'
    status_code: int = 200
    error: Optional[str] = None


@dataclass
class ExtractionResult:
    job_id: str
    url: str
    pages: List[PageInfo] = field(default_factory=list)
    total_pages: int = 0
    total_size: int = 0
    duration: float = 0.0
    status: str = 'pending'
    error: Optional[str] = None


class ProgressTracker:
    def __init__(self, total: int = 100, callback: Optional[Callable] = None):
        self.total = total
        self.current = 0
        self.callback = callback
        self._lock = threading.Lock()

    def update(self, increment: int = 1, message: Optional[str] = None) -> None:
        with self._lock:
            self.current = min(self.current + increment, self.total)

    @property
    def percentage(self) -> float:
        return (self.current / self.total * 100) if self.total > 0 else 0


# ============================================================================
# URL Utilities
# ============================================================================

class URLUtils:
    @staticmethod
    def normalize(url: str) -> str:
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip('/') or '/'
        return f'{scheme}://{netloc}{path}'

    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        return urlparse(url1).netloc.lower() == urlparse(url2).netloc.lower()

    @staticmethod
    def is_valid_url(url: str) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    @staticmethod
    def get_domain(url: str) -> str:
        return urlparse(url).netloc.lower()

    @staticmethod
    def should_skip(href: str) -> bool:
        skip_patterns = (
            r'^(#|javascript:|mailto:|tel:|whatsapp:|sms:)',
            r'\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|tar|gz)$',
            r'(logout|signout|sign-?out|exit|quit)',
            r'\.(jpg|jpeg|png|gif|svg|ico|webp|mp4|mp3|avi|mov)$',
        )
        return any(re.match(p, href, re.IGNORECASE) for p in skip_patterns)


# ============================================================================
# Rate Limiter
# ============================================================================

class RateLimiter:
    def __init__(self, requests_per_second: float = 2.0):
        self.min_interval = 1.0 / requests_per_second
        self.last_request: Dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, domain: str) -> None:
        with self._lock:
            last = self.last_request.get(domain, 0)
            elapsed = time.time() - last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request[domain] = time.time()


# ============================================================================
# Session Manager
# ============================================================================

class SessionManager:
    def __init__(self):
        self._sessions: Dict[str, requests.Session] = {}
        self._lock = threading.Lock()

    def get_session(self, job_id: str) -> requests.Session:
        if job_id not in self._sessions:
            with self._lock:
                if job_id not in self._sessions:
                    session = requests.Session()
                    session.headers.update({
                        'User-Agent': Config.USER_AGENT,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    })
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=10,
                        pool_maxsize=20,
                        max_retries=3
                    )
                    session.mount('https://', adapter)
                    session.mount('http://', adapter)
                    self._sessions[job_id] = session
        return self._sessions[job_id]

    def cleanup(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._sessions:
                self._sessions[job_id].close()
                del self._sessions[job_id]


session_manager = SessionManager()
rate_limiter = RateLimiter()


# ============================================================================
# Login Handlers
# ============================================================================

class LoginHandler:
    @staticmethod
    def get_login_forms(soup: BeautifulSoup) -> List[Dict[str, Any]]:
        forms = []
        for form in soup.find_all('form'):
            inputs = {}
            for inp in form.find_all(['input', 'button']):
                name = inp.get('name')
                if name:
                    inp_type = inp.get('type', 'text')
                    value = inp.get('value', '')
                    inputs[name] = {'type': inp_type, 'value': value}
            if inputs:
                forms.append({
                    'action': form.get('action', ''),
                    'method': form.get('method', 'get').lower(),
                    'inputs': inputs
                })
        return forms

    @staticmethod
    def try_get_login(session: requests.Session, url: str,
                      username: str, password: str) -> Optional[requests.Response]:
        strategies = [
            {'username': username, 'password': password},
            {'user': username, 'pass': password},
            {'email': username, 'password': password},
            {'login': username, 'pwd': password},
            {'user_login': username, 'user_pass': password},
            {'log': username, 'pwd': password},
            {'user_email': username, 'user_pass': password},
            {'user_name': username, 'user_pwd': password},
        ]
        for params in strategies:
            try:
                resp = session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
                if resp.status_code == 200 and LoginHandler._is_successful(resp):
                    return resp
            except Exception:
                continue
        return None

    @staticmethod
    def try_post_login(session: requests.Session, url: str,
                       username: str, password: str) -> Optional[requests.Response]:
        try:
            resp = session.get(url, timeout=Config.REQUEST_TIMEOUT)
            soup = BeautifulSoup(resp.text, 'html.parser')
            forms = LoginHandler.get_login_forms(soup)

            for form in forms:
                if form['method'] == 'post':
                    data = {}
                    for name, info in form['inputs'].items():
                        if info['type'] in ('text', 'email', 'username'):
                            data[name] = username
                        elif info['type'] == 'password':
                            data[name] = password
                        elif info['value']:
                            data[name] = info['value']

                    action_url = urljoin(url, form['action']) if form['action'] else url
                    try:
                        resp = session.post(action_url, data=data,
                                            timeout=Config.REQUEST_TIMEOUT)
                        if LoginHandler._is_successful(resp):
                            return resp
                    except Exception:
                        continue
        except Exception:
            pass
        return None

    @staticmethod
    def try_cookie_login(session: requests.Session, url: str,
                         cookies: Dict[str, str]) -> Optional[requests.Response]:
        try:
            for key, value in cookies.items():
                session.cookies.set(key, value)
            resp = session.get(url, timeout=Config.REQUEST_TIMEOUT)
            if resp.status_code == 200 and LoginHandler._is_successful(resp):
                return resp
        except Exception:
            pass
        return None

    @staticmethod
    def _is_successful(response: requests.Response) -> bool:
        text = response.text.lower()
        success = ['dashboard', 'welcome', 'account', 'profile',
                    'logged in', 'logout', 'my account', 'access granted']
        failure = ['login failed', 'invalid', 'error 405',
                   'method not allowed', 'access denied']

        for word in failure:
            if word in text:
                return False
        for word in success:
            if word in text:
                return True
        return 'username' not in text and 'password' not in text


# ============================================================================
# Main Engine
# ============================================================================

class ExtractionEngine:
    def __init__(self, url: str, username: str = '', password: str = '',
                 job_id: Optional[str] = None, cookies: Optional[Dict[str, str]] = None,
                 max_pages: int = Config.MAX_PAGES, max_depth: int = Config.MAX_DEPTH):
        self.url = URLUtils.normalize(url)
        self.domain = URLUtils.get_domain(self.url)
        self.username = username
        self.password = password
        self.cookies = cookies or {}
        self.job_id = job_id or str(uuid.uuid4())[:12]
        self.max_pages = max(max_pages, 10)
        self.max_depth = min(max_depth, 10)

        self.output_dir = os.path.join(Config.TEMP_DIR, self.job_id)
        self.assets_dir = os.path.join(self.output_dir, 'assets')
        self.session = session_manager.get_session(self.job_id)
        self.rate_limiter = rate_limiter

        self.pages: List[PageInfo] = []
        self.visited: Set[str] = set()
        self.progress = ProgressTracker(total=self.max_pages)
        self.status = 'initialized'
        self.start_time: Optional[float] = None
        self.error: Optional[str] = None
        self._stop_event = threading.Event()

        os.makedirs(self.assets_dir, exist_ok=True)

    def stop(self) -> None:
        self._stop_event.set()
        self.status = 'stopped'

    @property
    def elapsed(self) -> float:
        if self.start_time:
            return time.time() - self.start_time
        return 0.0

    def authenticate(self) -> bool:
        if not self.username or not self.password:
            return True

        self.status = 'authenticating'
        logger.info(f'[{self.job_id}] Authenticating to {self.domain}')

        if self.cookies:
            result = LoginHandler.try_cookie_login(
                self.session, self.url, self.cookies)
            if result:
                logger.info(f'[{self.job_id}] Cookie login successful')
                return True

        result = LoginHandler.try_get_login(
            self.session, self.url, self.username, self.password)
        if result:
            logger.info(f'[{self.job_id}] GET login successful')
            return True

        result = LoginHandler.try_post_login(
            self.session, self.url, self.username, self.password)
        if result:
            logger.info(f'[{self.job_id}] POST login successful')
            return True

        logger.error(f'[{self.job_id}] All login methods failed')
        self.error = 'Login failed'
        return False

    def extract(self) -> ExtractionResult:
        self.start_time = time.time()
        self.status = 'extracting'

        try:
            if not self.authenticate():
                self.status = 'login_failed'
                return self._build_result()

            self._crawl(self.url, 0)
            self._download_assets()

            self.status = 'completed'
            logger.info(
                f'[{self.job_id}] Extracted {len(self.pages)} pages '
                f'in {self.elapsed:.1f}s'
            )

        except MaxPagesReached:
            self.status = 'completed'
            logger.info(f'[{self.job_id}] Max pages reached')
        except ExtractionError as e:
            self.status = 'error'
            self.error = str(e)
            logger.error(f'[{self.job_id}] Error: {e}')
        except Exception as e:
            self.status = 'error'
            self.error = str(e)
            logger.exception(f'[{self.job_id}] Unexpected error')

        return self._build_result()

    def _crawl(self, url: str, depth: int) -> None:
        if self._stop_event.is_set():
            return
        if len(self.pages) >= self.max_pages:
            raise MaxPagesReached()
        if depth > self.max_depth:
            return
        if url in self.visited:
            return

        self.visited.add(url)
        self.rate_limiter.wait(self.domain)

        try:
            resp = self.session.get(url, timeout=Config.REQUEST_TIMEOUT)
            resp.raise_for_status()

            content_type = resp.headers.get('content-type', '').split(';')[0]
            if 'text/html' not in content_type:
                return

            filename = f'page_{len(self.pages) + 1:04d}.html'
            filepath = os.path.join(self.output_dir, filename)

            clean_html = self._clean_html(resp.text)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(clean_html)

            page = PageInfo(
                url=url,
                filename=filename,
                size=len(clean_html),
                depth=depth,
                content_type=content_type,
                status_code=resp.status_code,
            )
            self.pages.append(page)
            self.progress.update(1)

            if depth < self.max_depth:
                soup = BeautifulSoup(resp.text, 'html.parser')
                links = self._extract_links(soup, url)

                with ThreadPoolExecutor(max_workers=5) as executor:
                    fut_to_link = {
                        executor.submit(self._crawl, link, depth + 1): link
                        for link in links if link not in self.visited
                    }
                    for future in as_completed(fut_to_link):
                        try:
                            future.result()
                        except MaxPagesReached:
                            executor.shutdown(wait=False)
                            raise
                        except Exception as e:
                            logger.debug(f'Link error: {e}')

        except requests.Timeout:
            logger.warning(f'Timeout: {url[:80]}')
        except requests.RequestException as e:
            logger.debug(f'Request failed: {url[:60]} - {e}')

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> Set[str]:
        links = set()
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            if URLUtils.should_skip(href):
                continue
            full_url = urljoin(base_url, href)
            if (URLUtils.is_same_domain(base_url, full_url)
                    and full_url not in self.visited
                    and URLUtils.is_valid_url(full_url)):
                links.add(full_url)
        return links

    def _clean_html(self, html: str) -> str:
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style', 'noscript']):
            tag.decompose()
        for comment in soup.find_all(text=lambda t: isinstance(t, Comment)):
            comment.extract()
        return str(soup)

    def _download_assets(self) -> None:
        self.status = 'downloading_assets'
        asset_types = {
            'css': ('link', {'rel': 'stylesheet'}, 'href'),
            'js': ('script', {'src': True}, 'src'),
            'images': ('img', {'src': True}, 'src'),
            'fonts': ('link', {'rel': re.compile(r'font|preload')}, 'href'),
        }

        for folder, (tag, attrs, attr_name) in asset_types.items():
            folder_path = os.path.join(self.assets_dir, folder)
            os.makedirs(folder_path, exist_ok=True)

        all_assets: Set[str] = set()

        for page in self.pages:
            filepath = os.path.join(self.output_dir, page.filename)
            if not os.path.exists(filepath):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    soup = BeautifulSoup(f.read(), 'html.parser')

                for folder, (tag, attrs, attr_name) in asset_types.items():
                    for element in soup.find_all(tag, attrs):
                        url = element.get(attr_name)
                        if url:
                            full_url = urljoin(page.url, url)
                            if full_url not in all_assets:
                                all_assets.add(full_url)
                                self._download_file(
                                    full_url,
                                    os.path.join(self.assets_dir, folder)
                                )
            except Exception as e:
                logger.debug(f'Asset error in {page.filename}: {e}')

    def _download_file(self, url: str, folder: str) -> Optional[str]:
        try:
            self.rate_limiter.wait(self.domain)
            resp = self.session.get(url, timeout=Config.DOWNLOAD_TIMEOUT)
            if resp.status_code != 200:
                return None

            filename = secure_filename(os.path.basename(url.split('?')[0]))
            if not filename:
                filename = f'asset_{hash(url)}'

            filepath = os.path.join(folder, filename)
            counter = 1
            while os.path.exists(filepath):
                name, ext = os.path.splitext(filename)
                filepath = os.path.join(folder, f'{name}_{counter}{ext}')
                counter += 1

            with open(filepath, 'wb') as f:
                f.write(resp.content)

            return filepath

        except Exception as e:
            logger.debug(f'Download failed: {url[:60]} - {e}')
            return None

    def create_zip(self) -> str:
        zip_path = os.path.join(Config.TEMP_DIR, f'{self.job_id}.zip')

        self.status = 'packaging'
        logger.info(f'[{self.job_id}] Creating ZIP archive')

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(self.output_dir):
                for filename in files:
                    filepath = os.path.join(root, filename)
                    arcname = os.path.relpath(filepath, self.output_dir)
                    zipf.write(filepath, arcname)

            metadata = {
                'job_id': self.job_id,
                'url': self.url,
                'domain': self.domain,
                'pages': len(self.pages),
                'extracted_at': datetime.utcnow().isoformat(),
                'duration': self.elapsed,
                'status': self.status,
            }
            zipf.writestr('metadata.json', json.dumps(metadata, indent=2))

        return zip_path

    def _build_result(self) -> ExtractionResult:
        total_size = sum(p.size for p in self.pages)
        return ExtractionResult(
            job_id=self.job_id,
            url=self.url,
            pages=self.pages,
            total_pages=len(self.pages),
            total_size=total_size,
            duration=self.elapsed,
            status=self.status,
            error=self.error,
        )

    def cleanup(self) -> None:
        shutil.rmtree(self.output_dir, ignore_errors=True)
        session_manager.cleanup(self.job_id)

    def save_metadata(self) -> None:
        metadata = {
            'job_id': self.job_id,
            'url': self.url,
            'domain': self.domain,
            'pages': len(self.pages),
            'total_size': sum(p.size for p in self.pages),
            'duration': self.elapsed,
            'status': self.status,
            'extracted_at': datetime.utcnow().isoformat(),
            'pages_detail': [
                {'url': p.url, 'file': p.filename, 'size': p.size, 'depth': p.depth}
                for p in self.pages
            ],
        }
        meta_path = os.path.join(self.output_dir, 'extraction_metadata.json')
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)


# ============================================================================
# Background Job Manager
# ============================================================================

class JobManager:
    def __init__(self):
        self._jobs: Dict[str, ExtractionEngine] = {}
        self._results: Dict[str, ExtractionResult] = {}
        self._lock = threading.Lock()

    def submit(self, engine: ExtractionEngine) -> str:
        with self._lock:
            self._jobs[engine.job_id] = engine
            db.create_extraction(engine.job_id, engine.url)

        thread = threading.Thread(
            target=self._run_job,
            args=(engine,),
            daemon=True,
        )
        thread.start()
        return engine.job_id

    def _run_job(self, engine: ExtractionEngine) -> None:
        try:
            result = engine.extract()

            if result.status == 'completed':
                engine.save_metadata()
                engine.create_zip()

            with self._lock:
                self._results[engine.job_id] = result

            db.update_extraction(
                engine.job_id,
                status=result.status,
                pages=result.total_pages,
                total_size=result.total_size,
                completed_at=datetime.utcnow().isoformat(),
                metadata=json.dumps({
                    'duration': result.duration,
                    'error': result.error,
                }),
            )

        except Exception as e:
            logger.exception(f'Job {engine.job_id} failed: {e}')

    def get_result(self, job_id: str) -> Optional[ExtractionResult]:
        with self._lock:
            return self._results.get(job_id)

    def get_engine(self, job_id: str) -> Optional[ExtractionEngine]:
        with self._lock:
            return self._jobs.get(job_id)

    def stop_job(self, job_id: str) -> bool:
        engine = self.get_engine(job_id)
        if engine:
            engine.stop()
            return True
        return False

    def cleanup_old(self, max_age_hours: int = 24) -> int:
        cleaned = 0
        now = time.time()
        with self._lock:
            for job_id in list(self._jobs.keys()):
                engine = self._jobs[job_id]
                if engine.start_time and (now - engine.start_time) > max_age_hours * 3600:
                    engine.cleanup()
                    del self._jobs[job_id]
                    self._results.pop(job_id, None)
                    cleaned += 1
        return cleaned


job_manager = JobManager()


# ============================================================================
# API Decorators
# ============================================================================

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if Config.ENABLE_API_AUTH:
            api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
            if not api_key or api_key not in Config.API_KEYS:
                return jsonify({'error': 'Invalid or missing API key'}), 401
        return f(*args, **kwargs)
    return decorated


def validate_url(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        url = request.json.get('url') if request.is_json else request.args.get('url')
        if not url:
            return jsonify({'error': 'URL is required'}), 400
        if not URLUtils.is_valid_url(URLUtils.normalize(url)):
            return jsonify({'error': 'Invalid URL format'}), 400
        return f(*args, **kwargs)
    return decorated


def json_response(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            if isinstance(result, tuple):
                return jsonify(result[0]), result[1]
            return jsonify(result)
        except ExtractionError as e:
            return jsonify({'error': str(e)}), 400
        except Exception as e:
            logger.exception('API error')
            return jsonify({'error': 'Internal server error'}), 500
    return decorated


# ============================================================================
# Background Cleanup Thread
# ============================================================================

def cleanup_worker():
    while True:
        time.sleep(Config.CLEANUP_INTERVAL)
        try:
            cleaned = job_manager.cleanup_old()
            if cleaned:
                logger.info(f'Cleaned up {cleaned} old jobs')

            for item in os.listdir(Config.TEMP_DIR):
                item_path = os.path.join(Config.TEMP_DIR, item)
                if item.endswith('.zip'):
                    age = time.time() - os.path.getmtime(item_path)
                    if age > 86400:
                        os.remove(item_path)
                        logger.info(f'Removed old ZIP: {item}')
        except Exception as e:
            logger.error(f'Cleanup error: {e}')


threading.Thread(target=cleanup_worker, daemon=True).start()


# ============================================================================
# Flask Routes
# ============================================================================

@app.route('/')
def index():
    return render_template('index.html')


# ----- Extraction API -----

@app.route('/api/extract', methods=['POST'])
@require_api_key
@validate_url
@json_response
def api_extract():
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    username = data.get('username', '')
    password = data.get('password', '')
    cookies = data.get('cookies', {})
    max_pages = min(int(data.get('max_pages', Config.MAX_PAGES)), 200)
    max_depth = min(int(data.get('max_depth', 3)), Config.MAX_DEPTH)
    mode = data.get('mode', 'standard')

    engine = ExtractionEngine(
        url=url,
        username=username,
        password=password,
        cookies=cookies,
        max_pages=max_pages,
        max_depth=max_depth,
    )

    job_id = job_manager.submit(engine)

    return {
        'success': True,
        'job_id': job_id,
        'url': url,
        'domain': engine.domain,
        'mode': mode,
        'message': 'Extraction started successfully',
    }


@app.route('/api/status/<job_id>', methods=['GET'])
@require_api_key
@json_response
def api_status(job_id: str):
    result = job_manager.get_result(job_id)
    engine = job_manager.get_engine(job_id)

    zip_path = os.path.join(Config.TEMP_DIR, f'{job_id}.zip')
    zip_ready = os.path.exists(zip_path)

    response = {
        'job_id': job_id,
        'status': 'processing',
        'progress': 0,
        'pages': 0,
        'elapsed': 0,
        'error': None,
    }

    if engine:
        response['status'] = engine.status
        response['progress'] = engine.progress.percentage
        response['pages'] = len(engine.pages)
        response['elapsed'] = engine.elapsed
        response['error'] = engine.error
    elif result:
        response['status'] = result.status
        response['progress'] = 100 if result.status == 'completed' else 0
        response['pages'] = result.total_pages
        response['elapsed'] = result.duration
        response['error'] = result.error

    if zip_ready:
        response['status'] = 'ready'
        response['download_url'] = f'/api/download/{job_id}'

    return response


@app.route('/api/download/<job_id>', methods=['GET'])
@require_api_key
def api_download(job_id: str):
    zip_path = os.path.join(Config.TEMP_DIR, f'{job_id}.zip')
    if os.path.exists(zip_path):
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f'extract_{job_id}.zip',
            mimetype='application/zip',
        )
    return jsonify({'error': 'File not found'}), 404


@app.route('/api/cancel/<job_id>', methods=['POST'])
@require_api_key
@json_response
def api_cancel(job_id: str):
    stopped = job_manager.stop_job(job_id)
    if stopped:
        return {'success': True, 'message': 'Extraction cancelled'}
    return {'success': False, 'message': 'Job not found'}, 404


@app.route('/api/history', methods=['GET'])
@require_api_key
@json_response
def api_history():
    limit = min(int(request.args.get('limit', 20)), 100)
    extractions = db.get_extractions(limit=limit)
    return {'success': True, 'extractions': extractions}


@app.route('/api/info', methods=['GET'])
@require_api_key
@json_response
def api_info():
    total_jobs = len(job_manager._jobs)
    temp_size = sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, fn in os.walk(Config.TEMP_DIR) for f in fn
        if os.path.isfile(os.path.join(dp, f))
    ) if os.path.exists(Config.TEMP_DIR) else 0

    return {
        'success': True,
        'version': '3.0.0',
        'uptime': 'N/A',
        'active_jobs': total_jobs,
        'temp_size_bytes': temp_size,
        'config': {
            'max_pages': Config.MAX_PAGES,
            'max_depth': Config.MAX_DEPTH,
            'request_timeout': Config.REQUEST_TIMEOUT,
            'cleanup_interval': Config.CLEANUP_INTERVAL,
        },
    }


# ----- Legacy Routes (Backward Compat) -----

@app.route('/extract', methods=['POST'])
@validate_url
def legacy_extract():
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    username = data.get('username', '')
    password = data.get('password', '')

    engine = ExtractionEngine(url=url, username=username, password=password)
    job_id = job_manager.submit(engine)

    return jsonify({'job_id': job_id, 'message': 'Extraction started'})


@app.route('/status/<job_id>')
def legacy_status(job_id: str):
    result = job_manager.get_result(job_id)
    engine = job_manager.get_engine(job_id)
    zip_path = os.path.join(Config.TEMP_DIR, f'{job_id}.zip')

    if os.path.exists(zip_path):
        return jsonify({
            'status': 'ready',
            'download_url': f'/download/{job_id}',
        })

    if engine:
        return jsonify({
            'status': engine.status,
            'progress': engine.progress.percentage,
            'pages': len(engine.pages),
        })

    if result:
        return jsonify({
            'status': result.status,
            'pages': result.total_pages,
        })

    return jsonify({'status': 'pending'})


@app.route('/download/<job_id>')
def legacy_download(job_id: str):
    zip_path = os.path.join(Config.TEMP_DIR, f'{job_id}.zip')
    if os.path.exists(zip_path):
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f'website_extract_{job_id}.zip',
        )
    return jsonify({'error': 'File not found'}), 404


@app.route('/cleanup', methods=['POST'])
def legacy_cleanup():
    data = request.get_json(silent=True) or {}
    job_id = data.get('job_id')
    if job_id:
        extract_dir = os.path.join(Config.TEMP_DIR, job_id)
        zip_path = os.path.join(Config.TEMP_DIR, f'{job_id}.zip')
        shutil.rmtree(extract_dir, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return jsonify({'message': 'Cleaned up'})
    return jsonify({'error': 'job_id required'}), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': 'Internal server error'}), 500


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'

    logger.info('=' * 60)
    logger.info('  Website Extractor Pro v3.0')
    logger.info('  Enterprise-Grade Web Extraction Engine')
    logger.info('=' * 60)
    logger.info(f'  Port: {port}')
    logger.info(f'  Debug: {debug}')
    logger.info(f'  Temp: {os.path.abspath(Config.TEMP_DIR)}')
    logger.info(f'  Max Pages: {Config.MAX_PAGES}')
    logger.info(f'  Max Depth: {Config.MAX_DEPTH}')
    logger.info('=' * 60)

    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)
