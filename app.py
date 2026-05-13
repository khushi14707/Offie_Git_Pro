import os
import sqlite3
import threading
import time
import shutil
import re
import difflib
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from docx import Document

app = Flask(__name__)
app.config['SECRET_KEY'] = 'office-git-pro-clean-portal'

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'uploads')
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])


@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


# ---------- DATABASE ENGINE ----------
def get_db_connection():
    conn = sqlite3.connect(os.path.join(BASE_DIR, 'office_git.db'), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    conn.execute(
        'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT, role TEXT)')
    conn.execute('''CREATE TABLE IF NOT EXISTS documents
                    (
                        id
                        INTEGER
                        PRIMARY
                        KEY
                        AUTOINCREMENT,
                        filename
                        TEXT,
                        version_name
                        TEXT,
                        uploaded_by
                        TEXT,
                        timestamp
                        TEXT,
                        is_deleted
                        INTEGER
                        DEFAULT
                        0
                    )''')
    conn.commit()
    conn.close()


init_db()

# ---------- AUTHENTICATION ----------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role


@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    if user: return User(user['id'], user['username'], user['role'])
    return None


# ---------- ATOMIC LOCKING & WATCHDOG ----------
_portal_ignorer = {}
_recently_uploaded = {}
_recently_uploaded_lock = threading.Lock()
_last_autoversioned = {}
_last_autoversioned_lock = threading.Lock()


class AutoVersionHandler(FileSystemEventHandler):
    def process(self, event):
        if event.is_directory: return
        filepath = os.path.abspath(event.src_path)
        filename = os.path.basename(filepath)
        if filename.startswith("v") and "_" in filename or filename.startswith("~$") or ".tmp" in filename: return
        with _recently_uploaded_lock:
            if time.time() - _portal_ignorer.get(filename, 0) < 6: return
            if time.time() - _recently_uploaded.get(filename, 0) < 5: return
        with _last_autoversioned_lock:
            if time.time() - _last_autoversioned.get(filename, 0) < 3: return
            _last_autoversioned[filename] = time.time()
        try:
            time.sleep(2)
            conn = get_db_connection()
            res = conn.execute("SELECT COUNT(*) FROM documents WHERE filename = ?", (filename,)).fetchone()
            version_name = f"v{res[0] + 1}_{filename}"
            shutil.copyfile(filepath, os.path.join(app.config['UPLOAD_FOLDER'], version_name))
            conn.execute(
                'INSERT INTO documents (filename, version_name, uploaded_by, timestamp, is_deleted) VALUES (?, ?, ?, ?, 0)',
                (filename, version_name, "AUTO-SAVED", datetime.now().strftime("%Y-%m-%d %H:%M")))
            conn.commit()
            conn.close()
        except:
            pass

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


def run_watcher():
    observer = Observer()
    observer.schedule(AutoVersionHandler(), path=app.config['UPLOAD_FOLDER'], recursive=False)
    observer.start()
    try:
        while True: time.sleep(2)
    except:
        observer.stop()
    observer.join()


# ---------- APPLICATION ROUTES ----------
@app.route('/')
@login_required
def index(): return redirect(url_for('home'))


@app.route('/home')
@login_required
def home(): return render_template('home.html', page_title="Home")


@app.route('/dashboard')
@login_required
def dashboard():
    conn = get_db_connection()
    docs = conn.execute(
        'SELECT * FROM documents WHERE is_deleted = 0 AND id IN (SELECT MAX(id) FROM documents GROUP BY filename) ORDER BY id DESC').fetchall()
    stats = {'word': 0, 'pdf': 0, 'text': 0}
    for doc in docs:
        ext = doc['filename'].lower().split('.')[-1]
        if ext in ['doc', 'docx']:
            stats['word'] += 1
        elif ext == 'pdf':
            stats['pdf'] += 1
        elif ext in ['txt', 'md', 'json', 'csv']:
            stats['text'] += 1
    conn.close()
    return render_template('dashboard.html', docs=docs, stats=stats, page_title="Dashboard")


@app.route('/my_space')
@login_required
def my_space():
    conn = get_db_connection()
    docs = conn.execute(
        'SELECT * FROM documents WHERE is_deleted = 0 AND id IN (SELECT MAX(id) FROM documents GROUP BY filename) ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('dashboard.html', docs=docs, page_title="My Space")


@app.route('/recycle_bin')
@login_required
def recycle_bin():
    conn = get_db_connection()
    docs = conn.execute('SELECT * FROM documents WHERE is_deleted = 1 ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('dashboard.html', docs=docs, page_title="Recycle Bin")


# ---------- NEW AUDIT LOG ROUTE ----------
@app.route('/audit_log')
@login_required
def audit_log():
    conn = get_db_connection()
    logs = conn.execute('SELECT * FROM documents ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('audit_log.html', logs=logs, page_title="Audit Log")


@app.route('/restore/<int:id>')
@login_required
def restore_file(id):
    conn = get_db_connection()
    conn.execute('UPDATE documents SET is_deleted = 0 WHERE id = ?', (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('recycle_bin'))


@app.route('/upload', methods=['POST'])
@login_required
def upload():
    file = request.files.get('file')
    if not file or file.filename.strip() == '': return redirect(request.referrer)
    filename = secure_filename(file.filename)
    with _recently_uploaded_lock: _recently_uploaded[filename] = time.time()
    conn = get_db_connection()
    res = conn.execute("SELECT COUNT(*) FROM documents WHERE filename = ?", (filename,)).fetchone()
    version_name = f"v{res[0] + 1}_{filename}"
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], version_name))
    shutil.copyfile(os.path.join(app.config['UPLOAD_FOLDER'], version_name),
                    os.path.join(app.config['UPLOAD_FOLDER'], filename))
    conn.execute(
        'INSERT INTO documents (filename, version_name, uploaded_by, timestamp, is_deleted) VALUES (?, ?, ?, ?, 0)',
        (filename, version_name, current_user.username, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit()
    conn.close()
    return redirect(url_for('dashboard'))


def strip_html(text):
    text = text.replace('</p>', '\n').replace('<br>', '\n')
    return re.sub('<[^<]+>', '', text).strip()


@app.route('/portal_save', methods=['POST'])
@login_required
def portal_save():
    if request.is_json:
        data = request.get_json()
        filename = data.get('filename')
        content = data.get('content', '')
        file_obj = None
    else:
        filename = request.form.get('filename')
        file_obj = request.files.get('file')
        content = ""

    _portal_ignorer[filename] = time.time()
    conn = get_db_connection()
    res = conn.execute("SELECT COUNT(*) FROM documents WHERE filename = ?", (filename,)).fetchone()
    version_name = f"v{res[0] + 1}_{filename}"
    base_file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    v_file = os.path.join(app.config['UPLOAD_FOLDER'], version_name)

    ext = filename.split('.')[-1].lower()
    try:
        if file_obj:
            file_obj.save(v_file)
        elif ext in ['txt', 'md', 'json', 'csv']:
            with open(v_file, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            shutil.copyfile(base_file, v_file)
    except Exception as e:
        if not file_obj:
            shutil.copyfile(base_file, v_file)

    if os.path.exists(v_file): shutil.copyfile(v_file, base_file)

    conn.execute(
        'INSERT INTO documents (filename, version_name, uploaded_by, timestamp, is_deleted) VALUES (?, ?, ?, ?, 0)',
        (filename, version_name, current_user.username, datetime.now().strftime("%Y-%m-%d %H:%M")))
    conn.commit()
    conn.close()
    return jsonify({"status": "success", "version": version_name})


@app.route('/diff/<v1>/<v2>')
@login_required
def compare_versions(v1, v2):
    try:
        path1 = os.path.join(app.config['UPLOAD_FOLDER'], v1)
        path2 = os.path.join(app.config['UPLOAD_FOLDER'], v2)

        def get_text(p):
            if p.endswith('.docx'):
                return "\n".join([para.text for para in Document(p).paragraphs])
            else:
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()

        text1 = get_text(path1).splitlines()
        text2 = get_text(path2).splitlines()
        diff_html = difflib.HtmlDiff(wrapcolumn=80).make_file(text1, text2, v1, v2, context=True, numlines=5)
        return diff_html
    except Exception as e:
        return f"<h3>Compare Error: Supported for Text/Word files only.</h3><p>{str(e)}</p>"


@app.route('/file_versions/<filename>')
@login_required
def file_versions(filename):
    conn = get_db_connection()
    versions = conn.execute('SELECT * FROM documents WHERE filename = ? AND is_deleted = 0 ORDER BY id DESC',
                            (filename,)).fetchall()
    conn.close()
    return jsonify([dict(v) for v in versions])


@app.route('/view/<path:filename>')
def view_file(filename): return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/download/<path:filename>')
def download(filename): return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)


@app.route('/delete/<int:id>')
@login_required
def delete_file(id):
    conn = get_db_connection()
    conn.execute('UPDATE documents SET is_deleted = 1 WHERE id = ?', (id,))
    conn.commit()
    conn.close()
    return redirect(request.referrer)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        conn = get_db_connection()
        user = conn.execute('SELECT * FROM users WHERE username = ?', (request.form['username'],)).fetchone()
        conn.close()
        if user and check_password_hash(user['password'], request.form['password']):
            login_user(User(user['id'], user['username'], user['role']))
            return redirect(url_for('home'))
        flash("Invalid Credentials")
    return render_template('login.html', page_title="Login")


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        conn = get_db_connection()
        try:
            conn.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                         (request.form['username'], generate_password_hash(request.form['password']),
                          request.form['role']))
            conn.commit()
            return redirect(url_for('login'))
        except:
            flash("User Exists")
        finally:
            conn.close()
    return render_template('register.html', page_title="Register")


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))


if __name__ == '__main__':
    # THREADING BLOCKED: Stops watchdog daemon from creating background ghost versions permanently.
    app.run(debug=True, use_reloader=False)