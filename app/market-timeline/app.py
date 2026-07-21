#!/usr/bin/env python3
"""盘面时间轴 - Flask + SQLite 后端
数据(SQLite 数据库 + 图片文件)保存在本地 Google Drive 同步目录中,
由 Google Drive for Desktop 自动同步到云端。
"""
import os
import re
import json
import time
import uuid
import base64
import sqlite3
from pathlib import Path

from flask import Flask, request, jsonify, render_template, send_from_directory, Response

# ---------------- 数据目录检测 ----------------
# 优先级: 环境变量 MARKET_TL_DATA > 自动检测 Google Drive 本地同步目录 > 用户主目录
def detect_data_dir() -> Path:
    env = os.environ.get("MARKET_TL_DATA")
    if env:
        return Path(env).expanduser()

    home = Path.home()
    candidates = []

    # macOS 新版 Google Drive for Desktop
    cloud = home / "Library" / "CloudStorage"
    if cloud.exists():
        for p in sorted(cloud.glob("GoogleDrive-*")):
            candidates.append(p / "My Drive")
            candidates.append(p / "我的云端硬盘")

    # 旧版路径 / Windows 常见盘符
    candidates += [
        home / "Google Drive" / "My Drive",
        home / "Google Drive" / "我的云端硬盘",
        home / "Google Drive",
        Path("G:/My Drive"),
        Path("G:/我的云端硬盘"),
    ]

    for c in candidates:
        if c.exists():
            return c / "market-timeline"

    # 找不到 Google Drive 时退回本地目录(启动时会打印提示)
    return home / "market-timeline-data"


DATA_DIR = detect_data_dir()
IMG_DIR = DATA_DIR / "images"
DB_PATH = DATA_DIR / "timeline.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)
IMG_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024 * 1024  # 单次请求最大 64MB


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # 重要: 云盘同步目录中不要用 WAL 模式(会产生 -wal/-shm 附属文件, 与同步机制冲突)
    conn.execute("PRAGMA journal_mode=DELETE")
    return conn


with db() as _conn:
    _conn.execute(
        """CREATE TABLE IF NOT EXISTS annotations(
            id TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            ts INTEGER NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            date_key TEXT NOT NULL,
            images TEXT NOT NULL DEFAULT '[]',
            created_at INTEGER NOT NULL
        )"""
    )
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON annotations(date_key)")

# ---------------- 图片处理 ----------------
DATAURL_RE = re.compile(r"^data:image/(png|jpeg|jpg|webp);base64,(.+)$", re.S)


def save_dataurl(dataurl: str):
    m = DATAURL_RE.match(dataurl)
    if not m:
        return None
    ext = "jpg" if m.group(1) in ("jpeg", "jpg") else m.group(1)
    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}.{ext}"
    (IMG_DIR / name).write_bytes(base64.b64decode(m.group(2)))
    return name


def resolve_images(images):
    """前端传来的 images 列表: 已有图片是 '/img/文件名', 新粘贴的是 dataURL。
    返回文件名列表(新图片落盘保存)。"""
    out = []
    for im in images or []:
        if not isinstance(im, str):
            continue
        if im.startswith("/img/"):
            out.append(im[5:])
        elif im.startswith("data:image/"):
            name = save_dataurl(im)
            if name:
                out.append(name)
    return out


def delete_image_files(names):
    for n in names:
        try:
            (IMG_DIR / n).unlink(missing_ok=True)
        except OSError:
            pass


def row_to_dict(r) -> dict:
    return {
        "id": r["id"],
        "market": r["market"],
        "ts": r["ts"],
        "text": r["text"],
        "dateKey": r["date_key"],
        "createdAt": r["created_at"],
        "images": ["/img/" + n for n in json.loads(r["images"])],
    }


# ---------------- 路由 ----------------
@app.get("/")
def index():
    return render_template("index.html", data_dir=str(DATA_DIR))


@app.get("/img/<path:name>")
def img(name):
    return send_from_directory(IMG_DIR, name)


@app.get("/api/annotations")
def list_annotations():
    with db() as conn:
        rows = conn.execute("SELECT * FROM annotations ORDER BY ts").fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.post("/api/annotations")
def create_annotation():
    d = request.get_json(force=True)
    names = resolve_images(d.get("images"))
    aid = d.get("id") or f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:5]}"
    row = (
        aid,
        d["market"],
        int(d["ts"]),
        d.get("text", ""),
        d["dateKey"],
        json.dumps(names),
        int(d.get("createdAt") or time.time() * 1000),
    )
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO annotations VALUES(?,?,?,?,?,?,?)", row)
        r = conn.execute("SELECT * FROM annotations WHERE id=?", (aid,)).fetchone()
    return jsonify(row_to_dict(r)), 201


@app.put("/api/annotations/<aid>")
def update_annotation(aid):
    d = request.get_json(force=True)
    with db() as conn:
        r = conn.execute("SELECT * FROM annotations WHERE id=?", (aid,)).fetchone()
        if not r:
            return jsonify({"error": "not found"}), 404
        old_names = set(json.loads(r["images"]))
        new_names = resolve_images(d.get("images"))
        # 清理被移除的图片文件
        delete_image_files(old_names - set(new_names))
        conn.execute(
            "UPDATE annotations SET text=?, images=? WHERE id=?",
            (d.get("text", r["text"]), json.dumps(new_names), aid),
        )
        r = conn.execute("SELECT * FROM annotations WHERE id=?", (aid,)).fetchone()
    return jsonify(row_to_dict(r))


@app.delete("/api/annotations/<aid>")
def delete_annotation(aid):
    with db() as conn:
        r = conn.execute("SELECT * FROM annotations WHERE id=?", (aid,)).fetchone()
        if r:
            delete_image_files(json.loads(r["images"]))
            conn.execute("DELETE FROM annotations WHERE id=?", (aid,))
    return jsonify({"ok": True})


@app.get("/api/export")
def export_all():
    """完整备份: 图片以 base64 内嵌, 单文件可迁移"""
    with db() as conn:
        rows = conn.execute("SELECT * FROM annotations ORDER BY ts").fetchall()
    out = []
    for r in rows:
        d = row_to_dict(r)
        embedded = []
        for url in d["images"]:
            p = IMG_DIR / url[5:]
            if p.exists():
                ext = p.suffix.lstrip(".").replace("jpg", "jpeg")
                embedded.append(
                    f"data:image/{ext};base64," + base64.b64encode(p.read_bytes()).decode()
                )
        d["images"] = embedded
        out.append(d)
    fname = "market-timeline-backup-" + time.strftime("%Y-%m-%d") + ".json"
    return Response(
        json.dumps(out, ensure_ascii=False, indent=1),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


@app.post("/api/import")
def import_all():
    """导入旧 HTML 版工具或本工具导出的 JSON 备份, 按 id 去重合并"""
    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "格式不正确"}), 400
    added = 0
    with db() as conn:
        existing = {r["id"] for r in conn.execute("SELECT id FROM annotations")}
        for d in data:
            if not d.get("id") or not d.get("ts") or d["id"] in existing:
                continue
            names = resolve_images(d.get("images"))
            conn.execute(
                "INSERT INTO annotations VALUES(?,?,?,?,?,?,?)",
                (
                    d["id"],
                    d.get("market", "us"),
                    int(d["ts"]),
                    d.get("text", ""),
                    d.get("dateKey", ""),
                    json.dumps(names),
                    int(d.get("createdAt") or time.time() * 1000),
                ),
            )
            added += 1
    return jsonify({"added": added})


if __name__ == "__main__":
    print(f"\n  数据目录: {DATA_DIR}")
    if "market-timeline-data" in str(DATA_DIR):
        print("  ⚠ 未检测到 Google Drive 本地同步目录, 数据暂存在上述本地路径。")
        print("  可通过环境变量指定: export MARKET_TL_DATA='/path/to/Google Drive/market-timeline'")
    print("  打开: http://127.0.0.1:5001\n")
    app.run(host="127.0.0.1", port=5001, debug=False)
