# 盘面时间轴 (Flask + SQLite 版)

跨市场(A股/港股/日股/美股)盘面观察记录工具。数据保存在本地 Google Drive 同步目录中,由 Google Drive for Desktop 自动同步到云端。

## 安装运行

```bash
cd market-timeline
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install flask
python app.py
```

浏览器打开 http://127.0.0.1:5001

## 数据保存位置

启动时按以下顺序确定数据目录:

1. 环境变量 `MARKET_TL_DATA`(如果设置了)
2. 自动检测 Google Drive 本地同步目录:
   - macOS 新版: `~/Library/CloudStorage/GoogleDrive-<邮箱>/My Drive/market-timeline/`
   - 旧版/其他: `~/Google Drive/My Drive/market-timeline/` 或 `G:/My Drive/market-timeline/`
3. 都找不到时退回 `~/market-timeline-data/`(启动时会打印警告)

数据目录结构:

```
market-timeline/
├── timeline.db     # SQLite 数据库(所有标注文本和元数据)
└── images/         # 粘贴的截图,每张一个 jpg/png 文件
```

手动指定目录(比如想放在 Drive 里的其他位置):

```bash
export MARKET_TL_DATA="$HOME/Library/CloudStorage/GoogleDrive-你的邮箱@gmail.com/My Drive/trading/timeline"
python app.py
```

启动后页面顶部会显示实际使用的数据目录。

## 从旧版 HTML 工具迁移

在旧版工具里点"导出备份"得到 JSON 文件,在本工具里点"导入"选择该文件即可。图片会自动从 base64 落盘为文件,重复记录自动跳过。

## SQLite 放在云盘同步目录的注意事项

- 程序已强制使用 `journal_mode=DELETE`,避免 WAL 模式的 `-wal`/`-shm` 附属文件与云盘同步机制冲突。
- **不要在两台电脑上同时运行本程序**写同一个同步目录 —— Drive 不理解 SQLite 的文件锁,并发写入会产生"冲突副本"文件,可能丢数据。单机使用完全没问题;换电脑用时,等 Drive 同步完成后再启动。
- 如果某天在数据目录里看到 `timeline (1).db` 之类的冲突副本,说明发生过并发写入,用 `sqlite3` 打开两个文件对比后手动合并。
- 图片是普通文件,同步很可靠;数据库单文件也不大(纯文本标注每条不到 1KB)。

## 定时开机自启(可选, macOS)

```bash
# 用 launchd 或最简单的方式: 加到 crontab
crontab -e
# 添加一行:
@reboot cd /path/to/market-timeline && ./venv/bin/python app.py >> app.log 2>&1
```

## 功能

- 四市场实时时钟 + 开盘状态(开盘中/午间休市/盘前盘后/休市)
- 统一 UTC 时间轴,各市场交易时段色块显示,红线标记当前时刻
- 点击时间轴任意位置添加标注,自动换算四地本地时间
- 输入框内 Ctrl+V 直接粘贴截图(自动压缩到最长边 1400px)
- 点击已有标注点编辑/删除,缩略图点击看大图
- 按日期浏览 + 最近标注跨日期快速跳转
- 导出/导入 JSON 完整备份(图片内嵌 base64)
