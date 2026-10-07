#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""海外研报云端任务：抓取 nlg.news → 下载 PDF → 上传 IMA 知识库。

用途：在 WorkBuddy 小程序「云上模式」定时任务中运行。云端没有 D 盘，
本脚本只负责「下载 + 上传 IMA」；本地 D 盘归档由本机脚本补跑，
两端靠 IMA 内已存在的文件名去重，不会重复入库。

设计约束（重要）：
  1. 仅使用 Python 标准库，不依赖 requests / bs4 / PyYAML，
     云端沙箱通常无法 pip install。
  2. 文件名必须与本地 src/research_archive/download/naming.py
     逐字节一致，否则 IMA 会存成两份。
  3. 时区固定按 Asia/Shanghai 计算「今天」，云端多为 UTC。
  4. 去重以 IMA 知识库内已有文件名为准（IMA 是唯一共享状态）。

用法：
  python3 cloud_run.py --probe                # 探测环境能力
  python3 cloud_run.py --dry-run              # 只解析不下载
  python3 cloud_run.py --since 2026-10-06 --until 2026-10-07

环境变量（必填）：
  NLG_ACCOUNT       nlg.news 登录账号
  NLG_PASSWORD      nlg.news 登录密码
  IMA_CLIENT_ID     IMA OpenAPI Client ID
  IMA_API_KEY       IMA OpenAPI API Key

退出码：0 全部成功 / 1 部分失败 / 2 配置错误 / 3 登录失败 / 4 运行异常
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import http.cookiejar
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ────────────────────────── 常量 ──────────────────────────
SITE = "https://www.nlg.news"
LIST_URL = SITE + "/haiwaiyanbao"
LOGIN_URL = SITE + "/index/user/login.html"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

IMA_BASE = "https://ima.qq.com/openapi/wiki/v1"
MEDIA_TYPE_PDF = 1
CONTENT_TYPE_PDF = "application/pdf"
MAX_PDF_BYTES = 200 * 1024 * 1024
RETRYABLE_CODES = {110021, 110010}

# 与 config.yaml 保持一致
WHITELIST = ["GS", "MS", "DB", "CITI", "BofA", "UBS", "HSBC", "Nomura",
             "JPM", "Barclays"]
EXCLUSIONS = ["MSCI", "CITIC", "GSK", "DBRS", "MSIG", "MS&AD"]
MAX_FILENAME_LEN = 180
MIN_PDF_SIZE = 10240

KB_ID_DEFAULT = "-WfYtfIQ4wa3L32OTfkCOSII7Cc_rQxn54g9Q9cwsTc="


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def today_cn() -> date:
    """按北京时间取今天。云端多为 UTC，必须显式指定。"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).date()
    except Exception:
        return (datetime.now(timezone.utc) + timedelta(hours=8)).date()


# ────────────────────── HTTP（urllib + cookiejar） ──────────────────────
class Http:
    def __init__(self, timeout: int = 30):
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar))
        self.timeout = timeout
        self.count = 0

    def get(self, url: str, referer: str = "", raw: bool = False):
        headers = {"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9",
                   "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}
        if referer:
            headers["Referer"] = referer
        req = urllib.request.Request(url, headers=headers)
        self.count += 1
        with self.opener.open(req, timeout=self.timeout) as r:
            data = r.read()
            return data if raw else data.decode("utf-8", errors="replace")

    def post_form(self, url: str, fields: dict, referer: str = ""):
        body = urllib.parse.urlencode(fields).encode("utf-8")
        headers = {"User-Agent": UA, "Content-Type":
                   "application/x-www-form-urlencoded",
                   "Origin": SITE}
        if referer:
            headers["Referer"] = referer
        req = urllib.request.Request(url, data=body, headers=headers)
        self.count += 1
        with self.opener.open(req, timeout=self.timeout) as r:
            return r.read().decode("utf-8", errors="replace")

    def download(self, url: str, dest: Path, timeout: int = 180,
                 retries: int = 3) -> tuple[bool, str]:
        """流式下载到文件，返回 (是否成功, 错误信息)。5xx 指数退避重试。"""
        last = ""
        for attempt in range(1, retries + 1):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                self.count += 1
                with self.opener.open(req, timeout=timeout) as r:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest, "wb") as f:
                        while True:
                            chunk = r.read(1 << 20)
                            if not chunk:
                                break
                            f.write(chunk)
                return True, ""
            except urllib.error.HTTPError as e:
                last = f"HTTP {e.code}"
                if 500 <= e.code < 600 and attempt < retries:
                    time.sleep(3 * (2 ** (attempt - 1)))
                    continue
                return False, last
            except Exception as e:
                last = f"网络异常: {e}"
                if attempt < retries:
                    time.sleep(3 * (2 ** (attempt - 1)))
                    continue
                return False, last
        return False, last


# ────────────────────── 登录 ──────────────────────
def login(h: Http, account: str, password: str) -> bool:
    page = h.get(LOGIN_URL)
    m = re.search(r'name="__token__"\s+value="([^"]+)"', page)
    if not m:
        raise RuntimeError("登录页未找到 __token__，站点可能改版")
    resp = h.post_form(LOGIN_URL, {
        "url": "", "__token__": m.group(1),
        "account": account, "password": password, "keeplogin": "1",
    }, referer=LOGIN_URL)
    # 登录成功后返回页含用户名或跳转；以 cookie 中出现 uid 为准
    names = {c.name for c in h.jar}
    ok = bool(names & {"uid", "token", "PHPSESSID"}) or ("退出" in resp)
    log(f"登录: cookie={sorted(names)} ok={ok}")
    return ok


def logged_in(h: Http, probe_url: str) -> bool:
    """登录态判定。

    ⚠️ 必须要求页面出现下载按钮：置顶公告匿名可读且没有下载按钮，
    拿它当探针会把失效会话误判为有效（本机 2026-10-07 踩过这个坑）。
    """
    try:
        txt = h.get(probe_url)
    except Exception:
        return False
    if "请登录后再操作" in txt:
        return False
    return "btn-download" in txt


def verify_session(h: Http) -> bool:
    """登录后自检：最新几条里至少有一条能拿到下载按钮。"""
    try:
        items = parse_page(h.get(f"{LIST_URL}?orderway=desc&page=1"))
    except Exception:
        return False
    items.sort(key=lambda x: int(x["id"]) if x["id"].isdigit() else 0,
               reverse=True)
    for e in items[:3]:
        if logged_in(h, f"{SITE}/haiwaiyanbao/{e['id']}.html"):
            return True
    return False


# ────────────────────── 列表解析（正则，无 bs4） ──────────────────────
ITEM_RE = re.compile(r'<div class="article-item.*?(?=<div class="article-item|'
                     r'<div class="col-xs-12 col-md-6">\s*<div class="article-item|\Z)',
                     re.S)
HREF_RE = re.compile(r'href="/haiwaiyanbao/(\d+)\.html"')
TITLE_RE = re.compile(r'<h3 class="article-title">\s*<a[^>]*>(.*?)</a>', re.S)
DATE_RE = re.compile(r'<span itemprop="date"[^>]*>([^<]+)</span>')
CN_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def _unescape_twice(s: str) -> str:
    """站点存在双重转义：源码 S&amp;amp;P → 需解两次才得 S&P。"""
    import html as _html
    return _html.unescape(_html.unescape(s))


def parse_page(page_html: str) -> list[dict]:
    out = []
    for block in ITEM_RE.findall(page_html):
        href = HREF_RE.search(block)
        title = TITLE_RE.search(block)
        date_m = DATE_RE.search(block)
        if not href or not title:
            continue
        d = None
        if date_m:
            cm = CN_DATE_RE.search(date_m.group(1))
            if cm:
                d = date(int(cm.group(1)), int(cm.group(2)), int(cm.group(3)))
        raw = re.sub(r"<[^>]+>", "", title.group(1))
        out.append({
            "id": href.group(1),
            "title": _unescape_twice(raw).strip(),
            "pub_date": d,
        })
    return out


# ────────────────────── 机构过滤（对齐本地 filter/institution.py） ──────────────────────
_LEAD_PUNCT = re.compile(r"^[\s\-_—–·、,，:：\[\]【】()（）]+")


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = _LEAD_PUNCT.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def match_institution(title: str) -> tuple[str | None, str]:
    t = normalize(title).lower()
    for ex in EXCLUSIONS:
        exs = normalize(ex).lower()
        if exs and t.startswith(exs):
            return None, f"excluded:{ex}"
    names = []
    for code in WHITELIST:
        names.append((code, normalize(code).lower()))
    names.sort(key=lambda x: len(x[1]), reverse=True)
    for code, name in names:
        if name and t.startswith(name):
            return code, f"prefix:{name}"
    return None, "no_match"


# ────────────────────── 文件命名（对齐本地 download/naming.py） ──────────────────────
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
ILLEGAL_RE = re.compile(r'[\\/:*?"<>|]')
TRAIL_RE = re.compile(r"[. ]+$")
RESERVED = {"CON", "PRN", "AUX", "NUL",
            *{f"COM{i}" for i in range(1, 10)},
            *{f"LPT{i}" for i in range(1, 10)}}


def sanitize(s: str) -> str:
    s = unicodedata.normalize("NFC", s or "")
    s = CONTROL_RE.sub("", s)
    s = ILLEGAL_RE.sub("_", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = s.strip()
    s = TRAIL_RE.sub("", s)
    if s.upper().split(".")[0] in RESERVED:
        s = "_" + s
    return s


def build_filename(pub: date, institution: str, raw_title: str,
                   max_len: int = MAX_FILENAME_LEN) -> str:
    prefix = f"{pub.strftime('%Y%m%d')}_{institution}_"
    ext = ".pdf"
    stem = sanitize(raw_title)
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    budget = max_len - len(prefix) - len(ext)
    if budget <= 0:
        raise ValueError(f"文件名前缀过长: {prefix}")
    if len(stem) > budget:
        hs = hashlib.blake2b(stem.encode("utf-8"), digest_size=3).hexdigest()
        keep = max(budget - len(hs) - 1, 20)
        stem = stem[:keep].rstrip(" _-") + "_" + hs
    name = f"{prefix}{stem}{ext}"
    return name if len(name) <= max_len else name[: max_len - 4] + ext


# ────────────────────── IMA 客户端（标准库实现） ──────────────────────
class ImaError(RuntimeError):
    def __init__(self, code, msg):
        super().__init__(f"[{code}] {msg}")
        self.code = code


def _cos_authorization(secret_id, secret_key, method, pathname,
                       sign_headers, start_time, expired_time) -> str:
    key_time = f"{start_time};{expired_time}"

    def _h(key: str, data: str) -> str:
        return hmac.new(key.encode("utf-8"), data.encode("utf-8"),
                        hashlib.sha1).hexdigest()

    sign_key = _h(secret_key, key_time)
    keys = sorted(sign_headers.keys())
    header_list = ";".join(k.lower() for k in keys)
    http_headers = "&".join(
        f"{k.lower()}={urllib.parse.quote(str(sign_headers[k]), safe='')}"
        for k in keys)
    http_string = f"{method.lower()}\n{pathname}\n\n{http_headers}\n"
    string_to_sign = ("sha1\n" + key_time + "\n" +
                      hashlib.sha1(http_string.encode("utf-8")).hexdigest() + "\n")
    signature = _h(sign_key, string_to_sign)
    return "&".join([
        "q-sign-algorithm=sha1", f"q-ak={secret_id}",
        f"q-sign-time={key_time}", f"q-key-time={key_time}",
        f"q-header-list={header_list}", "q-url-param-list=",
        f"q-signature={signature}",
    ])


class Ima:
    def __init__(self, cid: str, key: str, kb_id: str, timeout: int = 30,
                 max_retries: int = 4):
        self.cid, self.key, self.kb_id = cid, key, kb_id
        self.timeout, self.max_retries = timeout, max_retries
        self.folder_id = ""
        self.calls = 0

    def post(self, api: str, body: dict) -> dict:
        url = f"{IMA_BASE}/{api}"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "ima-openapi-clientid": self.cid,
            "ima-openapi-apikey": self.key,
            "Content-Type": "application/json",
        })
        last = None
        for attempt in range(1, self.max_retries + 1):
            self.calls += 1
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    resp = json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                body_txt = e.read().decode("utf-8", errors="replace")[:200]
                last = ImaError(e.code, body_txt)
            except Exception as e:
                last = ImaError(-1, f"网络异常: {e}")
                time.sleep(3 * (2 ** (attempt - 1)))
                continue
            else:
                code = resp.get("code", -1)
                if code == 0:
                    return resp.get("data") or {}
                last = ImaError(code, resp.get("msg", ""))
                if code in RETRYABLE_CODES and attempt < self.max_retries:
                    time.sleep(5 * (2 ** (attempt - 1)))
                    continue
            raise last
        raise last

    def resolve_root_folder(self) -> str:
        data = self.post("get_knowledge_list",
                         {"cursor": "", "limit": 1,
                          "knowledge_base_id": self.kb_id})
        path = data.get("current_path") or []
        return (path[0].get("folder_id") or "") if path else ""

    def existing_names(self, cap: int = 20000) -> set:
        """拉取知识库已有文件名，用于去重（IMA 是唯一共享状态）。"""
        names, cursor = set(), ""
        while len(names) < cap:
            body = {"cursor": cursor, "limit": 50,
                    "knowledge_base_id": self.kb_id}
            if self.folder_id:
                body["folder_id"] = self.folder_id
            data = self.post("get_knowledge_list", body)
            for it in data.get("knowledge_list") or []:
                n = it.get("name") or it.get("title")
                if n:
                    names.add(n)
            if data.get("is_end"):
                break
            nxt = data.get("next_cursor") or ""
            if not nxt or nxt == cursor:
                break
            cursor = nxt
        return names

    def upload_pdf(self, path: Path, name: str) -> tuple[bool, str, str]:
        size = path.stat().st_size
        if size > MAX_PDF_BYTES:
            return False, "too_large", ""
        try:
            data = self.post("create_media", {
                "file_name": name, "file_size": size,
                "content_type": CONTENT_TYPE_PDF,
                "knowledge_base_id": self.kb_id, "file_ext": "pdf",
            })
            media_id = data["media_id"]
            cred = data["cos_credential"]
            bucket, region, cos_key = (cred["bucket_name"], cred["region"],
                                       cred["cos_key"])
            host = f"{bucket}.cos.{region}.myqcloud.com"
            pathname = "/" + cos_key
            start = int(cred.get("start_time") or time.time())
            expired = int(cred.get("expired_time") or (start + 3600))
            auth = _cos_authorization(
                cred["secret_id"], cred["secret_key"], "PUT", pathname,
                {"content-length": str(size), "host": host}, start, expired)
            with open(path, "rb") as f:
                payload = f.read()
            req = urllib.request.Request(
                f"https://{host}{pathname}", data=payload, method="PUT",
                headers={"Content-Type": CONTENT_TYPE_PDF,
                         "Content-Length": str(size),
                         "Authorization": auth,
                         "x-cos-security-token": cred["token"],
                         "Host": host})
            with urllib.request.urlopen(req, timeout=300) as r:
                if not (200 <= r.status < 300):
                    return False, f"COS HTTP {r.status}", ""
            self.post("add_knowledge", {
                "media_type": MEDIA_TYPE_PDF, "media_id": media_id,
                "title": name, "knowledge_base_id": self.kb_id,
                "folder_id": self.folder_id,
                "file_info": {"cos_key": cos_key, "file_size": size,
                              "last_modify_time": int(time.time()),
                              "file_name": name},
            })
            return True, "uploaded", media_id
        except Exception as e:
            return False, f"failed: {e}", ""


# ────────────────────── 主流程 ──────────────────────
def probe() -> int:
    """探测云端环境能力：python 版本、外网连通性。"""
    log(f"Python {sys.version.split()[0]} on {sys.platform}")
    h = Http(timeout=15)
    for label, url in (("nlg.news 列表页", LIST_URL),
                       ("nlg.news 登录页", LOGIN_URL),
                       ("ima.qq.com", "https://ima.qq.com/")):
        try:
            h.get(url)
            log(f"  可访问: {label}")
        except Exception as e:
            log(f"  不可访问: {label} -> {e}")
    try:
        today_cn()
        log("  时区: Asia/Shanghai 可用")
    except Exception as e:
        log(f"  时区异常: {e}")
    log(f"今天(北京时间) = {today_cn()}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="起始发布日 YYYY-MM-DD，默认昨天")
    ap.add_argument("--until", help="截止发布日 YYYY-MM-DD，默认今天")
    ap.add_argument("--max-pages", type=int, default=25, help="翻页上限")
    ap.add_argument("--limit", type=int, default=0, help="限制处理条数")
    ap.add_argument("--outdir", default=".", help="报告输出目录")
    ap.add_argument("--kb-id", default=os.environ.get("IMA_KB_ID", KB_ID_DEFAULT))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--probe", action="store_true", help="只探测环境")
    args = ap.parse_args()

    if args.probe:
        return probe()

    account = os.environ.get("NLG_ACCOUNT", "")
    password = os.environ.get("NLG_PASSWORD", "")
    cid = os.environ.get("IMA_CLIENT_ID", "")
    key = os.environ.get("IMA_API_KEY", "")
    if not account or not password:
        log("配置错误：缺少 NLG_ACCOUNT / NLG_PASSWORD 环境变量")
        return 2
    if not cid or not key:
        log("配置错误：缺少 IMA_CLIENT_ID / IMA_API_KEY 环境变量")
        return 2

    today = today_cn()
    since = (datetime.strptime(args.since, "%Y-%m-%d").date()
             if args.since else today - timedelta(days=1))
    until = (datetime.strptime(args.until, "%Y-%m-%d").date()
             if args.until else today)
    log(f"日期窗口(北京时间): {since} ~ {until}")

    h = Http(timeout=30)
    try:
        if not login(h, account, password):
            log("登录失败：未获得登录 cookie，请检查账号密码")
            return 3
    except Exception as e:
        log(f"登录异常: {e}")
        return 3

    # 登录自检：能拿到下载按钮才算真的有权限（会员到期会卡在这一步）
    if not verify_session(h):
        log("登录自检失败：详情页拿不到下载按钮，可能会员已到期或账号异常")
        return 3
    log("登录自检通过")

    # 1) 抓取 + 解析 + 过滤
    entries, pages = [], 0
    for page in range(1, args.max_pages + 1):
        try:
            html_txt = h.get(f"{LIST_URL}?orderway=desc&page={page}")
        except Exception as e:
            log(f"第 {page} 页抓取失败: {e}")
            break
        pages += 1
        items = parse_page(html_txt)
        if not items:
            log(f"第 {page} 页解析出 0 条，疑似站点改版，停止")
            break
        fresh = [x for x in items if x["pub_date"] and since <= x["pub_date"] <= until]
        entries.extend(fresh)
        page_max = max((x["pub_date"] for x in items if x["pub_date"]),
                       default=None)
        time.sleep(1.5)
        if page_max and page_max < since:
            log(f"第 {page} 页已全部早于 {since}，停止翻页")
            break
    log(f"抓取 {pages} 页，窗口内 {len(entries)} 条")

    matched, unmatched = [], []
    for e in entries:
        code, reason = match_institution(e["title"])
        e["institution"], e["reason"] = code, reason
        (matched if code else unmatched).append(e)
    log(f"白名单命中 {len(matched)} 条，未命中(跳过) {len(unmatched)} 条")

    if args.limit:
        matched = matched[: args.limit]

    # 2) 准备 IMA（去重以 IMA 已有文件名为准）
    ima = None
    existing: set = set()
    if not args.dry_run:
        try:
            ima = Ima(cid, key, args.kb_id)
            ima.folder_id = ima.resolve_root_folder()
            existing = ima.existing_names()
            log(f"IMA 就绪：folder={ima.folder_id}，已有 {len(existing)} 个文件")
        except Exception as e:
            log(f"IMA 初始化失败，本轮放弃上传: {e}")
            ima = None

    tmp = Path(args.outdir) / "_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    stats = {"ok": 0, "skip": 0, "fail": 0}
    uploaded, failed = [], []

    for i, e in enumerate(matched, 1):
        name = build_filename(e["pub_date"], e["institution"], e["title"])
        detail = f"{SITE}/haiwaiyanbao/{e['id']}.html"
        log(f"({i}/{len(matched)}) {e['institution']} {e['title'][:60]}")

        if args.dry_run:
            log(f"    [dry-run] 将产出: {name}")
            stats["skip"] += 1
            continue
        if name in existing:
            log(f"    IMA 已存在，跳过")
            stats["skip"] += 1
            continue

        try:
            dhtml = h.get(detail)
            # 单点登录（实测）：本机若同时登录，会把云端会话踢掉。
            # 检测到登录拦截页就重新登录并重试一次。
            if len(dhtml) < 8000 and "请登录后再操作" in dhtml:
                log("    会话被踢，重新登录")
                h = Http(timeout=30)
                login(h, account, password)
                dhtml = h.get(detail)
        except Exception as ex:
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": f"详情页: {ex}"})
            continue
        m = re.search(r'data-url="([^"]+\.pdf)"', dhtml)
        if not m:
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": "未找到PDF直链"})
            continue
        time.sleep(1.0)
        dest = tmp / name
        ok, err = h.download(m.group(1), dest)
        if not ok:
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": err})
            continue
        if dest.stat().st_size < MIN_PDF_SIZE or dest.open("rb").read(5) != b"%PDF-":
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": "非PDF或过小"})
            dest.unlink(missing_ok=True)
            continue

        if ima is None:
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": "IMA 不可用"})
            continue
        ok, status, media_id = ima.upload_pdf(dest, name)
        dest.unlink(missing_ok=True)
        if ok:
            stats["ok"] += 1
            existing.add(name)
            uploaded.append({"id": e["id"], "name": name,
                             "date": e["pub_date"].isoformat(),
                             "institution": e["institution"]})
            log(f"    上传成功 {status}")
        else:
            stats["fail"] += 1
            failed.append({"id": e["id"], "name": name, "err": status})
            log(f"    上传失败: {status}")

    # 3) 产出报告
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = today.strftime("%Y%m%d")
    lines = [f"# 海外研报云端归档日报 {stamp}", "",
             f"- 日期窗口：{since} ~ {until}（北京时间）",
             f"- 抓取 {pages} 页 / 窗口内 {len(entries)} 条",
             f"- 白名单命中 {len(matched)} 条，未命中 {len(unmatched)} 条",
             f"- 上传成功 {stats['ok']}，跳过(已存在) {stats['skip']}，"
             f"失败 {stats['fail']}", ""]
    if uploaded:
        lines.append("## 本次上传")
        for u in uploaded:
            lines.append(f"- {u['date']} {u['institution']} — {u['name']}")
        lines.append("")
    if failed:
        lines.append("## 失败明细")
        for f in failed:
            lines.append(f"- {f['name']} — {f['err']}")
        lines.append("")
    (outdir / f"cloud-report-{stamp}.md").write_text(
        "\n".join(lines), encoding="utf-8")
    (outdir / f"cloud-result-{stamp}.json").write_text(
        json.dumps({"since": since.isoformat(), "until": until.isoformat(),
                    "pages": pages, "in_window": len(entries),
                    "matched": len(matched), "unmatched": len(unmatched),
                    "stats": stats, "uploaded": uploaded, "failed": failed},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    log("──────── 汇总 ────────")
    log(f"上传成功 {stats['ok']}  跳过 {stats['skip']}  失败 {stats['fail']}")
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        log(f"运行异常: {exc}")
        sys.exit(4)
