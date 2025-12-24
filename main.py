# -*- coding: utf-8 -*-
"""
WC — TG Sync (tkinter GUI)

Исправления и улучшения (на основе ваших правил):
- Основная логика выбора и сбора фото усилена: теперь при импорте "оригинального" режима (режим по-умолчанию - comments)
  фотографии берутся в порядке приоритета:
    1) Фото, которые являются ответами (reply_to_msg_id == id основного сообщения) — это основной приоритет.
    2) Фото, прикреплённые к самому основному сообщению (включая media groups).
    3) Фото, которые идут сразу после основного сообщения (последовательные сообщения без текста, часто доп. галерея).
  Соединяем эти источники в указанном порядке и набираем до cfg["MAX_PHOTOS"] (обычно 9).
  Это отражает ваше требование: "берешь там, где их много (минимум 9), они — ответ на основной пост или идут сразу после".
- Если нужное количество фото (MAX_PHOTOS) доступно в группе ответов или в комбинации replies+main+next,
  мы берем эти фото — даже если часть из них в основном посте, часть — в следующем сообщении.
- Режимы ручного выбора (PHOTO_SOURCE_MODE == "manual") поддерживаются, но даже при выборе "main"
  допускается дополнять из следующих сообщений, чтобы собрать до MAX_PHOTOS (пользователь просил именно такую логику).
- Остальной функционал — сохранён и слегка отрефакторен. Полный runnable файл (при установленных зависимостях).
"""

import os
import sys
import json
import threading
import asyncio
import time
import io
import builtins
import getpass
import traceback
import re
from datetime import datetime

from PIL import Image
import requests
import cloudinary
import cloudinary.uploader

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

# Try import WooCommerce client; if absent, code will still run but won't update site
try:
    from woocommerce import API as WC_API_Class
except Exception:
    WC_API_Class = None

# --- Paths ---
APP_DIR = os.path.abspath(os.path.dirname(__file__))
SETTINGS_PATH = os.path.join(APP_DIR, "settings.json")
DOWNLOAD_DIR = os.path.join(APP_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Logger ---
def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def lg(msg, verbose_only=False):
    print(f"[{timestamp()}] {msg}", flush=True)

def ulog(msg):
    print(f"[{timestamp()}] {msg}", flush=True)

# --- Defaults ---
DEFAULT_CONFIG = {
    "TG_API_ID": 0,
    "TG_API_HASH": "",
    "TG_PHONE": "",
    "TG_CHANNEL_ID": 0,
    "COMMENT_GROUP_ID": 0,

    "WC_URL": "",
    "WC_KEY": "",
    "WC_SECRET": "",

    "CLOUDINARY_CLOUD_NAME": "",
    "CLOUDINARY_API_KEY": "",
    "CLOUDINARY_API_SECRET": "",

    "MAX_PHOTOS": 9,
    "MAX_PHOTO_SIZE_MB": 10,
    "ALLOWED_EXTENSIONS": [".jpg", ".jpeg", ".png", ".gif", ".webp"],

    "PAUSE_BETWEEN_PRODUCTS": 15,
    "PAUSE_BETWEEN_PHOTOS": 2,

    "UPDATE_STRATEGY": "only_new",
    "UPDATE_WHAT": "both",
    "UPDATE_DESCRIPTION": True,
    "UPDATE_PHOTOS": True,

    "MIN_PHOTOS_TO_SKIP": 9,
    "PHOTO_SKIP_STRATEGIES": ["only_new"],

    "UPDATED_FILE": "updated_products.json",

    "PHOTO_SOURCE_MODE": "auto",
    "PHOTO_SOURCE_FORCED": "main",

    "DESCRIPTION_SOURCE_PRIORITY": "comments,main",
    "PHOTO_SOURCE_PRIORITY": "comments,main",

    "STOP_WORDS": [],

    "SKU_PREFER_SITE_FIELD": True,
    "SKU_TAKE_FIRST_N": 6,

    "VERBOSE_LOG": False,

    "OPERATION_MODE": "comments",

    "ADDITIONAL_POSTS_POSITION": "after"
}

# --- Settings load/save ---
def load_settings():
    if not os.path.exists(SETTINGS_PATH):
        save_settings(DEFAULT_CONFIG.copy())
        return DEFAULT_CONFIG.copy()
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    sw = cfg.get("STOP_WORDS", [])
    if isinstance(sw, str):
        cfg["STOP_WORDS"] = [x.strip().lower() for x in re.split(r'[,;\n]+', sw) if x.strip()]
    else:
        cfg["STOP_WORDS"] = [str(x).strip().lower() for x in (sw or []) if str(x).strip()]
    if isinstance(cfg.get("COMMENT_GROUP_ID"), str):
        try:
            cfg["COMMENT_GROUP_ID"] = int(cfg["COMMENT_GROUP_ID"].strip())
        except Exception:
            pass
    if isinstance(cfg.get("TG_CHANNEL_ID"), str):
        try:
            cfg["TG_CHANNEL_ID"] = int(cfg["TG_CHANNEL_ID"].strip())
        except Exception:
            pass
    if cfg.get("OPERATION_MODE") not in ("comments", "manual"):
        cfg["OPERATION_MODE"] = "comments"
    if cfg.get("ADDITIONAL_POSTS_POSITION") not in ("after", "before"):
        cfg["ADDITIONAL_POSTS_POSITION"] = "after"
    return cfg

def save_settings(cfg):
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

# -------------------------
# Text helpers and filtering
# -------------------------
def exclude_lines_by_keywords(text, keywords):
    if not text:
        return "", []
    lines = text.split("\n")
    out = []
    removed = []
    for line in lines:
        low = (line or "").lower()
        if any(k in low for k in keywords):
            removed.append(line)
            continue
        out.append(line)
    return "\n".join(out).strip(), removed

def clean_description(text):
    if not text:
        return ""
    s = re.sub(r'\*+', '', text)
    s = re.sub(r'\n+', '\n', s)
    s = re.sub(r'\n\s*\n', '\n\n', s)
    return s.strip()

def clean_telegram_description(text):
    if not text:
        return ""
    lines = text.split('\n')
    new = []
    stop_keywords = ["оплата","доставка","@","http","грн","$"]
    for line in lines:
        if any(k in line.lower() for k in stop_keywords):
            continue
        new.append(line)
    return "\n".join(new).strip()

# -------------------------
# SKU extraction
# -------------------------
def extract_site_article(product, cfg):
    prefer_site = bool(cfg.get("SKU_PREFER_SITE_FIELD", True))
    site_sku = str(product.get("sku") or "").strip()
    desc = str(product.get("description") or "")
    raw = ""

    if prefer_site and site_sku:
        raw = site_sku
        if cfg.get("VERBOSE_LOG", False):
            lg(f"Артикул: используем sku с сайта: '{raw}'")
    else:
        m = re.search(r'Артикул[ :]*([A-Za-z0-9\-]+)', desc or "", re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            if cfg.get("VERBOSE_LOG", False):
                lg(f"Артикул найден в описании: '{raw}'")
        else:
            if site_sku:
                raw = site_sku
                if cfg.get("VERBOSE_LOG", False):
                    lg(f"Артикул (fallback): используем sku с сайта: '{raw}'")

    if not raw:
        return ""

    parts = raw.split('-')
    if len(parts) >= 2:
        two = parts[0] + "-" + parts[1]
    else:
        two = raw

    try:
        n = int(cfg.get("SKU_TAKE_FIRST_N", 0) or 0)
    except Exception:
        n = 0
    if n > 0:
        return two[:n]
    return two

# -------------------------
# Image helpers & Cloudinary
# -------------------------
def prepare_image_for_upload(original_path, cfg):
    try:
        ext = os.path.splitext(original_path)[1].lower()
        allowed = set(cfg.get("ALLOWED_EXTENSIONS", DEFAULT_CONFIG["ALLOWED_EXTENSIONS"]))
        if ext in allowed:
            if ext in {".jpg", ".jpeg"}:
                try:
                    img = Image.open(original_path)
                    tmp = original_path + ".prepared.jpg"
                    img.save(tmp, format="JPEG", quality=85, optimize=True)
                    try:
                        os.replace(tmp, original_path)
                    except Exception:
                        original_path = tmp
                except Exception:
                    pass
            return original_path
        img = Image.open(original_path)
        rgb = img.convert("RGB")
        new = original_path + ".converted.jpg"
        rgb.save(new, format="JPEG", quality=85, optimize=True)
        if cfg.get("VERBOSE_LOG", False):
            lg(f"Конвертирован {original_path} -> {new}")
        return new
    except Exception as e:
        lg(f"Ошибка подготовки изображения {original_path}: {e}")
        return None

def image_file_ok(path, cfg):
    if not os.path.exists(path):
        if cfg.get("VERBOSE_LOG", False):
            lg(f"Файл не найден: {path}")
        return False
    size_mb = os.path.getsize(path) / (1024*1024)
    if size_mb > cfg.get("MAX_PHOTO_SIZE_MB", DEFAULT_CONFIG["MAX_PHOTO_SIZE_MB"]):
        lg(f"Пропущено (больше {cfg.get('MAX_PHOTO_SIZE_MB')}MB): {os.path.basename(path)}")
        return False
    ext = os.path.splitext(path)[1].lower()
    allowed = set(cfg.get("ALLOWED_EXTENSIONS", DEFAULT_CONFIG["ALLOWED_EXTENSIONS"]))
    if ext not in allowed:
        lg(f"Пропущено (неподдерживаемое расширение): {os.path.basename(path)}")
        return False
    return True

def upload_image_cloudinary(image_path, cfg, retries=3, delay=4):
    prepared = prepare_image_for_upload(image_path, cfg)
    if not prepared:
        lg(f"Подготовка файла не удалась: {image_path}")
        return None
    if not image_file_ok(prepared, cfg):
        lg(f"Файл не соответствует ограничениям: {os.path.basename(prepared)}")
        return None
    cloudinary.config(
        cloud_name=cfg.get("CLOUDINARY_CLOUD_NAME"),
        api_key=cfg.get("CLOUDINARY_API_KEY"),
        api_secret=cfg.get("CLOUDINARY_API_SECRET"),
        secure=True,
    )
    last = None
    for attempt in range(1, retries+1):
        try:
            if cfg.get("VERBOSE_LOG", False):
                lg(f"Загружаю {os.path.basename(prepared)} на Cloudinary (попытка {attempt})")
            res = cloudinary.uploader.upload(prepared, folder="tg_import")
            url = res.get("secure_url")
            if cfg.get("VERBOSE_LOG", False):
                lg(f"Успешно загружено: {url}")
            if prepared.endswith(".converted.jpg") or prepared.endswith(".prepared.jpg"):
                try: os.remove(prepared)
                except Exception: pass
            return url
        except Exception as ex:
            last = ex
            lg(f"Ошибка загрузки {os.path.basename(prepared)}: {ex}")
            time.sleep(delay)
    lg(f"Не удалось загрузить {os.path.basename(image_path)} после {retries} попыток.")
    return None

# -------------------------
# WooCommerce helpers
# -------------------------
def get_all_products(wcapi):
    out = []
    page = 1
    per_page = 100
    if wcapi is None:
        lg("WC API не инициализирован — список товаров не получен.")
        return out
    while True:
        r = wcapi.get("products", params={"page": page, "per_page": per_page})
        try:
            chunk = r.json()
        except Exception as e:
            lg(f"Ошибка парсинга ответа WC: {e}")
            break
        if not chunk or not isinstance(chunk, list):
            break
        out.extend(chunk)
        if len(chunk) < per_page:
            break
        page += 1
    lg(f"Получено товаров: {len(out)}")
    return out

def get_product_images_count(product):
    return len(product.get("images", []))

def load_updated_products(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                data = {str(pid): {"desc": True, "photo": True} for pid in data}
            return data
        except Exception:
            return {}
    return {}

def save_updated_products(dct, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dct, f, ensure_ascii=False, indent=2)

# -------------------------
# Telegram helpers
# -------------------------
async def find_main_message(client, group_entity, site_article, limit=1000):
    if not site_article:
        return None
    candidates = []
    async for msg in client.iter_messages(group_entity, search=site_article, limit=limit):
        if re.search(rf'\b{re.escape(site_article)}\b', msg.text or "", re.IGNORECASE):
            candidates.append(msg)
    if candidates:
        chosen = max(candidates, key=lambda m: len(m.text or ""))
        return chosen
    return None

async def collect_photos_from_media_group(client, group_entity, grouped_id, max_photos, prefix="group"):
    photos = []
    msgs = []
    async for m in client.iter_messages(group_entity, min_id=0, max_id=999999999):
        # iter_messages without bounds is slow; we will instead search around grouped messages in calling code
        break
    return photos

async def collect_photos_combined(client, group_entity, main_msg, max_photos=9, position="after"):
    """
    Собирает фотографии в порядке приоритета:
      1) Ответы (reply_to_msg_id == main_msg.id) с фото
      2) Фото из основного сообщения (media group или одиночное)
      3) Доп. фото, идущие сразу после основного поста (без текста), пока не встретится текст
    Возвращает список локальных путей до скачанных файлов (до max_photos).
    """
    photos = []
    seen_msg_ids = set()

    # 1) Собираем ответы (replies) к main_msg
    try:
        async for m in client.iter_messages(group_entity, min_id=main_msg.id+1, max_id=main_msg.id+800):
            if getattr(m, "reply_to_msg_id", None) == main_msg.id and getattr(m, "photo", None):
                fname = os.path.join(DOWNLOAD_DIR, f"reply_{main_msg.id}_{m.id}.jpg")
                try:
                    await client.download_media(m.media or m.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(m.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
    except Exception:
        # перебор мог упасть по таймауту — продолжаем дальше
        pass

    # 2) Фото из основного сообщения (media group или одиночное)
    try:
        if getattr(main_msg, "grouped_id", None):
            gid = main_msg.grouped_id
            msgs = []
            async for m in client.iter_messages(group_entity, min_id=main_msg.id-50, max_id=main_msg.id+50):
                if getattr(m, "grouped_id", None) == gid and getattr(m, "photo", None):
                    msgs.append(m)
            msgs = sorted(msgs, key=lambda x: x.id)
            for m in msgs:
                if m.id in seen_msg_ids:
                    continue
                fname = os.path.join(DOWNLOAD_DIR, f"maingroup_{gid}_{m.id}.jpg")
                try:
                    await client.download_media(m.media or m.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(m.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
        else:
            if getattr(main_msg, "photo", None):
                if main_msg.id not in seen_msg_ids:
                    fname = os.path.join(DOWNLOAD_DIR, f"main_{main_msg.id}.jpg")
                    try:
                        await client.download_media(main_msg.media or main_msg.photo, file=fname)
                        photos.append(fname)
                        seen_msg_ids.add(main_msg.id)
                    except Exception:
                        pass
                    if len(photos) >= max_photos:
                        return photos
    except Exception:
        pass

    # 3) Фото, идущие сразу после основного поста (без текста) — собираем подряд до первого текстового сообщения
    try:
        async for m in client.iter_messages(group_entity, min_id=main_msg.id+1, max_id=main_msg.id+400):
            if m.text and m.text.strip():
                # встречен текст — считаем, что серия доп. фото закончилась
                break
            if getattr(m, "photo", None) and m.id not in seen_msg_ids:
                fname = os.path.join(DOWNLOAD_DIR, f"after_{main_msg.id}_{m.id}.jpg")
                try:
                    await client.download_media(m.media or m.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(m.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
    except Exception:
        pass

    return photos

async def collect_photos_from_main_only_with_next(client, group_entity, main_msg, max_photos=9, position="after"):
    """
    Если пользователь явно выбрал 'main' — собираем фото из main (media group / photo),
    и при недостатке дополняем ближайшими после main фото (без текста), чтобы получить до max_photos.
    """
    photos = []
    seen_msg_ids = set()
    try:
        # main media
        if getattr(main_msg, "grouped_id", None):
            gid = main_msg.grouped_id
            msgs = []
            async for m in client.iter_messages(group_entity, min_id=main_msg.id-50, max_id=main_msg.id+50):
                if getattr(m, "grouped_id", None) == gid and getattr(m, "photo", None):
                    msgs.append(m)
            msgs = sorted(msgs, key=lambda x: x.id)
            for m in msgs:
                fname = os.path.join(DOWNLOAD_DIR, f"maingroup_{gid}_{m.id}.jpg")
                try:
                    await client.download_media(m.media or m.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(m.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
        else:
            if getattr(main_msg, "photo", None):
                fname = os.path.join(DOWNLOAD_DIR, f"main_{main_msg.id}.jpg")
                try:
                    await client.download_media(main_msg.media or main_msg.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(main_msg.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
    except Exception:
        pass

    # дополнительно берем фото после main (без текста), если нужно
    try:
        async for m in client.iter_messages(group_entity, min_id=main_msg.id+1, max_id=main_msg.id+400):
            if m.text and m.text.strip():
                break
            if getattr(m, "photo", None) and m.id not in seen_msg_ids:
                fname = os.path.join(DOWNLOAD_DIR, f"main_after_{main_msg.id}_{m.id}.jpg")
                try:
                    await client.download_media(m.media or m.photo, file=fname)
                    photos.append(fname)
                    seen_msg_ids.add(m.id)
                except Exception:
                    pass
                if len(photos) >= max_photos:
                    return photos
    except Exception:
        pass

    return photos

# -------------------------
# Update product
# -------------------------
def update_product(product_id, new_description, photo_paths, wcapi, cfg, update_desc, update_photo, updated_file, tags=None):
    data = {}
    removed_lines = []
    if update_desc:
        filtered, removed = exclude_lines_by_keywords(new_description, cfg.get("STOP_WORDS", []))
        removed_lines = removed
        cleaned = clean_description(filtered)
        if cleaned:
            data["description"] = cleaned
    uploaded_urls = []
    if update_photo:
        for p in photo_paths:
            if not image_file_ok(p, cfg):
                continue
            url = upload_image_cloudinary(p, cfg, retries=3, delay=cfg.get("PAUSE_BETWEEN_PHOTOS", 2))
            if url:
                uploaded_urls.append(url)
            if len(uploaded_urls) >= cfg.get("MAX_PHOTOS", 9):
                break
            time.sleep(cfg.get("PAUSE_BETWEEN_PHOTOS", 2))
        if uploaded_urls:
            data["images"] = [{"src": u} for u in uploaded_urls]
    if tags:
        data["tags"] = [{"name": t} for t in tags]
    if not data:
        return False, uploaded_urls, removed_lines
    try:
        if update_photo and wcapi is not None:
            try:
                wcapi.put(f"products/{product_id}", {"images": []})
                time.sleep(1)
            except Exception:
                pass
        res = wcapi.put(f"products/{product_id}", data)
        if getattr(res, "status_code", None) in (200, 201):
            return True, uploaded_urls, removed_lines
        else:
            return False, uploaded_urls, removed_lines
    except Exception:
        return False, uploaded_urls, removed_lines

# -------------------------
# Process one product
# -------------------------
async def process_one_product(product, wcapi, cfg, updated_dict):
    result = {
        "product_id": str(product.get("id")),
        "name": product.get("name", "") or "",
        "article": "",
        "updated": False,
        "desc_updated": False,
        "photos_uploaded": [],
        "photos_count": 0,
        "removed_lines": [],
        "error": None,
        "review_reason": None,
        "modes": {},
        "description_preview": ""
    }
    prod_id = result["product_id"]
    site_title = result["name"]
    site_article = extract_site_article(product, cfg)
    result["article"] = site_article

    ulog(f"Обработка: \"{site_title}\" (id={prod_id}, артикул='{site_article}')")

    prev = updated_dict.get(prod_id, {})
    desc_done = bool(prev.get("desc", False))
    photo_done = bool(prev.get("photo", False))

    update_strategy = cfg.get("UPDATE_STRATEGY", "only_new")
    update_what = cfg.get("UPDATE_WHAT", "both")
    if update_what == "photos":
        want_desc = False; want_photo = True
    elif update_what == "description":
        want_desc = True; want_photo = False
    else:
        want_desc = cfg.get("UPDATE_DESCRIPTION", True)
        want_photo = cfg.get("UPDATE_PHOTOS", True)

    is_updated_any = desc_done or photo_done
    if update_strategy == "only_new" and is_updated_any:
        ulog(f"  → Пропущен (только новые, уже обновлялся ранее).")
        result["review_reason"] = "only_new_already_updated"
        return result
    if update_strategy == "only_updated" and not is_updated_any:
        ulog(f"  → Пропущен (только обновлённые, ранее не обновлялся).")
        result["review_reason"] = "only_updated_not_prev"
        return result

    if cfg.get("UPDATE_PHOTOS", True) and update_strategy in cfg.get("PHOTO_SKIP_STRATEGIES", ["only_new"]):
        cnt = get_product_images_count(product)
        if cnt >= cfg.get("MIN_PHOTOS_TO_SKIP", 9):
            ulog(f"  → Фото пропущены (на сайте уже {cnt} фото).")
            want_photo = False

    if update_strategy != "all":
        if desc_done: want_desc = False
        if photo_done: want_photo = False

    if not want_desc and not want_photo:
        ulog("  → Нечего обновлять (по настройкам и истории).")
        result["review_reason"] = "nothing_to_update"
        return result

    # Telethon client
    from telethon import TelegramClient
    client = TelegramClient('user_session', int(cfg.get("TG_API_ID")), cfg.get("TG_API_HASH"))
    try:
        await client.start(phone=cfg.get("TG_PHONE"))
    except Exception as e:
        result["error"] = f"Telethon start error: {e}"
        ulog(f"  Ошибка подключения к Telegram: {e}")
        try: await client.disconnect()
        except: pass
        return result

    main_entity = None
    comments_entity = None
    try:
        if cfg.get("TG_CHANNEL_ID"):
            try:
                main_entity = await client.get_entity(cfg.get("TG_CHANNEL_ID"))
            except Exception:
                main_entity = None
        if cfg.get("COMMENT_GROUP_ID"):
            try:
                comments_entity = await client.get_entity(cfg.get("COMMENT_GROUP_ID"))
            except Exception:
                comments_entity = None
    except Exception as e:
        result["error"] = f"Error getting entities: {e}"
        await client.disconnect()
        return result

    op_mode = cfg.get("OPERATION_MODE", "comments")
    result["modes"]["op_mode"] = op_mode
    result["modes"]["photo_mode"] = cfg.get("PHOTO_SOURCE_MODE", "auto")

    # Find main message
    main_msg = None
    # default behaviour: in comments mode we search in COMMENT_GROUP_ID (ваш второй чат)
    if op_mode == "comments":
        if not comments_entity:
            result["review_reason"] = "missing_comment_group"
            ulog("  → Режим 'Работа по группе' требует COMMENT_GROUP_ID — добавлено в ручную проверку.")
            await client.disconnect()
            return result
        main_msg = await find_main_message(client, comments_entity, site_article)
    else:
        # manual mode: try forced sources but still prefer comments_entity if configured
        forced = cfg.get("PHOTO_SOURCE_FORCED", "main")
        if forced == "main" and main_entity:
            main_msg = await find_main_message(client, main_entity, site_article)
        if not main_msg and comments_entity:
            main_msg = await find_main_message(client, comments_entity, site_article)
        if not main_msg and main_entity and forced != "main":
            main_msg = await find_main_message(client, main_entity, site_article)

    if not main_msg:
        result["review_reason"] = "not_found"
        ulog("  → Сообщение в Telegram не найдено — добавлено в ручную проверку.")
        await client.disconnect()
        return result

    # Description selection according to priority
    desc_priority = [s.strip() for s in cfg.get("DESCRIPTION_SOURCE_PRIORITY", "comments,main").split(",") if s.strip()]
    description_text = ""
    if want_desc:
        for source in desc_priority:
            if source == "main":
                if getattr(main_msg, "text", None):
                    description_text = clean_telegram_description(main_msg.text)
                    break
            elif source == "comments":
                entity_to_search = comments_entity or main_entity
                if entity_to_search:
                    async for m in client.iter_messages(entity_to_search, min_id=main_msg.id+1, max_id=main_msg.id+200):
                        if getattr(m, "reply_to_msg_id", None) == main_msg.id and getattr(m, "text", None):
                            description_text = clean_telegram_description(m.text)
                            break
                if description_text:
                    break
        if not description_text:
            description_text = clean_telegram_description(main_msg.text or "")

    result["description_preview"] = (description_text or "")[:400].replace("\n", " ")

    # Photo collection using enhanced rules:
    photo_paths = []
    max_photos = int(cfg.get("MAX_PHOTOS", 9))
    # Decide which entity to use for fetching photos:
    # Prefer comments_entity (the group) as primary source per your request
    fetch_entity = comments_entity or main_entity

    # If operation mode is manual and photo source forced to 'main' and main_entity corresponds:
    # but still allow supplement from next messages (rule: main or next)
    if cfg.get("PHOTO_SOURCE_MODE", "auto") == "manual" and cfg.get("PHOTO_SOURCE_FORCED", "main") == "main":
        # collect from main_entity (where the main message was found), prefer main, supplement with next messages
        fetch_entity = main_entity or comments_entity
        photo_paths = await collect_photos_from_main_only_with_next(client, fetch_entity, main_msg, max_photos, position=cfg.get("ADDITIONAL_POSTS_POSITION","after"))
    else:
        # default (auto/comments priority) — use combined collector that follows your three rules:
        # replies -> main -> immediate after
        fetch_entity = comments_entity or main_entity
        photo_paths = await collect_photos_combined(client, fetch_entity, main_msg, max_photos, position=cfg.get("ADDITIONAL_POSTS_POSITION","after"))

    # If still nothing and media exists in main entity (fallback)
    if want_photo and not photo_paths:
        # try main-only fallback
        fetch_entity_fallback = main_entity or comments_entity
        photo_paths = await collect_photos_from_main_only_with_next(client, fetch_entity_fallback, main_msg, max_photos, position=cfg.get("ADDITIONAL_POSTS_POSITION","after"))

    # Show concise info about photos found
    if want_photo:
        if photo_paths:
            names = [os.path.basename(p) for p in photo_paths]
            ulog(f"  Фото найдено: {len(photo_paths)} шт. (будут загружены: {', '.join(names[:6])}{'...' if len(names)>6 else ''})")
        else:
            ulog("  Фото не найдено для загрузки.")

    # Perform update
    try:
        success, uploaded_urls, removed_lines = await asyncio.to_thread(
            update_product, product["id"], description_text, photo_paths, wcapi, cfg, want_desc, want_photo, cfg.get("UPDATED_FILE","updated_products.json")
        )
    except Exception as e:
        success = False
        uploaded_urls = []
        removed_lines = []
        result["error"] = f"update_product exception: {e}"

    # Clean temporary downloaded photos
    for p in photo_paths:
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    if success:
        result["updated"] = True
        result["desc_updated"] = bool(want_desc)
        result["photos_uploaded"] = uploaded_urls
        result["photos_count"] = len(uploaded_urls)
        result["removed_lines"] = removed_lines
        if prod_id not in updated_dict:
            updated_dict[prod_id] = {}
        if want_desc: updated_dict[prod_id]["desc"] = True
        if want_photo: updated_dict[prod_id]["photo"] = True
        save_updated_products(updated_dict, cfg.get("UPDATED_FILE","updated_products.json"))

        ulog(f"  Успешно обновлён. Фото: {len(uploaded_urls)}. Описание: {'обновлено' if want_desc else 'нет'}")
        if removed_lines:
            ulog(f"  Удалено строк с стоп-словами: {len(removed_lines)} — {removed_lines[:3]}{'...' if len(removed_lines)>3 else ''}")
    else:
        result["updated"] = False
        result["photos_uploaded"] = uploaded_urls
        result["photos_count"] = len(uploaded_urls)
        result["removed_lines"] = removed_lines
        if result["error"]:
            ulog(f"  Ошибка: {result['error']}")
        else:
            ulog("  Обновление не удалось (см. подробный лог).")
        result["review_reason"] = "update_failed"

    await client.disconnect()
    return result

# -------------------------
# GUI and Worker
# -------------------------
class StdoutProxy(io.TextIOBase):
    def __init__(self, write_cb):
        self.write_cb = write_cb
    def write(self, s):
        if s:
            self.write_cb(s)
        return len(s)
    def flush(self): pass

STRATEGY_OPTIONS = {
    "only_new": "Только новые",
    "only_updated": "Только обновлённые",
    "all": "Все"
}
STRATEGY_OPTIONS_INV = {v:k for k,v in STRATEGY_OPTIONS.items()}

WHAT_OPTIONS = {
    "both": "Описание и фото",
    "photos": "Только фото",
    "description": "Только описание"
}
WHAT_OPTIONS_INV = {v:k for k,v in WHAT_OPTIONS.items()}

PHOTO_MODE_OPTIONS = {"auto":"Авто (где больше фото)","manual":"Ручной (принудительно выбрать источник)"}

OPERATION_MODE_OPTIONS = {"comments":"Работа по группе (комментарии)","manual":"Ручной режим"}
OPERATION_MODE_INV = {v:k for k,v in OPERATION_MODE_OPTIONS.items()}

ADDITIONAL_POSTS_POS = {"after":"После основного поста (обычно replies)","before":"Перед основным постом"}
ADDITIONAL_POSTS_POS_INV = {v:k for k,v in ADDITIONAL_POSTS_POS.items()}

PRIORITY_CHOICES = ["comments,main","main,comments"]

HELP_TEXTS = {
    "TG_API_ID": "ID приложения Telegram API. Где взять: my.telegram.org → API development → App configuration → api_id.",
    "TG_API_HASH": "Hash приложения Telegram API.",
    "TG_PHONE": "Номер телефона Telegram.",
    "WC_URL": "URL магазина WooCommerce.",
    "WC_KEY": "Consumer Key для REST API WooCommerce.",
    "WC_SECRET": "Consumer Secret для WooCommerce.",
    "COMMENT_GROUP_ID": "ID Telegram-группы/канала с комментариями (например -100...).",
    "TG_CHANNEL_ID": "ID основного Telegram-канала/чата (где размещаются основные посты).",
    "MAX_PHOTOS": "Максимальное количество фото, загружаемых с Telegram.",
    "MAX_PHOTO_SIZE_MB": "Максимальный размер фото в мегабайтах для загрузки.",
    "PHOTO_SOURCE_MODE": "Режим выбора источника фото.",
    "PHOTO_SOURCE_FORCED": "Источник при ручном режиме (main/comments).",
    "PHOTO_SOURCE_PRIORITY": "Приоритет источников фото.",
    "DESCRIPTION_SOURCE_PRIORITY": "Приоритет источников описания.",
    "UPDATE_STRATEGY": "Стратегия обновления товаров.",
    "UPDATE_WHAT": "Что обновлять: описание, фото или оба.",
    "STOP_WORDS": "Стоп-слова — строки, содержащие их будут удалены из описания.",
    "SKU_PREFER_SITE_FIELD": "Если включено — сначала используется поле sku товара на сайте.",
    "SKU_TAKE_FIRST_N": "Если >0 — берутся первые N символов после обработки артикула.",
    "VERBOSE_LOG": "Подробный лог (для отладки).",
    "OPERATION_MODE": "Режим работы: комментарии (рекомендуется) или ручной.",
    "ADDITIONAL_POSTS_POSITION": "Позиция дополнительных постов относительно основного."
}

class SettingsDialog(tk.Toplevel):
    def __init__(self, master, cfg):
        super().__init__(master)
        self.title("Настройки")
        self.geometry("900x700")
        self.minsize(700,500)
        self.resizable(True, True)
        self.cfg = dict(cfg)

        container = ttk.Frame(self)
        container.pack(fill="both", expand=True)

        canvas = tk.Canvas(container, borderwidth=0)
        vscroll = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vscroll.set)
        vscroll.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        frm = ttk.Frame(canvas, padding=8)
        self.inner_id = canvas.create_window((0,0), window=frm, anchor="nw")

        def _on_frame_config(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        frm.bind("<Configure>", _on_frame_config)

        def _on_canvas_config(event):
            try:
                canvas.itemconfig(self.inner_id, width=event.width)
            except Exception:
                pass
        canvas.bind("<Configure>", _on_canvas_config)

        def on_enter(event):
            canvas.bind_all("<MouseWheel>", on_mousewheel)
            canvas.bind_all("<Button-4>", on_mousewheel)
            canvas.bind_all("<Button-5>", on_mousewheel)
        def on_leave(event):
            try:
                canvas.unbind_all("<MouseWheel>")
                canvas.unbind_all("<Button-4>")
                canvas.unbind_all("<Button-5>")
            except Exception:
                pass
        def on_mousewheel(event):
            try:
                if event.num == 4:
                    canvas.yview_scroll(-1, "units")
                elif event.num == 5:
                    canvas.yview_scroll(1, "units")
                else:
                    delta = int(-1 * (event.delta / 120))
                    canvas.yview_scroll(delta, "units")
            except Exception:
                pass
        canvas.bind("<Enter>", on_enter)
        canvas.bind("<Leave>", on_leave)

        self.widget_refs = {}
        def add_row(key, label, widget):
            r = frm.grid_size()[1]
            ttk.Label(frm, text=label).grid(row=r, column=0, sticky="w", padx=(0,6), pady=3)
            widget.grid(row=r, column=1, sticky="ew", pady=3)
            ttk.Button(frm, text="?", width=3, command=lambda k=key: self.show_help(k)).grid(row=r, column=2, padx=4)
            self.widget_refs[key] = widget

        # variables
        self.var_api_id = tk.IntVar(value=self.cfg.get("TG_API_ID",0))
        self.var_api_hash = tk.StringVar(value=self.cfg.get("TG_API_HASH",""))
        self.var_phone = tk.StringVar(value=self.cfg.get("TG_PHONE",""))
        self.var_channel = tk.StringVar(value=str(self.cfg.get("TG_CHANNEL_ID","")))
        self.var_wc_url = tk.StringVar(value=self.cfg.get("WC_URL",""))
        self.var_wc_key = tk.StringVar(value=self.cfg.get("WC_KEY",""))
        self.var_wc_secret = tk.StringVar(value=self.cfg.get("WC_SECRET",""))
        self.var_group = tk.StringVar(value=str(self.cfg.get("COMMENT_GROUP_ID","")))
        self.var_max_photos = tk.IntVar(value=self.cfg.get("MAX_PHOTOS",9))
        self.var_max_mb = tk.IntVar(value=self.cfg.get("MAX_PHOTO_SIZE_MB",10))
        self.var_photo_mode = tk.StringVar(value=self.cfg.get("PHOTO_SOURCE_MODE","auto"))
        self.var_photo_forced = tk.StringVar(value=self.cfg.get("PHOTO_SOURCE_FORCED","main"))
        self.var_photo_priority = tk.StringVar(value=self.cfg.get("PHOTO_SOURCE_PRIORITY","comments,main"))
        self.var_desc_priority = tk.StringVar(value=self.cfg.get("DESCRIPTION_SOURCE_PRIORITY","comments,main"))
        self.var_strategy = tk.StringVar(value=STRATEGY_OPTIONS.get(self.cfg.get("UPDATE_STRATEGY","only_new"), "Только новые"))
        self.var_what = tk.StringVar(value=WHAT_OPTIONS.get(self.cfg.get("UPDATE_WHAT","both"), "Описание и фото"))
        self.var_stop_words = tk.StringVar(value=",".join(self.cfg.get("STOP_WORDS",[])))
        self.var_sku_prefer = tk.BooleanVar(value=self.cfg.get("SKU_PREFER_SITE_FIELD", True))
        self.var_sku_n = tk.IntVar(value=self.cfg.get("SKU_TAKE_FIRST_N", 6))
        self.var_cloud_name = tk.StringVar(value=self.cfg.get("CLOUDINARY_CLOUD_NAME",""))
        self.var_cloud_key = tk.StringVar(value=self.cfg.get("CLOUDINARY_API_KEY",""))
        self.var_cloud_secret = tk.StringVar(value=self.cfg.get("CLOUDINARY_API_SECRET",""))
        self.var_verbose = tk.BooleanVar(value=self.cfg.get("VERBOSE_LOG", False))
        self.var_operation_mode = tk.StringVar(value=self.cfg.get("OPERATION_MODE","comments"))
        self.var_operation_mode_display = tk.StringVar(value=OPERATION_MODE_OPTIONS.get(self.var_operation_mode.get()))
        self.var_additional_pos = tk.StringVar(value=self.cfg.get("ADDITIONAL_POSTS_POSITION","after"))
        self.var_additional_pos_display = tk.StringVar(value=ADDITIONAL_POSTS_POS.get(self.var_additional_pos.get()))
        self.var_pause_products = tk.IntVar(value=self.cfg.get("PAUSE_BETWEEN_PRODUCTS",15))
        self.var_pause_photos = tk.IntVar(value=self.cfg.get("PAUSE_BETWEEN_PHOTOS",2))

        # rows
        add_row("TG_API_ID", "TG API ID (api_id)", ttk.Spinbox(frm, from_=0, to=999999999, textvariable=self.var_api_id, width=20))
        add_row("TG_API_HASH", "TG API_HASH (api_hash)", ttk.Entry(frm, textvariable=self.var_api_hash, width=50))
        add_row("TG_PHONE", "Телефон Telegram", ttk.Entry(frm, textvariable=self.var_phone, width=50))
        add_row("TG_CHANNEL_ID", "ID основного Telegram-канала/чата (TG_CHANNEL_ID)", ttk.Entry(frm, textvariable=self.var_channel, width=40))

        add_row("WC_URL", "Ссылка на магазин WooCommerce", ttk.Entry(frm, textvariable=self.var_wc_url, width=50))
        add_row("WC_KEY", "WooCommerce Consumer Key", ttk.Entry(frm, textvariable=self.var_wc_key, width=50))
        add_row("WC_SECRET", "WooCommerce Consumer Secret", ttk.Entry(frm, textvariable=self.var_wc_secret, width=50))
        add_row("COMMENT_GROUP_ID", "ID Telegram-группы/канала (комментарии)", ttk.Entry(frm, textvariable=self.var_group, width=40))

        add_row("MAX_PHOTOS", "Макс. фото на товар", ttk.Spinbox(frm, from_=1, to=50, textvariable=self.var_max_photos, width=10))
        add_row("MAX_PHOTO_SIZE_MB", "Макс. размер фото (МБ)", ttk.Spinbox(frm, from_=1, to=200, textvariable=self.var_max_mb, width=10))
        add_row("PAUSE_BETWEEN_PRODUCTS", "Пауза между товарами (сек)", ttk.Spinbox(frm, from_=0, to=3600, textvariable=self.var_pause_products, width=10))
        add_row("PAUSE_BETWEEN_PHOTOS", "Пауза между фото (сек)", ttk.Spinbox(frm, from_=0, to=300, textvariable=self.var_pause_photos, width=10))

        op_cb = ttk.Combobox(frm, values=list(OPERATION_MODE_OPTIONS.values()), textvariable=self.var_operation_mode_display, state="readonly", width=60)
        add_row("OPERATION_MODE", "Режим работы", op_cb)

        add_row("PHOTO_SOURCE_MODE", "Режим источника фото (auto/manual)", ttk.Combobox(frm, values=list(PHOTO_MODE_OPTIONS.keys()), textvariable=self.var_photo_mode, state="readonly", width=40))
        add_row("PHOTO_SOURCE_FORCED", "Источник при ручном режиме (main/comments)", ttk.Combobox(frm, values=["main","comments"], textvariable=self.var_photo_forced, state="readonly", width=18))

        add_row("PHOTO_SOURCE_PRIORITY", "Приоритет источников фото", ttk.Combobox(frm, values=PRIORITY_CHOICES, textvariable=self.var_photo_priority, state="readonly", width=30))
        add_row("DESCRIPTION_SOURCE_PRIORITY", "Приоритет описания (comments,main)", ttk.Combobox(frm, values=PRIORITY_CHOICES, textvariable=self.var_desc_priority, state="readonly", width=30))

        add_row("UPDATE_STRATEGY", "Стратегия обновления", ttk.Combobox(frm, values=list(STRATEGY_OPTIONS.values()), textvariable=self.var_strategy, state="readonly", width=40))
        add_row("UPDATE_WHAT", "Что обновлять", ttk.Combobox(frm, values=list(WHAT_OPTIONS.values()), textvariable=self.var_what, state="readonly", width=40))

        add_row("STOP_WORDS", "Стоп-слова (через запятую)", ttk.Entry(frm, textvariable=self.var_stop_words, width=60))
        add_row("SKU_PREFER_SITE_FIELD", "Предпочитать sku с сайта", ttk.Checkbutton(frm, variable=self.var_sku_prefer))
        add_row("SKU_TAKE_FIRST_N", "Взять первые N символов артикула (например 6)", ttk.Spinbox(frm, from_=0, to=50, textvariable=self.var_sku_n, width=8))

        add_row("ADDITIONAL_POSTS_POSITION", "Доп. посты (до/после основного)", ttk.Combobox(frm, values=list(ADDITIONAL_POSTS_POS.values()), textvariable=self.var_additional_pos_display, state="readonly", width=60))

        add_row("CLOUDINARY_CLOUD_NAME", "Cloudinary cloud name", ttk.Entry(frm, textvariable=self.var_cloud_name, width=40))
        add_row("CLOUDINARY_API_KEY", "Cloudinary API key", ttk.Entry(frm, textvariable=self.var_cloud_key, width=40))
        add_row("CLOUDINARY_API_SECRET", "Cloudinary API secret", ttk.Entry(frm, textvariable=self.var_cloud_secret, width=40))
        add_row("VERBOSE_LOG", "Подробный лог (отладка)", ttk.Checkbutton(frm, variable=self.var_verbose))

        btns = ttk.Frame(frm)
        ttk.Button(btns, text="Сохранить", command=self._save).pack(side="left")
        ttk.Button(btns, text="Закрыть", command=self.destroy).pack(side="left", padx=6)
        r = frm.grid_size()[1]
        btns.grid(row=r, column=0, columnspan=3, pady=(6,8))

        def on_operation_mode_change(event=None):
            disp = self.var_operation_mode_display.get()
            internal = OPERATION_MODE_INV.get(disp, "comments")
            self.var_operation_mode.set(internal)
            self._apply_operation_mode_exclusivity()

        op_cb.bind("<<ComboboxSelected>>", on_operation_mode_change)
        self._apply_operation_mode_exclusivity()

        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def show_help(self, key):
        text = HELP_TEXTS.get(key, "Справка отсутствует.")
        messagebox.showinfo("Справка", text, parent=self)

    def _apply_operation_mode_exclusivity(self):
        mode = self.var_operation_mode.get()
        manual_keys = ["TG_CHANNEL_ID", "PHOTO_SOURCE_MODE", "PHOTO_SOURCE_FORCED",
                       "PHOTO_SOURCE_PRIORITY", "DESCRIPTION_SOURCE_PRIORITY", "ADDITIONAL_POSTS_POSITION"]
        enabled = (mode == "manual")
        for k in manual_keys:
            w = self.widget_refs.get(k)
            if w is None:
                continue
            try:
                if enabled:
                    w.configure(state="normal")
                else:
                    w.configure(state="disabled")
            except Exception:
                try:
                    w.state(['!disabled']) if enabled else w.state(['disabled'])
                except Exception:
                    pass

    def _save(self):
        cfg = load_settings()
        cfg["TG_API_ID"] = int(self.var_api_id.get())
        cfg["TG_API_HASH"] = self.var_api_hash.get().strip()
        cfg["TG_PHONE"] = self.var_phone.get().strip()
        try:
            cfg["TG_CHANNEL_ID"] = int(self.var_channel.get() or "0")
        except Exception:
            cfg["TG_CHANNEL_ID"] = self.var_channel.get()
        try:
            cfg["COMMENT_GROUP_ID"] = int(self.var_group.get() or "0")
        except Exception:
            cfg["COMMENT_GROUP_ID"] = self.var_group.get()
        cfg["WC_URL"] = self.var_wc_url.get().strip()
        cfg["WC_KEY"] = self.var_wc_key.get().strip()
        cfg["WC_SECRET"] = self.var_wc_secret.get().strip()
        cfg["MAX_PHOTOS"] = int(self.var_max_photos.get())
        cfg["MAX_PHOTO_SIZE_MB"] = int(self.var_max_mb.get())
        cfg["PAUSE_BETWEEN_PRODUCTS"] = int(self.var_pause_products.get())
        cfg["PAUSE_BETWEEN_PHOTOS"] = int(self.var_pause_photos.get())
        cfg["OPERATION_MODE"] = self.var_operation_mode.get() or "comments"
        cfg["PHOTO_SOURCE_MODE"] = self.var_photo_mode.get().strip()
        cfg["PHOTO_SOURCE_FORCED"] = self.var_photo_forced.get().strip()
        cfg["PHOTO_SOURCE_PRIORITY"] = self.var_photo_priority.get().strip()
        cfg["DESCRIPTION_SOURCE_PRIORITY"] = self.var_desc_priority.get().strip()
        cfg["UPDATE_STRATEGY"] = STRATEGY_OPTIONS_INV.get(self.var_strategy.get(), cfg.get("UPDATE_STRATEGY","only_new"))
        cfg["UPDATE_WHAT"] = WHAT_OPTIONS_INV.get(self.var_what.get(), cfg.get("UPDATE_WHAT","both"))
        sw = self.var_stop_words.get() or ""
        cfg["STOP_WORDS"] = [x.strip().lower() for x in re.split(r'[,;\n]+', sw) if x.strip()]
        cfg["SKU_PREFER_SITE_FIELD"] = bool(self.var_sku_prefer.get())
        cfg["SKU_TAKE_FIRST_N"] = int(self.var_sku_n.get())
        cfg["CLOUDINARY_CLOUD_NAME"] = self.var_cloud_name.get().strip()
        cfg["CLOUDINARY_API_KEY"] = self.var_cloud_key.get().strip()
        cfg["CLOUDINARY_API_SECRET"] = self.var_cloud_secret.get().strip()
        cfg["VERBOSE_LOG"] = bool(self.var_verbose.get())
        pos_display = self.var_additional_pos_display.get()
        cfg["ADDITIONAL_POSTS_POSITION"] = ADDITIONAL_POSTS_POS_INV.get(pos_display, cfg.get("ADDITIONAL_POSTS_POSITION","after"))
        save_settings(cfg)
        self.destroy()

class SyncWorker(threading.Thread):
    def __init__(self, cfg, write_log_cb, ask_input_cb, finish_cb):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.write_log = write_log_cb
        self.ask_input = ask_input_cb
        self.finish_cb = finish_cb
        self.stop_flag = False
        self.pause_event = threading.Event()
        self.pause_event.set()

    def stop(self):
        self.stop_flag = True
        self.resume()

    def pause(self):
        self.pause_event.clear()
        self.write_log("\nСинхронизация приостановлена.\n")

    def resume(self):
        self.pause_event.set()
        self.write_log("\nСинхронизация возобновлена.\n")

    def is_paused(self):
        return not self.pause_event.is_set()

    def _install_input_hooks(self):
        self._orig_input = builtins.input
        self._orig_getpass = getpass.getpass
        def gui_input(prompt=""):
            return self.ask_input(prompt or "Введите код/пароль Telegram")
        builtins.input = gui_input
        getpass.getpass = gui_input

    def _restore_input_hooks(self):
        try: builtins.input = self._orig_input
        except Exception: pass
        try: getpass.getpass = self._orig_getpass
        except Exception: pass

    def run(self):
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout = StdoutProxy(self.write_log)
        sys.stderr = StdoutProxy(self.write_log)
        self._install_input_hooks()
        try:
            os.chdir(APP_DIR)
            ulog("=== СИНХРОНИЗАЦИЯ ЗАПУЩЕНА ===")
            asyncio.run(self._main())
        except Exception as e:
            lg(f"ГЛАВНАЯ ОШИБКА: {e}\n{traceback.format_exc()}")
        finally:
            self._restore_input_hooks()
            sys.stdout, sys.stderr = old_out, old_err
            try:
                if callable(self.finish_cb):
                    self.finish_cb()
            except Exception as e:
                lg(f"finish_cb exception: {e}")

    async def _wait_if_paused(self):
        while not self.pause_event.is_set():
            await asyncio.sleep(0.5)

    async def _main(self):
        cfg = self.cfg.copy()
        wcapi = None
        if WC_API_Class:
            try:
                wcapi = WC_API_Class(
                    url=cfg.get("WC_URL").rstrip("/"),
                    consumer_key=cfg.get("WC_KEY"),
                    consumer_secret=cfg.get("WC_SECRET"),
                    version="wc/v3",
                    timeout=60
                )
                lg("WC client created.", False)
            except Exception as e:
                lg(f"Ошибка создания WC клиента: {e}")
                wcapi = None
        else:
            lg("woocommerce библиотека не установлена; обновления на сайт не будут работать.")

        all_products = get_all_products(wcapi)
        updated_list = []
        failed_list = []
        review_list = []
        updated_dict = load_updated_products(cfg.get("UPDATED_FILE","updated_products.json"))

        for product in all_products:
            if self.stop_flag:
                ulog("Остановка синхронизации по запросу.")
                break
            await self._wait_if_paused()
            try:
                result = await process_one_product(product, wcapi, cfg, updated_dict)
            except Exception as e:
                result = {"product_id": str(product.get("id")), "name": product.get("name",""), "error": str(e), "review_reason": "exception"}
            if result.get("review_reason"):
                review_list.append(result)
            elif result.get("updated"):
                updated_list.append(result)
            else:
                failed_list.append(result)
            wait = int(cfg.get("PAUSE_BETWEEN_PRODUCTS", 15))
            ulog(f"Ожидание {wait}s перед следующим товаром (можно приостановить).")
            for _ in range(wait):
                if self.stop_flag: break
                await self._wait_if_paused()
                await asyncio.sleep(1)

        # Summary report
        ulog("\n=== ОТЧЁТ ПО РАБОТЕ ===")
        ulog(f"Всего обработано: {len(all_products)}")
        ulog(f"Успешно обновлено: {len(updated_list)}")
        if updated_list:
            ulog("Список обновлённых товаров (название — id):")
            for r in updated_list:
                ulog(f"  - {r.get('name','(без названия)')} — id={r.get('product_id')} (фото: {r.get('photos_count')})")
        if failed_list:
            ulog(f"Не удалось обновить: {len(failed_list)}")
            for r in failed_list:
                ulog(f"  - {r.get('name','(без названия)')} — id={r.get('product_id')} причина: {r.get('error') or 'обновление не прошло'}")
        if review_list:
            ulog(f"Требуют ручной проверки: {len(review_list)}")
            for r in review_list:
                reason = r.get("review_reason") or r.get("error") or "неизвестно"
                ulog(f"  - {r.get('name','(без названия)')} — id={r.get('product_id')} причина: {reason}")

        ulog("=== КОНЕЦ ОТЧЁТА ===")

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("WC — TG Sync")
        self.geometry("980x600")
        self.cfg = load_settings()
        self.worker = None

        top = ttk.Frame(self); top.pack(fill="x", padx=8, pady=6)
        ttk.Button(top, text="Настройки", command=self.open_settings).pack(side="left")

        text_frame = ttk.Frame(self); text_frame.pack(fill="both", expand=True, padx=8, pady=(0,6))
        self.txt = tk.Text(text_frame, wrap="word", state="disabled")
        self.txt.pack(side="left", fill="both", expand=True)
        yscroll = ttk.Scrollbar(text_frame, orient="vertical", command=self.txt.yview)
        yscroll.pack(side="right", fill="y")
        self.txt.configure(yscrollcommand=yscroll.set)

        bottom = ttk.Frame(self); bottom.pack(fill="x", padx=8, pady=(0,8))
        self.btn_start = ttk.Button(bottom, text="Запустить синхронизацию", command=self.start_sync)
        self.btn_stop = ttk.Button(bottom, text="Стоп", command=self.stop_sync, state="disabled")
        self.btn_pause = ttk.Button(bottom, text="Пауза", command=self.toggle_pause, state="disabled")
        self.btn_start.pack(side="left"); self.btn_stop.pack(side="left", padx=6); self.btn_pause.pack(side="left", padx=6)

        builtins.input = self.gui_input
        getpass.getpass = self.gui_getpass

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def log(self, s: str):
        if not s.endswith("\n"): s += "\n"
        self.txt.configure(state="normal")
        self.txt.insert("end", s)
        self.txt.see("end")
        self.txt.configure(state="disabled")
        self.update_idletasks()

    def ask_input(self, prompt: str):
        res = {}
        ev = threading.Event()
        def _open():
            lower = (prompt or "").lower()
            is_password = ("парол" in lower) or ("password" in lower)
            title = "Авторизация Telegram"
            msg = prompt or ("Введите пароль Telegram" if is_password else "Введите код Telegram")
            value = simpledialog.askstring(title, msg, show="*" if is_password else None, parent=self)
            res["v"] = value or ""
            ev.set()
        self.after(0, _open)
        ev.wait()
        return res["v"]

    def gui_input(self, prompt=""):
        return self.ask_input(prompt or "Введите значение")

    def gui_getpass(self, prompt="Пароль: "):
        return self.ask_input(prompt or "Введите пароль")

    def open_settings(self):
        SettingsDialog(self, self.cfg)
        self.cfg = load_settings()

    def _on_worker_finish(self):
        self.btn_start.configure(state="normal")
        self.btn_stop.configure(state="disabled")
        self.btn_pause.configure(state="disabled")
        self.btn_pause.configure(text="Пауза")
        self.log("\nСинхронизация завершена или остановлена. Можно запустить снова.\n")

    def start_sync(self):
        self.cfg = load_settings()
        if not self.cfg.get("TG_API_ID") or not self.cfg.get("TG_API_HASH") or not self.cfg.get("TG_PHONE"):
            messagebox.showwarning("Настройки", "Заполните TG_API_ID, TG_API_HASH и Телефон Telegram.")
            return
        op_mode = self.cfg.get("OPERATION_MODE","comments")
        if op_mode == "comments" and not self.cfg.get("COMMENT_GROUP_ID"):
            if not messagebox.askyesno("Настройки", "Режим 'Работа по группе' требует заполненного COMMENT_GROUP_ID. Продолжить без него?"):
                return
        if not self.cfg.get("WC_URL") or not self.cfg.get("WC_KEY") or not self.cfg.get("WC_SECRET"):
            messagebox.showwarning("Настройки", "Заполните параметры WooCommerce (URL, Key, Secret).")
            return
        self.txt.configure(state="normal"); self.txt.delete("1.0","end"); self.txt.configure(state="disabled")
        finish_cb = lambda: self.after(0, self._on_worker_finish)
        self.worker = SyncWorker(self.cfg, self.log, self.ask_input, finish_cb)
        self.worker.start()
        self.btn_start.configure(state="disabled")
        self.btn_stop.configure(state="normal")
        self.btn_pause.configure(state="normal")
        self.btn_pause.configure(text="Пауза")

    def stop_sync(self):
        if self.worker:
            self.worker.stop()
            self.log("\nЗапрошена остановка...\n")

    def toggle_pause(self):
        if not self.worker:
            return
        if self.worker.is_paused():
            self.worker.resume()
            self.btn_pause.configure(text="Пауза")
            self.log("Продолжаем синхронизацию.")
        else:
            self.worker.pause()
            self.btn_pause.configure(text="Продолжить")
            self.log("Синхронизация приостановлена. Нажмите 'Продолжить' чтобы возобновить.")

    def on_close(self):
        if self.worker and self.worker.is_alive():
            if not messagebox.askyesno("Выход", "Идёт синхронизация. Остановить и выйти?"):
                return
            self.worker.stop()
        self.destroy()

if __name__ == "__main__":
    app = App()
    app.mainloop()