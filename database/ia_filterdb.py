import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from typing import Dict, List
from collections import defaultdict
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow import ValidationError
from info import *
from utils import get_settings, save_group_settings
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Global cache for DB size
_db_stats_cache = {"timestamp": None, "primary_size": 0.0}

# Primary DB
client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

# secondary db
client2 = AsyncIOMotorClient(DATABASE_URI2)
db2 = client2[DATABASE_NAME]
instance2 = Instance.from_db(db2)

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    cover = fields.StrField(allow_none=True)
    langs = fields.ListField(fields.StrField(), allow_none=True)

    class Meta:
        indexes = ("$file_name", "langs")
        collection_name = COLLECTION_NAME

@instance2.register
class Media2(Document):
    file_id = fields.StrField(attribute="_id")
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    cover = fields.StrField(allow_none=True)
    langs = fields.ListField(fields.StrField(), allow_none=True)

    class Meta:
        indexes = ("$file_name", "langs")
        collection_name = COLLECTION_NAME

async def check_db_size(db):
    try:
        now = datetime.utcnow()
        cache_stale_by_time = _db_stats_cache["timestamp"] is None or (
            now - _db_stats_cache["timestamp"] > timedelta(minutes=10)
        )
        refresh_if_size_threshold = _db_stats_cache["primary_size"] >= 10.0
        if not cache_stale_by_time and not refresh_if_size_threshold:
            return _db_stats_cache["primary_size"]
        stats = await db.command("dbstats")
        db_size_mb = (stats["dataSize"] + stats["indexSize"]) / (1024 * 1024)
        _db_stats_cache["primary_size"] = db_size_mb
        _db_stats_cache["timestamp"] = now
        return db_size_mb
    except Exception as e:
        print(f"Error Checking Database Size: {e}")
        return 0

async def save_file(media):
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"[_\-\.#+$%^&*()!~`,;:\"'?/<>\[\]{}=|\\]", " ", str(media.file_name))
    file_name = re.sub(r"\s+", " ", file_name).strip()
    saveMedia = Media
    target_db = "Primary"
    if MULTIPLE_DB:
        try:
            exists = await Media.count_documents({"file_id": file_id}, limit=1)
            if exists: return False, 0
            primary_db_size = await check_db_size(db)
            if primary_db_size >= 407:
                saveMedia = Media2
                target_db = "Secondary"
        except: pass
    try:
        cover_to_use = getattr(getattr(media, "cover", None), "file_id", None)
        record = saveMedia(
            file_id=file_id, file_ref=file_ref, file_name=file_name,
            file_size=media.file_size, file_type=media.file_type,
            mime_type=media.mime_type, cover=cover_to_use if COVERX else None,
            caption=(media.caption.html if media.caption and INDEX_CAPTION else None),
        )
        await record.commit()
        return True, 1
    except DuplicateKeyError: return False, 0
    except Exception as e:
        logger.exception(f"[ERROR] {e}")
        return False, 3

async def get_search_results(chat_id, query, file_type=None, max_results=None, offset=0, filter=False):
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        if max_results is None:
            max_results = 7 if settings.get("max_btn") else int(MAX_B_TN)

    if isinstance(query, list):
        raw_pattern = '|'.join(re.escape(q.strip()) for q in query if q.strip())
        regex = re.compile(raw_pattern, re.IGNORECASE)
    else:
        query = query.strip()
        if not query: return [], None, 0
        
        # Smart Language Detection
        lang_map = {"hindi": "hi", "english": "en", "tamil": "ta", "telugu": "te", "malayalam": "ml", "kannada": "kn"}
        found_lang_key = next((l for l in lang_map if l in query.lower()), None)
        
        if found_lang_key:
            clean_query = query.lower().replace(found_lang_key, "").strip()
            regex = re.compile(re.escape(clean_query), flags=re.IGNORECASE)
            filter_mongo = {
                "file_name": regex,
                "$or": [{"langs": lang_map[found_lang_key]}, {"file_name": re.compile(re.escape(found_lang_key), re.IGNORECASE)}]
            }
        else:
            words = [re.escape(word) for word in query.split()]
            raw_pattern = r'.*'.join(words) if ' ' in query else r"\b" + re.escape(query) + r"\b"
            regex = re.compile(raw_pattern, flags=re.IGNORECASE)
            filter_mongo = {"$or": [{"file_name": regex}, {"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}

    if file_type: filter_mongo["file_type"] = file_type

    find_tasks = [Media.find(filter_mongo).sort("$natural", -1).skip(offset).limit(max_results).to_list(length=max_results)]
    if MULTIPLE_DB:
        find_tasks.append(Media2.find(filter_mongo).sort("$natural", -1).skip(offset).limit(max_results).to_list(length=max_results))
    
    results = await asyncio.gather(*find_tasks)
    files = results[0]
    if MULTIPLE_DB and len(results) > 1: files.extend(results[1])
    files = files[:max_results]
    
    total_results = await Media.count_documents(filter_mongo)
    if MULTIPLE_DB: total_results += await Media2.count_documents(filter_mongo)
    
    next_offset = offset + len(files) if offset + len(files) < total_results else ""
    return files, next_offset, total_results

async def update_file_langs(file_id, languages):
    await Media.collection.update_one({"_id": file_id}, {"$set": {"langs": languages}})
    if MULTIPLE_DB:
        await Media2.collection.update_one({"_id": file_id}, {"$set": {"langs": languages}})

async def get_file_details(query):
    filter = {"file_id": query}
    tasks = [Media.find(filter).to_list(length=1)]
    if MULTIPLE_DB: tasks.append(Media2.find(filter).to_list(length=1))
    results = await asyncio.gather(*tasks)
    for res in results:
        if res: return res
    return []

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = base64.urlsafe_b64encode(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash)).decode().rstrip("=")
    file_ref = base64.urlsafe_b64encode(decoded.file_reference).decode().rstrip("=")
    return file_id, file_ref
