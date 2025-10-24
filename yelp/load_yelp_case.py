#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build & Load Yelp Running Case (S0–S8) into MongoDB, with per-version cap.

Quick install:
  pip install pymongo python-dateutil
"""

import argparse
import json
import re
from typing import Dict, Any, Iterable, Optional, Tuple
from pymongo import MongoClient
from dateutil import parser as dateparser

# ---------------------- IO ----------------------

def stream_ndjson(path: str, limit: Optional[int]=None) -> Iterable[Dict[str, Any]]:
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            yield obj
            n += 1
            if limit is not None and n >= limit:
                break

def stream_ndjson_with_limit(path: str, limit: int) -> Iterable[Dict[str, Any]]:
    return stream_ndjson(path, None if not limit else limit)

# ---------------------- Mongo helpers ----------------------

def ensure_collection_with_validator(db, coll_name: str, json_schema: Dict[str, Any]):
    exists = coll_name in db.list_collection_names()
    validator = {"$jsonSchema": json_schema}
    if not exists:
        db.create_collection(coll_name, validator=validator)
    else:
        db.command("collMod", coll_name, validator=validator)

def create_or_empty_collection(db, coll_name: str):
    if coll_name in db.list_collection_names():
        db[coll_name].delete_many({})
    else:
        db.create_collection(coll_name)

def batch_insert(coll, docs, batch_size: int):
    buf = []
    total = 0
    for d in docs:
        buf.append(d)
        if len(buf) >= batch_size:
            coll.insert_many(buf, ordered=False)
            total += len(buf)
            buf.clear()
    if buf:
        coll.insert_many(buf, ordered=False)
        total += len(buf)
    return total

# ---------------------- JSON Schema validators ----------------------

def schema_S0() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "stars": {"bsonType": ["int", "long", "double"]},
            "date": {"bsonType": "string"},
            "text": {"bsonType": "string"},
            "useful": {"bsonType": ["int", "long"]},
            "funny": {"bsonType": ["int", "long"]},
            "cool": {"bsonType": ["int", "long"]},
        }
    }

def schema_S1() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "text"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["int", "long", "double"], "minimum": 1, "maximum": 5},
            "date": {"bsonType": "string"},  # ISO8601
            "text": {"bsonType": "string"},
            "useful": {"bsonType": ["int", "long"]},
            "funny": {"bsonType": ["int", "long"]},
            "cool": {"bsonType": ["int", "long"]},
        }
    }

def schema_S2() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["int", "long", "double"], "minimum": 1, "maximum": 5},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {
                "bsonType": "object",
                "properties": {
                    "useful": {"bsonType": ["int", "long"]},
                    "funny": {"bsonType": ["int", "long"]},
                    "cool": {"bsonType": ["int", "long"]},
                    "tags": {"bsonType": ["array"], "items": {"bsonType": "string"}}
                }
            }
        }
    }

def schema_S3() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["int", "long", "double"]},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {"bsonType": "object"},
            "embedded_business": {
                "bsonType": "object",
                "required": ["name", "categories", "city", "state"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "categories": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "city": {"bsonType": "string"},
                    "state": {"bsonType": "string"}
                }
            }
        }
    }

def schema_S4() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions", "rating_detail"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["double", "int", "long"], "minimum": 1, "maximum": 5},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {"bsonType": "object"},
            "embedded_business": {"bsonType": "object"},
            "rating_detail": {
                "bsonType": "object",
                "required": ["taste", "service", "env"],
                "properties": {
                    "taste": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 5},
                    "service": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 5},
                    "env": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 5},
                }
            }
        }
    }

def schema_S5() -> Dict[str, Any]:
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions", "rating_detail"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["double", "int", "long"]},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {
                "bsonType": "object",
                "properties": {
                    "useful": {"bsonType": ["int", "long"]},
                    "funny": {"bsonType": ["int", "long"]},
                    "cool": {"bsonType": ["int", "long"]},
                    "tags": {"bsonType": ["array"], "items": {"bsonType": "string"}},
                    "summary": {"bsonType": "string"}
                }
            },
            "embedded_business": {"bsonType": "object"},
            "rating_detail": {"bsonType": "object"},
            "rating_avg": {"bsonType": ["double", "int", "long"]},
        }
    }

def schema_S6() -> Dict[str, Any]:
    allowed_states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
        "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA",
        "RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
    ]
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions", "rating_detail"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["double", "int", "long"], "minimum": 1, "maximum": 5},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {"bsonType": "object"},
            "embedded_business": {
                "bsonType": "object",
                "properties": {
                    "name": {"bsonType": "string"},
                    "categories": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "city": {"bsonType": "string"},
                    "state": {"bsonType": "string", "enum": allowed_states}
                }
            },
            "rating_detail": {
                "bsonType": "object",
                "required": ["taste", "service", "env"],
                "properties": {
                    "taste": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 10},
                    "service": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 10},
                    "env": {"bsonType": ["int", "long"], "minimum": 1, "maximum": 10},
                }
            },
            "rating_avg": {"bsonType": ["double", "int", "long"]},
        }
    }

def schema_S7() -> Dict[str, Any]:
    # ToScalar: reactions.tags -> reactions.tags_csv (string)
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions", "rating_detail"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["double", "int", "long"]},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {
                "bsonType": "object",
                "properties": {
                    "useful": {"bsonType": ["int", "long"]},
                    "funny": {"bsonType": ["int", "long"]},
                    "cool": {"bsonType": ["int", "long"]},
                    "summary": {"bsonType": "string"},
                    "tags_csv": {"bsonType": "string"}  # CSV string after ToScalar
                }
            },
            "embedded_business": {"bsonType": "object"},
            "rating_detail": {"bsonType": "object"},
            "rating_avg": {"bsonType": ["double", "int", "long"]},
        }
    }

def schema_S8() -> Dict[str, Any]:
    # ToArray: add actors array from user_id; keep user_id for backward compatibility
    return {
        "bsonType": "object",
        "required": ["review_id", "user_id", "business_id", "rating", "date", "title", "body", "reactions", "rating_detail", "actors"],
        "properties": {
            "review_id": {"bsonType": "string"},
            "user_id": {"bsonType": "string"},
            "business_id": {"bsonType": "string"},
            "rating": {"bsonType": ["double", "int", "long"]},
            "date": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "body": {"bsonType": "string"},
            "reactions": {"bsonType": "object"},
            "embedded_business": {"bsonType": "object"},
            "rating_detail": {"bsonType": "object"},
            "rating_avg": {"bsonType": ["double", "int", "long"]},
            "actors": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["role", "user_id"],
                    "properties": {
                        "role": {"bsonType": "string"},
                        "user_id": {"bsonType": "string"}
                    }
                }
            }
        }
    }

# ---------------------- Transforms ----------------------

def s1_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    if "stars" in out and "rating" not in out:
        out["rating"] = out.pop("stars")
    try:
        d = out.get("date")
        if isinstance(d, str):
            out["date"] = dateparser.parse(d).isoformat()
    except Exception:
        pass
    return out

SENT_SPLIT = re.compile(r'(?<=[\.\!\?。！？])\s+')

def split_title_body(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    parts = SENT_SPLIT.split(text.strip(), maxsplit=1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]

def s2_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    if "rating" not in out and "stars" in out:
        out["rating"] = out.get("stars")
    title, body = split_title_body(out.get("text", ""))
    out["title"] = title or ""
    out["body"] = body or ""
    reactions = {
        "useful": out.pop("useful", 0),
        "funny": out.pop("funny", 0),
        "cool": out.pop("cool", 0),
        "tags": []
    }
    out["reactions"] = reactions
    return out

def s3_transform(doc: Dict[str, Any], biz_lookup: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(doc)
    bid = out.get("business_id")
    emb = biz_lookup.get(bid)
    if emb:
        out["embedded_business"] = emb
    return out

def s4_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    try:
        r = out.get("rating")
        if r is not None:
            out["rating"] = float(r)
    except Exception:
        pass
    base = int(round(out.get("rating", 3))) if isinstance(out.get("rating"), (int, float)) else 3
    rd = {
        "taste": max(1, min(5, base)),
        "service": max(1, min(5, base)),
        "env": max(1, min(5, base))
    }
    out["rating_detail"] = rd
    return out

def s5_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    u = out.get("reactions", {}).get("useful", 0) or 0
    f = out.get("reactions", {}).get("funny", 0) or 0
    c = out.get("reactions", {}).get("cool", 0) or 0
    out.setdefault("reactions", {})
    out["reactions"]["summary"] = f"useful:{u}|funny:{f}|cool:{c}"
    rd = out.get("rating_detail")
    if isinstance(rd, dict) and all(k in rd for k in ("taste","service","env")):
        out["rating_avg"] = float((rd["taste"] + rd["service"] + rd["env"]) / 3.0)
    else:
        try:
            out["rating_avg"] = float(out.get("rating", 0))
        except Exception:
            out["rating_avg"] = 0.0
    return out

def s6_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    rd = out.get("rating_detail")
    if isinstance(rd, dict):
        for k in ("taste","service","env"):
            v = rd.get(k)
            if isinstance(v, (int, float)):
                newv = int(max(1, min(10, round(v * 2))))
                rd[k] = newv
        out["rating_detail"] = rd
    return out

def s7_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    tags = out.get("reactions", {}).get("tags", [])
    if isinstance(tags, list):
        csv = ",".join(str(t).strip() for t in tags if str(t).strip())
    else:
        csv = ""
    out.setdefault("reactions", {})
    out["reactions"]["tags_csv"] = csv
    # 可选：保留 tags 原字段以便回溯；也可以删除
    if "tags" in out["reactions"]:
        del out["reactions"]["tags"]
    return out

def s8_transform(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    uid = out.get("user_id")
    out["actors"] = [{"role": "author", "user_id": uid}] if uid else []
    return out

# ---------------------- Main ----------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo-uri", required=True)
    ap.add_argument("--db", default="yelp_case")
    ap.add_argument("--review", required=True, help="Path to yelp_academic_dataset_review.json")
    ap.add_argument("--business", required=True, help="Path to yelp_academic_dataset_business.json")
    ap.add_argument("--user", required=True, help="Path to yelp_academic_dataset_user.json")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=None, help="Limit number of reviews (read-side)")
    ap.add_argument("--versions", default="S0,S1,S2,S3,S4,S5,S6,S7,S8",
                    help="Comma separated review versions to build, e.g. S0,S1,S2")
    ap.add_argument("--skip_aux", action="store_true",
                    help="Skip loading businesses/users and skip S3 embedding")
    ap.add_argument("--aux-limit", type=int, default=0,
                    help="Limit rows for businesses/users (0 = no limit)")
    ap.add_argument("--per-version-limit", type=int, default=100000,
                    help="Max documents per version collection (cap).")
    args = ap.parse_args()

    versions = tuple(v.strip().upper() for v in args.versions.split(",") if v.strip())
    versions_set = set(versions)
    allowed = {"S0","S1","S2","S3","S4","S5","S6","S7","S8"}
    if not versions_set.issubset(allowed):
        raise SystemExit(f"--versions 只允许: {','.join(sorted(allowed))}")

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    # Prepare validators and empty collections
    if "S0" in versions_set: ensure_collection_with_validator(db, "reviews_S0", schema_S0())
    if "S1" in versions_set: ensure_collection_with_validator(db, "reviews_S1", schema_S1())
    if "S2" in versions_set: ensure_collection_with_validator(db, "reviews_S2", schema_S2())
    if "S3" in versions_set: ensure_collection_with_validator(db, "reviews_S3", schema_S3())
    if "S4" in versions_set: ensure_collection_with_validator(db, "reviews_S4", schema_S4())
    if "S5" in versions_set: ensure_collection_with_validator(db, "reviews_S5", schema_S5())
    if "S6" in versions_set: ensure_collection_with_validator(db, "reviews_S6", schema_S6())
    if "S7" in versions_set: ensure_collection_with_validator(db, "reviews_S7", schema_S7())
    if "S8" in versions_set: ensure_collection_with_validator(db, "reviews_S8", schema_S8())

    for c in ("S0","S1","S2","S3","S4","S5","S6","S7","S8"):
        if c in versions_set:
            create_or_empty_collection(db, f"reviews_{c}")

    # Aux collections and embedding lookup
    biz_lookup: Dict[str, Dict[str, Any]] = {}
    if not args.skip_aux:
        if "businesses" not in db.list_collection_names():
            db.create_collection("businesses")
        else:
            db["businesses"].delete_many({})
        if "users" not in db.list_collection_names():
            db.create_collection("users")
        else:
            db["users"].delete_many({})

        print("[*] Loading businesses...")
        biz_count = batch_insert(db["businesses"],
                                 stream_ndjson_with_limit(args.business, args.aux_limit),
                                 args.batch_size)
        print(f"[OK] businesses inserted: {biz_count}")

        print("[*] Loading users...")
        user_count = batch_insert(db["users"],
                                  stream_ndjson_with_limit(args.user, args.aux_limit),
                                  args.batch_size)
        print(f"[OK] users inserted: {user_count}")

        if "S3" in versions_set or "S6" in versions_set:
            print("[*] Building business lookup for embedding (S3/S6)...")
            for doc in stream_ndjson(args.business):
                bid = doc.get("business_id")
                if not bid:
                    continue
                cats = doc.get("categories")
                if isinstance(cats, str):
                    cats_list = [c.strip() for c in cats.split(",") if c.strip()]
                elif isinstance(cats, list):
                    cats_list = cats
                else:
                    cats_list = []
                biz_lookup[bid] = {
                    "name": doc.get("name"),
                    "categories": cats_list,
                    "city": doc.get("city"),
                    "state": doc.get("state"),
                    "stars": doc.get("stars"),
                }
            print(f"[OK] business lookup size: {len(biz_lookup)}")

    # Handles
    colls = {v: db[f"reviews_{v}"] for v in versions_set}
    buffers: Dict[str, list] = {v: [] for v in versions_set}
    inserted: Dict[str, int] = {v: 0 for v in versions_set}
    bsize = args.batch_size

    print("[*] Processing reviews with per-version cap =", args.per_version_limit)
    processed = 0

    for r in stream_ndjson(args.review, limit=args.limit):
        if all(inserted[v] >= args.per_version_limit for v in versions_set):
            break

        # S0 base
        d0 = {
            "review_id": r.get("review_id"),
            "user_id": r.get("user_id"),
            "business_id": r.get("business_id"),
            "stars": r.get("stars"),
            "date": r.get("date"),
            "text": r.get("text"),
            "useful": r.get("useful", 0),
            "funny": r.get("funny", 0),
            "cool": r.get("cool", 0),
        }

        if "S0" in versions_set and inserted["S0"] < args.per_version_limit:
            buffers["S0"].append(d0.copy())

        d1 = s1_transform(d0)
        if "S1" in versions_set and inserted["S1"] < args.per_version_limit:
            buffers["S1"].append(d1.copy())

        d2 = s2_transform(d1)
        if "S2" in versions_set and inserted["S2"] < args.per_version_limit:
            buffers["S2"].append(d2.copy())

        d3 = s3_transform(d2, biz_lookup) if not args.skip_aux else d2
        if "S3" in versions_set and inserted["S3"] < args.per_version_limit:
            buffers["S3"].append(d3.copy())

        d4 = s4_transform(d3)
        if "S4" in versions_set and inserted["S4"] < args.per_version_limit:
            buffers["S4"].append(d4.copy())

        d5 = s5_transform(d4)
        if "S5" in versions_set and inserted["S5"] < args.per_version_limit:
            buffers["S5"].append(d5.copy())

        d6 = s6_transform(d5)
        if "S6" in versions_set and inserted["S6"] < args.per_version_limit:
            buffers["S6"].append(d6.copy())

        d7 = s7_transform(d6)
        if "S7" in versions_set and inserted["S7"] < args.per_version_limit:
            buffers["S7"].append(d7.copy())

        d8 = s8_transform(d7)
        if "S8" in versions_set and inserted["S8"] < args.per_version_limit:
            buffers["S8"].append(d8.copy())

        processed += 1
        if processed % (bsize*10) == 0:
            for v in versions_set:
                if buffers[v]:
                    remain = args.per_version_limit - inserted[v]
                    if remain > 0:
                        batch = buffers[v][:remain]
                        if batch:
                            colls[v].insert_many(batch, ordered=False)
                            inserted[v] += len(batch)
                    buffers[v].clear()
            print(f"  .. processed {processed} | per-version inserted: {inserted}")
            if all(inserted[v] >= args.per_version_limit for v in versions_set):
                break

    # final flush
    for v in versions_set:
        if buffers[v] and inserted[v] < args.per_version_limit:
            remain = args.per_version_limit - inserted[v]
            batch = buffers[v][:remain]
            if batch:
                colls[v].insert_many(batch, ordered=False)
                inserted[v] += len(batch)
            buffers[v].clear()

    print(f"[DONE] processed reviews: {processed}")
    for v in sorted(versions_set):
        print(f"  - reviews_{v}: {inserted[v]} docs (cap {args.per_version_limit})")

if __name__ == "__main__":
    main()

# 一次性生成 S0–S8，给每版上限 10000；读取 120000 行足够“填满”多数版本。
# python .\load_yelp_case.py `
#   --mongo-uri "mongodb://duwendi:duwendi@223.223.185.189:12130/?authSource=admin" `
#   --db yelp_case `
#   --review ".\datas\yelp_academic_dataset_review.json" `
#   --business ".\datas\yelp_academic_dataset_business.json" `
#   --user ".\datas\yelp_academic_dataset_user.json" `
#   --versions S0,S1,S2,S3,S4,S5,S6,S7,S8 `
#   --per-version-limit 10000 `
#   --limit 120000 `
#   --aux-limit 50000 `
#   --batch-size 1000

#python .\load_yelp_case.py --mongo-uri "mongodb://darwin:darwin_test@223.223.185.189:12130/?authSource=admin" --db yelp_case --review ".\datas\yelp_academic_dataset_review.json" --business ".\datas\yelp_academic_dataset_business.json" --user ".\datas\yelp_academic_dataset_user.json" --versions S0,S1,S2,S3,S4,S5,S6,S7,S8 --per-version-limit 10000 --limit 120000 --aux-limit 50000 --batch-size 1000