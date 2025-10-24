#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_schema_and_ops.py
- 从包含 schema_S0...schema_S8() 的 Python 文件中：
  1) 导出每个版本的 JSON Schema 文件
  2) 计算相邻版本间的模式演化操作，并输出 JSON
"""
import os, sys, json, argparse, importlib.util
from typing import Dict, Any, Tuple, List

def load_module(path: str):
    spec = importlib.util.spec_from_file_location("schemas_mod", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def get_schemas(mod, versions):
    res = {}
    for v in versions:
        fn = getattr(mod, f"schema_{v}", None)
        if fn is None:
            raise RuntimeError(f"{v} not found")
        res[v] = fn()
    return res

# ----------diff helpers----------
def norm_type(t): return tuple(sorted(t)) if isinstance(t, list) else t
def walk(schema: Dict[str, Any], root="$") -> Dict[str, Dict[str, Any]]:
    idx = {}
    def _walk(p, node):
        idx[p] = node
        if node.get("bsonType") == "object":
            for k,v in (node.get("properties") or {}).items():
                _walk(f"{p}.{k}", v)
        elif node.get("bsonType") == "array":
            it = node.get("items")
            if isinstance(it, dict):
                _walk(f"{p}[]", it)
    _walk(root, schema)
    return idx

def type_change(a,b,p):
    oa, ob = norm_type(a.get("bsonType")), norm_type(b.get("bsonType"))
    if oa!=ob:
        if oa=="array" and ob in ("string","object","double","int","long"):
            return {"op":"ToScalar","path":p}
        if ob=="array" and oa in ("string","object","double","int","long"):
            return {"op":"ToArray","path":p}
        return {"op":"ChangeType","path":p,"from":oa,"to":ob}

def required_ops(a,b,p):
    ops=[]
    if a.get("bsonType")=="object" and b.get("bsonType")=="object":
        A=set(a.get("required",[]) or []); B=set(b.get("required",[]) or [])
        for x in B-A: ops.append({"op":"AddRequired","path":f"{p}.{x}"})
        for x in A-B: ops.append({"op":"DropRequired","path":f"{p}.{x}"})
    return ops

def range_enum_ops(a,b,p):
    ops=[]
    # Range
    amin,amax=a.get("minimum"),a.get("maximum")
    bmin,bmax=b.get("minimum"),b.get("maximum")
    if (amin,bmax)!=(bmin,bmax):
        if (amin is None and amax is None) and (bmin is not None or bmax is not None):
            ops.append({"op":"AddRange","path":p,"spec":{"minimum":bmin,"maximum":bmax}})
        elif (bmin is None and bmax is None) and (amin is not None or amax is not None):
            ops.append({"op":"DropRange","path":p})
        elif amin!=bmin or amax!=bmax:
            ops.append({"op":"ModifyRange","path":p,
                        "from":{"minimum":amin,"maximum":amax},
                        "to":{"minimum":bmin,"maximum":bmax}})
    # Enum
    e1,e2=tuple(a.get("enum",[]) or []),tuple(b.get("enum",[]) or [])
    if e1!=e2:
        if not e1 and e2: ops.append({"op":"AddEnum","path":p,"values":list(e2)})
        elif e1 and not e2: ops.append({"op":"DropEnum","path":p})
        else: ops.append({"op":"ModifyEnum","path":p,"from":list(e1),"to":list(e2)})
    return ops

def detect_moves_and_renames(old_idx,new_idx):
    rem=list(set(old_idx)-set(new_idx))
    add=list(set(new_idx)-set(old_idx))
    # 简化：直接按同名匹配 Move
    renames,moves=[],[]
    for r in rem[:]:
        name=r.split(".")[-1]
        for a in add[:]:
            if name==a.split(".")[-1]:
                moves.append((r,a))
                rem.remove(r); add.remove(a)
                break
    return renames,moves,rem,add

def diff_schemas(A,B):
    a=walk(A); b=walk(B)
    rem,add=set(a)-set(b),set(b)-set(a)
    common=set(a)&set(b)
    ops=[]
    renames,moves,rem_eff,add_eff=detect_moves_and_renames(a,b)
    for p in rem_eff: ops.append({"op":"DropField","path":p})
    for p in add_eff: ops.append({"op":"AddField","path":p,"dtype":b[p].get("bsonType")})
    for o,n in moves: ops.append({"op":"MoveField","from":o,"to":n})
    for p in common:
        ops += required_ops(a[p],b[p],p)
        r = range_enum_ops(a[p],b[p],p)
        if r: ops += r
        t = type_change(a[p],b[p],p)
        if t: ops.append(t)
    return ops

# ----------main----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--py-file", required=True)
    ap.add_argument("--prefix", default="reviews")
    ap.add_argument("--versions", default="S0,S1,S2,S3,S4,S5,S6,S7,S8")
    args = ap.parse_args()

    versions=[v.strip() for v in args.versions.split(",")]
    mod=load_module(args.py_file)
    schemas=get_schemas(mod,versions)

    # 1) 输出每个版本的 schema json
    for v in versions:
        fn=f"schema_{args.prefix}_{v}.json"
        with open(fn,"w",encoding="utf-8") as f:
            json.dump(schemas[v], f, ensure_ascii=False, indent=2)
        print("Wrote", fn)

    # 2) 输出相邻版本的演化操作 json
    for i in range(len(versions)-1):
        vA,vB=versions[i],versions[i+1]
        ops=diff_schemas(schemas[vA], schemas[vB])
        fn=f"operations_{args.prefix}_{vA}_to_{args.prefix}_{vB}.json"
        with open(fn,"w",encoding="utf-8") as f:
            json.dump(ops, f, ensure_ascii=False, indent=2)
        print("Wrote", fn)

    print("[DONE] all schemas and diffs exported.")

if __name__ == "__main__":
    main()
# python batch_schema_diff.py --py-file load_yelp_case.py --prefix reviews --versions S0,S1,S2,S3,S4,S5,S6,S7,S8