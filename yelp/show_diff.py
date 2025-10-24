#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
schema_diff.py
对两份 MongoDB $jsonSchema 做结构/类型/约束的树形 diff，
输出：
  - operations_<A>_to_<B>.json  （模式演化操作序列，使用你的最终版操作命名）
  - diff_tree_<A>_to_<B>.mmd    （Mermaid 树图，新增=绿、删除=红、修改=黄）
  - tree_<A>.mmd / tree_<B>.mmd （各自版本的树）
"""

import json, sys, os, copy
from collections import defaultdict

# --------------------- 解析 & 树展开 ---------------------

def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def norm_type(t):
    if isinstance(t, list): return tuple(sorted(t))
    return t

def node_signature(node):
    """用于相似度匹配的签名：类型、min/max、enum、必填子字段名集合等"""
    sig = {
        "bsonType": norm_type(node.get("bsonType")),
        "minimum": node.get("minimum"),
        "maximum": node.get("maximum"),
        "enum_sz": len(node.get("enum", [])) if node.get("enum") else 0,
        "has_items": "items" in node,
    }
    if "items" in node and isinstance(node["items"], dict):
        sig["itemsType"] = norm_type(node["items"].get("bsonType"))
    # 子属性名（不含类型）用于衡量对象结构相近性
    if node.get("bsonType") == "object":
        props = node.get("properties", {})
        sig["child_keys"] = tuple(sorted(props.keys()))
        req = node.get("required", [])
        sig["required_set"] = tuple(sorted(req))
    return sig

def jaccard(a, b):
    a = set(a); b = set(b)
    if not a and not b: return 1.0
    return len(a & b) / max(1, len(a | b))

def sim(sig1, sig2):
    """粗略相似度：类型一致加分；子字段集合 jaccard；数值约束接近；"""
    s = 0.0
    if sig1.get("bsonType") == sig2.get("bsonType"): s += 0.4
    if "child_keys" in sig1 and "child_keys" in sig2:
        s += 0.3 * jaccard(sig1["child_keys"], sig2["child_keys"])
    if sig1.get("enum_sz") == sig2.get("enum_sz"): s += 0.1
    if sig1.get("has_items") == sig2.get("has_items"): s += 0.1
    if sig1.get("itemsType") == sig2.get("itemsType"): s += 0.1
    return s

def walk(schema, root_name):
    """把 JSON Schema 展开成 path->node 的字典；array 用 path[] 表示"""
    idx = {}
    def _walk(prefix, node):
        idx[prefix] = node
        bt = node.get("bsonType")
        if bt == "object":
            props = node.get("properties", {})
            for k, v in props.items():
                _walk(f"{prefix}.{k}", v)
        elif bt == "array":
            items = node.get("items", {})
            if isinstance(items, dict):
                _walk(f"{prefix}[]", items)
    _walk(root_name, schema)
    return idx

# --------------------- Diff 核心 ---------------------

def compute_required_ops(old_node, new_node, path):
    ops = []
    if not (old_node and new_node): return ops
    old_req = set(old_node.get("required", []))
    new_req = set(new_node.get("required", []))
    for r in sorted(new_req - old_req):
        ops.append({"op":"AddRequired","path": f"{path}.{r}"})
    for r in sorted(old_req - new_req):
        ops.append({"op":"DropRequired","path": f"{path}.{r}"})
    return ops

def constraint_ops(old_node, new_node, path):
    ops = []
    # range
    omin, omax = old_node.get("minimum"), old_node.get("maximum")
    nmin, nmax = new_node.get("minimum"), new_node.get("maximum")
    if omin is None and omax is None:
        if nmin is not None or nmax is not None:
            ops.append({"op":"AddRange","path":path,"spec":{"minimum":nmin,"maximum":nmax}})
    else:
        if nmin != omin or nmax != omax:
            ops.append({"op":"ModifyRange","path":path,"from":{"minimum":omin,"maximum":omax},"to":{"minimum":nmin,"maximum":nmax}})
    # enum
    oenum = tuple(old_node.get("enum", []) or [])
    nenum = tuple(new_node.get("enum", []) or [])
    if oenum and not nenum:
        ops.append({"op":"DropEnum","path":path})
    elif not oenum and nenum:
        ops.append({"op":"AddEnum","path":path,"values":list(nenum)})
    elif oenum and nenum and oenum != nenum:
        # 简化为 ModifyEnum；也可细分 add/remove
        ops.append({"op":"ModifyEnum","path":path,"from_sz":len(oenum),"to_sz":len(nenum)})
    return ops

def type_change_op(old_node, new_node, path):
    ot = norm_type(old_node.get("bsonType"))
    nt = norm_type(new_node.get("bsonType"))
    if ot != nt:
        # ToArray / ToScalar 的特殊识别
        if (ot == "array" and nt in ("string","object","double","int","long")):
            return {"op":"ToScalar","path":path}
        if (nt == "array" and ot in ("string","object","double","int","long")):
            return {"op":"ToArray","path":path}
        return {"op":"ChangeType","path":path,"from":ot,"to":nt}

def detect_moves_and_renames(removed, added, old_idx, new_idx):
    """基于签名相似度 + 父节点关系，推断 Move/Rename"""
    renames = []
    moves = []
    pairs = []  # (old_path, new_path, score)
    sig_old = {p:node_signature(old_idx[p]) for p in removed}
    sig_new = {p:node_signature(new_idx[p]) for p in added}
    for po in removed:
        for pn in added:
            s = sim(sig_old[po], sig_new[pn])
            # 同父不同名 ⇒ 候选重命名；不同父同名 ⇒ 候选移动；都不同 ⇒ move+rename 候选
            if s >= 0.6:
                pairs.append((po, pn, s))
    pairs.sort(key=lambda x: x[2], reverse=True)
    used_old, used_new = set(), set()
    for po, pn, s in pairs:
        if po in used_old or pn in used_new:
            continue
        old_parent = ".".join(po.split(".")[:-1])
        new_parent = ".".join(pn.split(".")[:-1])
        old_name = po.split(".")[-1]
        new_name = pn.split(".")[-1]
        if old_parent == new_parent and old_name != new_name:
            renames.append((po, pn))
        elif old_parent != new_parent and old_name == new_name:
            moves.append((po, pn))
        else:
            # 综合：既换父也改名，记作 Move+Rename
            moves.append((po, pn))
            renames.append((po, pn))
        used_old.add(po); used_new.add(pn)
    return renames, moves, used_old, used_new

def diff(old_schema_path, new_schema_path):
    baseA = os.path.basename(old_schema_path).replace("_schema.json","")
    baseB = os.path.basename(new_schema_path).replace("_schema.json","")
    A = load_schema(old_schema_path)
    B = load_schema(new_schema_path)
    idxA = walk(A, baseA)
    idxB = walk(B, baseB)

    pathsA = set(idxA.keys())
    pathsB = set(idxB.keys())

    # 纯集合差
    removed = sorted(p for p in (pathsA - pathsB) if not p.endswith("[]"))
    added   = sorted(p for p in (pathsB - pathsA) if not p.endswith("[]"))
    common  = sorted(pathsA & pathsB)

    ops = []

    # rename / move 识别（在“字段删除/新增”之间匹配）
    renames, moves, used_old, used_new = detect_moves_and_renames(removed, added, idxA, idxB)

    # 剔除已被识别为 move/rename 的出入
    removed_eff = [p for p in removed if p not in used_old]
    added_eff   = [p for p in added   if p not in used_new]

    # 生成 Drop / Add
    for p in removed_eff:
        ops.append({"op":"DropField","path":p})
    for p in added_eff:
        node = idxB[p]
        ops.append({"op":"AddField","path":p, "dtype":node.get("bsonType")})

    # 生成 Rename / Move
    for po, pn in renames:
        ops.append({"op":"RenameField","from":po, "to":pn})
    for po, pn in moves:
        if po != pn:
            ops.append({"op":"MoveField","from":po, "to":pn})

    # 公共路径上检查类型 & 约束变化；并检查 object 的 required 差异
    for p in common:
        a = idxA[p]; b = idxB[p]
        if a.get("bsonType") and b.get("bsonType"):
            tchg = type_change_op(a, b, p)
            if tchg: ops.append(tchg)
        ops += constraint_ops(a, b, p)
        # object 的 required 列表 diff
        if a.get("bsonType") == "object" and b.get("bsonType") == "object":
            ops += compute_required_ops(a, b, p)

    return ops, idxA, idxB, baseA, baseB

# --------------------- Mermaid 树渲染 ---------------------

def mmd_tree(idx, title):
    """把一个 schema 索引渲染为树（Mermaid flowchart TD）"""
    lines = ["flowchart TD", f'classDef added fill:#e6ffed,stroke:#2ecc71,stroke-width:1px;'
                             f'classDef removed fill:#ffecec,stroke:#e74c3c,stroke-width:1px;'
                             f'classDef changed fill:#fffbe6,stroke:#f1c40f,stroke-width:1px;']
    # 建树关系
    nodes = set(idx.keys())
    for p in sorted(nodes):
        nid = p.replace(".","_").replace("[]","Arr")
        label = p.split(".")[-1]
        bt = idx[p].get("bsonType")
        lines.append(f'{nid}["{label}\\n({bt})"]')
        if "." in p:
            parent = ".".join(p.split(".")[:-1])
            pid = parent.replace(".","_").replace("[]","Arr")
            lines.append(f"{pid} --> {nid}")
    lines.insert(1, f"%% {title}")
    return "\n".join(lines)

def mmd_diff_tree(idxA, idxB, baseA, baseB, ops):
    """根据 ops 给节点上色：Add=绿、Drop=红、Change=黄"""
    # 标记
    add_nodes = set()
    drop_nodes = set()
    chg_nodes = set()
    for op in ops:
        if op["op"] == "AddField":
            add_nodes.add(op["path"])
        elif op["op"] == "DropField":
            drop_nodes.add(op["path"])
        elif op["op"] in ("RenameField","MoveField","ChangeType","ToArray","ToScalar",
                          "AddRange","ModifyRange","DropRange",
                          "AddEnum","ModifyEnum","DropEnum",
                          "AddRequired","DropRequired","AddItemsConstraint","ModifyItemsConstraint","DropItemsConstraint"):
            # 统一当作 changed
            for k in ("path","from","to"):
                if k in op and isinstance(op[k], str):
                    chg_nodes.add(op[k])

    # 合并两版的节点用于画一棵“对齐树”
    all_nodes = set(idxA.keys()) | set(idxB.keys())
    lines = [
        "flowchart TD",
        f'%% diff {baseA} -> {baseB}',
        'classDef added fill:#e6ffed,stroke:#2ecc71;'
        'classDef removed fill:#ffecec,stroke:#e74c3c;'
        'classDef changed fill:#fffbe6,stroke:#f1c40f;',
    ]
    for p in sorted(all_nodes):
        nid = p.replace(".","_").replace("[]","Arr")
        label = p.split(".")[-1]
        btA = idxA.get(p, {}).get("bsonType")
        btB = idxB.get(p, {}).get("bsonType")
        bt = btB or btA
        lines.append(f'{nid}["{label}\\n({bt})"]')
        if "." in p:
            parent = ".".join(p.split(".")[:-1])
            pid = parent.replace(".","_").replace("[]","Arr")
            lines.append(f"{pid} --> {nid}")

    # 上色
    for p in add_nodes:
        nid = p.replace(".","_").replace("[]","Arr")
        lines.append(f"class {nid} added;")
    for p in drop_nodes:
        nid = p.replace(".","_").replace("[]","Arr")
        lines.append(f"class {nid} removed;")
    for p in chg_nodes:
        nid = p.replace(".","_").replace("[]","Arr")
        lines.append(f"class {nid} changed;")
    return "\n".join(lines)

# --------------------- 主流程 ---------------------

def main():
    if len(sys.argv) != 3:
        print("Usage: python schema_diff.py <old_schema.json> <new_schema.json>")
        sys.exit(1)
    oldp, newp = sys.argv[1], sys.argv[2]
    ops, idxA, idxB, baseA, baseB = diff(oldp, newp)

    # 写操作序列
    ops_path = f"operations_{baseA}_to_{baseB}.json"
    with open(ops_path, "w", encoding="utf-8") as f:
        json.dump(ops, f, ensure_ascii=False, indent=2)
    print("Wrote", ops_path)

    # 单版树
    with open(f"tree_{baseA}.mmd","w",encoding="utf-8") as f:
        f.write(mmd_tree(idxA, baseA))
    with open(f"tree_{baseB}.mmd","w",encoding="utf-8") as f:
        f.write(mmd_tree(idxB, baseB))
    print("Wrote", f"tree_{baseA}.mmd", f"tree_{baseB}.mmd")

    # diff 树
    with open(f"diff_tree_{baseA}_to_{baseB}.mmd","w",encoding="utf-8") as f:
        f.write(mmd_diff_tree(idxA, idxB, baseA, baseB, ops))
    print("Wrote", f"diff_tree_{baseA}_to_{baseB}.mmd")

if __name__ == "__main__":
    main()
