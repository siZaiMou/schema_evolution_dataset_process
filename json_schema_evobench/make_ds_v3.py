import os
import json
import random
from datasets import load_dataset, get_dataset_config_names
from faker import Faker
from copy import deepcopy

fake = Faker()

# ---------- 配置 ----------
DATASET_NAME = "epfl-dlab/JSONSchemaBench"
OUTPUT_DIR = "./evolved_dataset"
NUM_VERSIONS = 10  # 10 个版本
NUM_DOCS_PER_VERSION = 5  # 每个版本 5 个文档
MIN_FIELDS = 5  # 降低阈值，适应 Github_easy
MAX_SCHEMAS_PER_SUBSET = 5  # 每个子集最多 5 个 schema

# 动态获取可用子集
try:
    SUBSETS = get_dataset_config_names(DATASET_NAME)
    print(f"可用子集: {SUBSETS}")
except Exception as e:
    print(f"获取子集失败: {e}")
    SUBSETS = ["Github_easy"]  # 回退到 Github_easy

# 禁用 Hugging Face 符号链接警告
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "true"

# ---------- 工具函数 ----------
def count_fields(schema, parent_key=""):
    """递归计算 schema 中所有字段数（包括嵌套字段）"""
    count = 0
    props = schema.get("properties", {})
    count += len(props)
    for key, value in props.items():
        if value.get("type") == "object":
            count += count_fields(value, f"{parent_key}.{key}" if parent_key else key)
        elif value.get("type") == "array" and value.get("items", {}).get("type") == "object":
            count += count_fields(value["items"], f"{parent_key}.{key}[]" if parent_key else f"{key}[]")
    return count

def get_array_fields(schema):
    """获取 schema 中类型为 array 的字段"""
    return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "array"]

def get_object_fields(schema):
    """获取 schema 中类型为 object 的字段"""
    return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "object"]

def generate_example(schema):
    """根据 JSON Schema 生成随机 JSON 文档"""
    example = {}
    required = schema.get("required", [])
    for prop, definition in schema.get("properties", {}).items():
        typ = definition.get("type", "string")
        if typ == "string":
            value = fake.word()
        elif typ == "integer":
            value = fake.random_int(min=0, max=100)
        elif typ == "number":
            value = fake.random_number(digits=5) / 100.0
        elif typ == "boolean":
            value = fake.boolean()
        elif typ == "object":
            value = generate_example(definition)
        elif typ == "array":
            items_def = definition.get("items", {"type": "string"})
            value = [generate_example(items_def) for _ in range(random.randint(1, 3))]
        else:
            value = None
        if value is not None or prop in required:
            example[prop] = value if value is not None else fake.word()
    return example

def evolve_schema(schema, version_num):
    """生成演化版本 Schema，基于数据集字段动态生成变更"""
    new_schema = deepcopy(schema)
    props = new_schema.get("properties", {})
    required = new_schema.get("required", [])

    change_types = [
        "add_field", "remove_field", "rename_field", "type_change",
        "nest_field", "unnest_field", "split_array", "merge_objects"
    ]
    change_type = change_types[(version_num - 1) % len(change_types)]

    change_desc = "无变更"

    if change_type == "add_field":
        existing_fields = list(props.keys())
        new_field = f"{random.choice(existing_fields or ['field'])}_{version_num}" if existing_fields else f"field_{version_num}"
        field_type = random.choice(["string", "integer", "boolean", "number"])
        props[new_field] = {"type": field_type, "description": f"Added field {new_field}"}
        if random.random() > 0.5:
            required.append(new_field)
            new_schema["required"] = required
        change_desc = f"新增字段 {new_field} (类型: {field_type}, 必填: {new_field in required})"

    elif change_type == "remove_field" and props:
        non_required = [k for k in props.keys() if k not in required]
        remove_field = random.choice(non_required or list(props.keys()))
        props.pop(remove_field)
        if remove_field in required:
            required.remove(remove_field)
        change_desc = f"删除字段 {remove_field}"

    elif change_type == "rename_field" and props:
        rename_field = random.choice(list(props.keys()))
        new_name = f"{rename_field}_renamed_{version_num}"
        props[new_name] = props.pop(rename_field)
        if rename_field in required:
            required.remove(rename_field)
            required.append(new_name)
        change_desc = f"重命名字段 {rename_field} → {new_name}"

    elif change_type == "type_change" and props:
        field = random.choice(list(props.keys()))
        old_type = props[field].get("type", "string")
        compatible_types = [t for t in ["string", "integer", "boolean", "number"] if t != old_type]
        if old_type in ["string", "integer", "number"] and random.random() > 0.5:
            props[field] = {
                "type": "object",
                "properties": {
                    "value": {"type": old_type},
                    "unit": {"type": "string"}
                },
                "required": ["value", "unit"],
                "description": f"{field} as object"
            }
            change_desc = f"字段类型变更 {field}: {old_type} → object {{value: {old_type}, unit: string}}"
        elif compatible_types:
            new_type = random.choice(compatible_types)
            props[field]["type"] = new_type
            change_desc = f"字段类型变更 {field}: {old_type} → {new_type}"
        else:
            change_desc = f"字段类型变更 {field}: 无兼容类型，保持不变"

    elif change_type == "nest_field" and len(props) >= 2:
        keys = random.sample(list(props.keys()), 2)
        new_key = f"{keys[0]}_nested_{version_num}"
        nested_obj = {k: props.pop(k) for k in keys}
        props[new_key] = {"type": "object", "properties": nested_obj}
        for k in keys:
            if k in required:
                required.remove(k)
        if any(k in required for k in keys):
            required.append(new_key)
        change_desc = f"嵌套调整字段 {keys} → {new_key}"

    elif change_type == "unnest_field" and any(v.get("type") == "object" for v in props.values()):
        object_fields = get_object_fields(new_schema)
        unnest_field = random.choice(object_fields)
        nested_props = props[unnest_field].get("properties", {})
        for nested_k, nested_v in nested_props.items():
            props[f"{unnest_field}_{nested_k}"] = nested_v
        props.pop(unnest_field)
        if unnest_field in required:
            required.remove(unnest_field)
            for nested_k in nested_props:
                if random.random() > 0.5:
                    required.append(f"{unnest_field}_{nested_k}")
        change_desc = f"解嵌套字段 {unnest_field}"

    elif change_type == "split_array" and any(v.get("type") == "array" for v in props.values()):
        array_fields = get_array_fields(new_schema)
        split_field = random.choice(array_fields)
        props[f"{split_field}_part1"] = {
            "type": "array",
            "items": props[split_field].get("items", {"type": "string"}),
            "description": f"Part 1 of {split_field}"
        }
        props[f"{split_field}_part2"] = {
            "type": "array",
            "items": props[split_field].get("items", {"type": "string"}),
            "description": f"Part 2 of {split_field}"
        }
        props.pop(split_field)
        if split_field in required:
            required.remove(split_field)
            required.extend([f"{split_field}_part1", f"{split_field}_part2"])
        change_desc = f"拆分数组 {split_field} → {split_field}_part1 和 {split_field}_part2"

    elif change_type == "merge_objects" and len(get_object_fields(new_schema)) >= 2:
        object_fields = get_object_fields(new_schema)
        merge_fields = random.sample(object_fields, 2)
        merged_props = {}
        for f in merge_fields:
            merged_props.update(props[f].get("properties", {}))
            props.pop(f)
            if f in required:
                required.remove(f)
        new_key = f"merged_{merge_fields[0]}_{version_num}"
        props[new_key] = {"type": "object", "properties": merged_props}
        required.append(new_key)
        change_desc = f"合并对象 {merge_fields} → {new_key}"

    new_schema["properties"] = props
    new_schema["required"] = required
    return new_schema, change_desc

# ---------- 生成数据集 ----------
processed_schemas = []

for subset in SUBSETS:
    try:
        ds = load_dataset(DATASET_NAME, name=subset)
        train_schemas = ds["train"]
    except Exception as e:
        print(f"加载子集 {subset} 失败: {e}")
        continue

    print(f"子集 {subset} features: {train_schemas.features}")
    print(f"子集 {subset} First 2 examples: {[train_schemas[i] for i in range(min(2, len(train_schemas)))]}")

    train_schemas_subset = [train_schemas[i] for i in range(len(train_schemas))]
    subset_processed_count = 0

    for idx, example in enumerate(train_schemas_subset):
        if subset_processed_count >= MAX_SCHEMAS_PER_SUBSET:
            break

        if not isinstance(example, dict):
            print(f"子集 {subset} 跳过示例 {idx}: 预期为字典，实际为 {type(example)}: {example}")
            continue

        schema_str = example.get("json_schema")
        unique_id = example.get("unique_id")
        if not schema_str or not unique_id:
            print(f"子集 {subset} 跳过示例 {idx}: 未找到 json_schema 或 unique_id")
            continue

        try:
            schema = json.loads(schema_str)
        except json.JSONDecodeError as e:
            print(f"子集 {subset} 跳过示例 {idx}: 无效的 JSON Schema - {e}")
            continue

        total_fields = count_fields(schema)
        if total_fields < MIN_FIELDS:
            print(f"子集 {subset} 跳过 {unique_id}: 字段数太少 ({total_fields})")
            continue

        subset_processed_count += 1
        processed_schemas.append((subset, unique_id, schema))

for subset, unique_id, schema in processed_schemas:
    entity_dir = os.path.join(OUTPUT_DIR, subset, unique_id)
    os.makedirs(entity_dir, exist_ok=True)

    log = []

    for v in range(1, NUM_VERSIONS + 1):
        evolved_schema, desc = evolve_schema(schema, v)
        schema_dir = os.path.join(entity_dir, f"v{v}")
        os.makedirs(schema_dir, exist_ok=True)

        with open(os.path.join(schema_dir, "schema.json"), "w", encoding="utf-8") as f:
            json.dump(evolved_schema, f, indent=2, ensure_ascii=False)

        for doc_id in range(1, NUM_DOCS_PER_VERSION + 1):
            data = generate_example(evolved_schema)
            with open(os.path.join(schema_dir, f"{doc_id}.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        log.append(f"v{v}: {desc}")

        schema = evolved_schema

    with open(os.path.join(entity_dir, "change_log.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(log))

print(f"生成完成！总计处理了 {len(processed_schemas)} 个 schema")