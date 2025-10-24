import os
import json
import random
from datasets import load_dataset
from faker import Faker
from copy import deepcopy

fake = Faker()

# ---------- 配置 ----------
DATASET_NAME = "epfl-dlab/JSONSchemaBench"
SUBSET_NAME = "Github_easy"  # 明确指定子集
OUTPUT_DIR = "./evolved_dataset"
NUM_VERSIONS = 5  # 每个初始 schema 生成多少个演化版本
NUM_DOCS_PER_VERSION = 5  # 每个版本生成多少 JSON 文档

# 禁用 Hugging Face 符号链接警告
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "true"

# ---------- 加载数据集 ----------
try:
    ds = load_dataset(DATASET_NAME, name=SUBSET_NAME)
    train_schemas = ds["train"]
except Exception as e:
    print(f"加载数据集失败: {e}")
    exit(1)

# 调试：检查数据集结构和前几行数据
print(f"Dataset features: {train_schemas.features}")
print(f"First 2 examples: {[train_schemas[i] for i in range(min(2, len(train_schemas)))]}")

# ---------- 工具函数 ----------
def generate_example(schema):
    """根据 JSON Schema 生成随机 JSON 文档"""
    example = {}
    for prop, definition in schema.get("properties", {}).items():
        typ = definition.get("type", "string")
        if typ == "string":
            example[prop] = fake.word()
        elif typ == "integer":
            example[prop] = fake.random_int(min=0, max=100)
        elif typ == "number":
            example[prop] = fake.random_number(digits=5) / 100.0
        elif typ == "boolean":
            example[prop] = fake.boolean()
        elif typ == "object":
            example[prop] = generate_example(definition)
        elif typ == "array":
            items_def = definition.get("items", {"type": "string"})
            example[prop] = [generate_example(items_def)]
    return example


def evolve_schema(schema, version_num):
    """生成演化版本 Schema，示例包含基础字段和嵌套调整"""
    new_schema = deepcopy(schema)
    props = new_schema.get("properties", {})

    # 随机选择变更类型
    change_type = random.choice(["add_field", "remove_field", "rename_field", "type_change", "nest_field"])

    if change_type == "add_field":
        new_field = f"new_field_v{version_num}"
        props[new_field] = {"type": random.choice(["string", "integer", "boolean"])}
        change_desc = f"新增字段 {new_field}"

    elif change_type == "remove_field" and props:
        remove_field = random.choice(list(props.keys()))
        props.pop(remove_field)
        change_desc = f"删除字段 {remove_field}"

    elif change_type == "rename_field" and props:
        rename_field = random.choice(list(props.keys()))
        new_name = rename_field + f"_v{version_num}"
        props[new_name] = props.pop(rename_field)
        change_desc = f"重命名字段 {rename_field} → {new_name}"

    elif change_type == "type_change" and props:
        field = random.choice(list(props.keys()))
        old_type = props[field].get("type", "string")
        new_type = random.choice([t for t in ["string", "integer", "boolean", "number"] if t != old_type])
        props[field]["type"] = new_type
        change_desc = f"字段类型变更 {field}: {old_type} → {new_type}"

    elif change_type == "nest_field" and len(props) >= 2:
        # 简单嵌套：将两个字段合并为一个嵌套对象
        keys = random.sample(list(props.keys()), 2)
        nested_obj = {k: props.pop(k) for k in keys}
        new_key = f"nested_v{version_num}"
        props[new_key] = {"type": "object", "properties": nested_obj}
        change_desc = f"嵌套调整字段 {keys} → {new_key}"

    else:
        change_desc = "无变更"

    new_schema["properties"] = props
    return new_schema, change_desc


# ---------- 生成数据集 ----------
# 强制转换为字典列表，避免迭代器问题
train_schemas_subset = [train_schemas[i] for i in range(min(10, len(train_schemas)))]
for idx, example in enumerate(train_schemas_subset):
    entity_dir = os.path.join(OUTPUT_DIR, f"entity_{idx}")
    os.makedirs(entity_dir, exist_ok=True)

    # 检查 example 是否为字典
    if not isinstance(example, dict):
        print(f"跳过示例 {idx}: 预期为字典，实际为 {type(example)}: {example}")
        continue

    # 获取 json_schema 字段
    schema_str = example.get("json_schema")
    if not schema_str:
        print(f"跳过示例 {idx}: 未找到 json_schema 字段")
        continue

    # 解析 JSON Schema 字符串
    try:
        schema = json.loads(schema_str)
    except json.JSONDecodeError as e:
        print(f"跳过示例 {idx}: 无效的 JSON Schema - {e}")
        continue

    log = []

    for v in range(1, NUM_VERSIONS + 1):
        # 演化 Schema
        evolved_schema, desc = evolve_schema(schema, v)
        schema_dir = os.path.join(entity_dir, f"v{v}")
        os.makedirs(schema_dir, exist_ok=True)

        # 保存 Schema
        with open(os.path.join(schema_dir, "schema.json"), "w", encoding="utf-8") as f:
            json.dump(evolved_schema, f, indent=2, ensure_ascii=False)

        # 生成 JSON 文档
        for doc_id in range(1, NUM_DOCS_PER_VERSION + 1):
            data = generate_example(evolved_schema)
            with open(os.path.join(schema_dir, f"{doc_id}.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        # 记录变更日志
        log.append(f"v{v}: {desc}")

        # 为下一版本使用当前版本 Schema
        schema = evolved_schema

    # 保存日志
    with open(os.path.join(entity_dir, "change_log.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(log))

print("生成完成！")