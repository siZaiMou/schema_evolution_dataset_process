import os
import json
import random
from copy import deepcopy
from faker import Faker
from datasets import load_dataset, get_dataset_config_names

fake = Faker()


class SchemaEvolver:
    def __init__(self, output_dir="./evolved_dataset", num_versions=10, num_docs_per_version=5):
        self.output_dir = output_dir
        self.num_versions = num_versions
        self.num_docs_per_version = num_docs_per_version
        os.makedirs(self.output_dir, exist_ok=True)

    # ---------- 工具函数 ----------
    @staticmethod
    def count_fields(schema):
        count = 0
        props = schema.get("properties", {})
        count += len(props)
        for key, value in props.items():
            if value.get("type") == "object":
                count += SchemaEvolver.count_fields(value)
            elif value.get("type") == "array" and value.get("items", {}).get("type") == "object":
                count += SchemaEvolver.count_fields(value["items"])
        return count

    @staticmethod
    def get_array_fields(schema):
        return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "array"]

    @staticmethod
    def get_object_fields(schema):
        return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "object"]

    @staticmethod
    def generate_example(schema):
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
                value = SchemaEvolver.generate_example(definition)
            elif typ == "array":
                items_def = definition.get("items", {"type": "string"})
                value = [SchemaEvolver.generate_example(items_def) for _ in range(random.randint(1, 3))]
            else:
                value = None
            if value is not None or prop in required:
                example[prop] = value if value is not None else fake.word()
        return example

    @staticmethod
    def evolve_schema(schema, version_num):
        """保留原始演化逻辑"""
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
            new_field = f"{random.choice(existing_fields or ['field'])}_{version_num}"
            field_type = random.choice(["string", "integer", "boolean", "number"])
            props[new_field] = {"type": field_type}
            if random.random() > 0.5:
                required.append(new_field)
            change_desc = f"新增字段 {new_field} (类型: {field_type})"

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

        # 可继续保留其他演化策略...

        new_schema["properties"] = props
        new_schema["required"] = required
        return new_schema, change_desc

    # ---------- 核心处理函数 ----------
    def process_hf_dataset(self, dataset_name="epfl-dlab/JSONSchemaBench", max_schemas_per_subset=5, min_fields=5):
        os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "true"
        try:
            subsets = get_dataset_config_names(dataset_name)
            print(f"可用子集: {subsets}")
        except Exception as e:
            print(f"获取子集失败: {e}")
            subsets = ["default"]

        processed_schemas = []
        for subset in subsets:
            try:
                ds = load_dataset(dataset_name, name=subset)
                train_schemas = ds["train"]
            except Exception as e:
                print(f"加载子集 {subset} 失败: {e}")
                continue

            subset_processed_count = 0
            for idx, example in enumerate(train_schemas):
                if subset_processed_count >= max_schemas_per_subset:
                    break

                schema_str = example.get("json_schema")
                unique_id = example.get("unique_id")
                if not schema_str or not unique_id:
                    continue
                try:
                    schema = json.loads(schema_str)
                except json.JSONDecodeError:
                    continue

                if self.count_fields(schema) < min_fields:
                    continue

                subset_processed_count += 1
                processed_schemas.append((subset, unique_id, schema))

        self._generate_dataset(processed_schemas)
        return processed_schemas

    def process_local_file(self, file_path, unique_id=None):
        """处理单个本地 JSON Schema 文件"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        uid = unique_id or os.path.splitext(os.path.basename(file_path))[0]
        processed_schemas = [("local", uid, schema)]
        self._generate_dataset(processed_schemas)
        return processed_schemas

    def process_schema_source(self, source, **kwargs):
        """
        统一接口：
        - source=str: 本地文件路径
        - source=list/tuple/dict/其他: 可扩展为 HF 数据集名称
        """
        if isinstance(source, str) and os.path.isfile(source):
            return self.process_local_file(source, **kwargs)
        else:
            # 默认按 HF 数据集处理
            return self.process_hf_dataset(source, **kwargs)

    # ---------- 内部生成函数 ----------
    def _generate_dataset(self, processed_schemas):
        for subset, unique_id, schema in processed_schemas:
            entity_dir = os.path.join(self.output_dir, subset, unique_id)
            os.makedirs(entity_dir, exist_ok=True)

            log = []
            for v in range(1, self.num_versions + 1):
                evolved_schema, desc = self.evolve_schema(schema, v)
                schema_dir = os.path.join(entity_dir, f"v{v}")
                os.makedirs(schema_dir, exist_ok=True)

                with open(os.path.join(schema_dir, "schema.json"), "w", encoding="utf-8") as f:
                    json.dump(evolved_schema, f, indent=2, ensure_ascii=False)

                for doc_id in range(1, self.num_docs_per_version + 1):
                    data = self.generate_example(evolved_schema)
                    with open(os.path.join(schema_dir, f"{doc_id}.json"), "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)

                log.append(f"v{v}: {desc}")
                schema = evolved_schema

            with open(os.path.join(entity_dir, "change_log.txt"), "w", encoding="utf-8") as f:
                f.write("\n".join(log))

        print(f"生成完成！总计处理了 {len(processed_schemas)} 个 schema")


# ---------- 示例用法 ----------
if __name__ == "__main__":
    evolver = SchemaEvolver(output_dir="./evolved_dataset", num_versions=5, num_docs_per_version=3)

    # 1. 处理 HF 数据集
    evolver.process_schema_source("epfl-dlab/JSONSchemaBench", max_schemas_per_subset=3)

    # 2. 处理本地 JSON Schema 文件
    # evolver.process_schema_source("./my_schema.json", unique_id="example_schema")
