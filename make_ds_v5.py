import os
import json
import random
from copy import deepcopy
from faker import Faker
from datasets import load_dataset, get_dataset_config_names
from typing import Dict, List, Tuple, Optional, Union

fake = Faker()


class SchemaUtils:
    """提供 JSON Schema 相关的工具函数"""

    @staticmethod
    def count_fields(schema: Dict) -> int:
        """递归计算 schema 中的字段总数"""
        count = 0
        props = schema.get("properties", {})
        count += len(props)
        for key, value in props.items():
            if value.get("type") == "object":
                count += SchemaUtils.count_fields(value)
            elif value.get("type") == "array" and value.get("items", {}).get("type") == "object":
                count += SchemaUtils.count_fields(value["items"])
        return count

    @staticmethod
    def get_array_fields(schema: Dict) -> List[str]:
        """获取 schema 中的数组字段名"""
        return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "array"]

    @staticmethod
    def get_object_fields(schema: Dict) -> List[str]:
        """获取 schema 中的对象字段名"""
        return [k for k, v in schema.get("properties", {}).items() if v.get("type") == "object"]

    @staticmethod
    def generate_example(schema: Dict) -> Dict:
        """根据 schema 生成示例数据"""
        example = {}
        required = schema.get("required", [])
        for prop, definition in schema.get("properties", {}).items():
            typ = definition.get("type", "string")
            value = None
            if typ == "string":
                value = fake.word()
            elif typ == "integer":
                value = fake.random_int(min=0, max=100)
            elif typ == "number":
                value = fake.random_number(digits=5) / 100.0
            elif typ == "boolean":
                value = fake.boolean()
            elif typ == "object":
                value = SchemaUtils.generate_example(definition)
            elif typ == "array":
                items_def = definition.get("items", {"type": "string"})
                value = [SchemaUtils.generate_example(items_def) for _ in range(random.randint(1, 3))]
            if value is not None or prop in required:
                example[prop] = value if value is not None else fake.word()
        return example


class SchemaEvolutionStrategy:
    """定义 schema 演化策略"""

    @staticmethod
    def add_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """添加新字段"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])
        existing_fields = list(props.keys())
        new_field = f"{random.choice(existing_fields or ['field'])}_{version_num}"
        field_type = random.choice(["string", "integer", "boolean", "number"])
        props[new_field] = {"type": field_type}
        if random.random() > 0.5:
            required.append(new_field)
        new_schema["properties"] = props
        new_schema["required"] = required
        return new_schema, f"新增字段 {new_field} (类型: {field_type})"

    @staticmethod
    def remove_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """删除字段（优先选择非必需字段）"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])
        if not props:
            return new_schema, "无变更（无字段可删除）"
        non_required = [k for k in props.keys() if k not in required]
        remove_field = random.choice(non_required or list(props.keys()))
        props.pop(remove_field)
        if remove_field in required:
            required.remove(remove_field)
        new_schema["properties"] = props
        new_schema["required"] = required
        return new_schema, f"删除字段 {remove_field}"

    @staticmethod
    def rename_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """重命名字段"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])
        if not props:
            return new_schema, "无变更（无字段可重命名）"
        rename_field = random.choice(list(props.keys()))
        new_name = f"{rename_field}_renamed_{version_num}"
        props[new_name] = props.pop(rename_field)
        if rename_field in required:
            required.remove(rename_field)
            required.append(new_name)
        new_schema["properties"] = props
        new_schema["required"] = required
        return new_schema, f"重命名字段 {rename_field} → {new_name}"

    @staticmethod
    def evolve_schema(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """根据版本号选择演化策略"""
        strategies = [
            SchemaEvolutionStrategy.add_field,
            SchemaEvolutionStrategy.remove_field,
            SchemaEvolutionStrategy.rename_field,
            # 可扩展其他策略：type_change, nest_field, unnest_field, split_array, merge_objects
        ]
        strategy = strategies[(version_num - 1) % len(strategies)]
        return strategy(schema, version_num)


class SchemaEvolver:
    """主类：管理 JSON Schema 的演化与数据生成"""

    def __init__(self, output_dir: str = "./evolved_dataset", num_versions: int = 10, num_docs_per_version: int = 5):
        """初始化演化器"""
        self.output_dir = output_dir
        self.num_versions = num_versions
        self.num_docs_per_version = num_docs_per_version
        os.makedirs(self.output_dir, exist_ok=True)

    def _save_json_file(self, data: Dict, file_path: str) -> None:
        """保存 JSON 数据到文件"""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _generate_version(self, schema: Dict, subset: str, unique_id: str) -> List[str]:
        """为单个 schema 生成多个版本和示例数据"""
        current_schema = schema
        log = []
        entity_dir = os.path.join(self.output_dir, subset, unique_id)
        os.makedirs(entity_dir, exist_ok=True)

        for v in range(1, self.num_versions + 1):
            # 演化 schema
            evolved_schema, desc = SchemaEvolutionStrategy.evolve_schema(current_schema, v)
            schema_dir = os.path.join(entity_dir, f"v{v}")
            os.makedirs(schema_dir, exist_ok=True)

            # 保存 schema
            self._save_json_file(evolved_schema, os.path.join(schema_dir, "schema.json"))

            # 生成并保存示例数据
            for doc_id in range(1, self.num_docs_per_version + 1):
                data = SchemaUtils.generate_example(evolved_schema)
                self._save_json_file(data, os.path.join(schema_dir, f"{doc_id}.json"))

            log.append(f"v{v}: {desc}")
            current_schema = evolved_schema

        # 保存变更日志
        with open(os.path.join(entity_dir, "change_log.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(log))
        return log

    def _process_hf_dataset(self, dataset_name: str, max_schemas_per_subset: int = 5, min_fields: int = 5) -> List[
        Tuple[str, str, Dict]]:
        """处理 Hugging Face 数据集"""
        os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "true"
        processed_schemas = []

        try:
            subsets = get_dataset_config_names(dataset_name)
            print(f"可用子集: {subsets}")
        except Exception as e:
            print(f"获取子集失败: {e}")
            subsets = ["default"]

        for subset in subsets:
            try:
                ds = load_dataset(dataset_name, name=subset)
                train_schemas = ds["train"]
            except Exception as e:
                print(f"加载子集 {subset} 失败: {e}")
                continue

            subset_processed_count = 0
            for example in train_schemas:
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

                if SchemaUtils.count_fields(schema) < min_fields:
                    continue

                subset_processed_count += 1
                processed_schemas.append((subset, unique_id, schema))

        return processed_schemas

    def _process_local_file(self, file_path: str, unique_id: Optional[str] = None) -> List[Tuple[str, str, Dict]]:
        """处理本地 JSON Schema 文件"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        uid = unique_id or os.path.splitext(os.path.basename(file_path))[0]
        return [("local", uid, schema)]

    def process_schema_source(self, source: Union[str, Dict], **kwargs) -> List[Tuple[str, str, Dict]]:
        """统一处理 schema 来源（本地文件或 HF 数据集）"""
        if isinstance(source, str) and os.path.isfile(source):
            processed_schemas = self._process_local_file(source, **kwargs)
        else:
            processed_schemas = self._process_hf_dataset(source, **kwargs)

        # 生成演化版本和数据
        for subset, unique_id, schema in processed_schemas:
            self._generate_version(schema, subset, unique_id)

        print(f"生成完成！总计处理了 {len(processed_schemas)} 个 schema")
        return processed_schemas


if __name__ == "__main__":
    evolver = SchemaEvolver(output_dir="./evolved_dataset", num_versions=5, num_docs_per_version=3)
    evolver.process_schema_source("epfl-dlab/JSONSchemaBench", max_schemas_per_subset=3)
    # evolver.process_schema_source("./my_schema.json", unique_id="example_schema")