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

            # 处理枚举类型
            if "enum" in definition:
                value = random.choice(definition["enum"])
            elif typ == "string":
                # 处理特定格式
                if "format" in definition:
                    if definition["format"] == "email":
                        value = fake.email()
                    elif definition["format"] == "date-time":
                        value = fake.iso8601()
                    elif definition["format"] == "uri":
                        value = fake.uri()
                    else:
                        value = fake.word()
                else:
                    value = fake.word()
            elif typ == "integer":
                min_val = definition.get("minimum", 0)
                max_val = definition.get("maximum", 100)
                value = fake.random_int(min=min_val, max=max_val)
            elif typ == "number":
                min_val = definition.get("minimum", 0)
                max_val = definition.get("maximum", 100)
                value = round(min_val + random.random() * (max_val - min_val), 2)
            elif typ == "boolean":
                value = fake.boolean()
            elif typ == "object":
                value = SchemaUtils.generate_example(definition)
            elif typ == "array":
                items_def = definition.get("items", {"type": "string"})
                min_items = definition.get("minItems", 1)
                max_items = definition.get("maxItems", 3)
                num_items = random.randint(min_items, max_items)
                value = [SchemaUtils.generate_example(items_def) for _ in range(num_items)]

            if value is not None or prop in required:
                example[prop] = value if value is not None else fake.word()

        return example


class EnhancedSchemaEvolutionStrategy:
    """增强的Schema演化策略，覆盖结构、约束和语义演化"""

    # 定义一个操作池，包含各种演化操作及其权重和类别
    EVOLUTION_OPERATIONS = [
        # 结构演化 - 字段级操作
        {"func": "add_field", "weight": 15, "category": "structural"},
        {"func": "remove_field", "weight": 10, "category": "structural"},
        {"func": "rename_field", "weight": 8, "category": "structural"},
        {"func": "change_field_type", "weight": 5, "category": "structural"},

        # 结构演化 - 嵌套结构操作
        {"func": "nest_fields", "weight": 5, "category": "structural"},
        {"func": "unnest_field", "weight": 5, "category": "structural"},
        {"func": "promote_field", "weight": 4, "category": "structural"},
        {"func": "demote_field", "weight": 4, "category": "structural"},
        {"func": "change_array_structure", "weight": 4, "category": "structural"},

        # 约束演化
        {"func": "change_required_constraint", "weight": 8, "category": "constraint"},
        {"func": "change_enum_options", "weight": 6, "category": "constraint"},
        {"func": "change_min_max_constraint", "weight": 7, "category": "constraint"},
        {"func": "change_pattern_constraint", "weight": 4, "category": "constraint"},

        # 语义演化
        {"func": "split_field", "weight": 3, "category": "semantic"},
        {"func": "merge_fields", "weight": 3, "category": "semantic"},
        {"func": "add_conditional_validation", "weight": 4, "category": "semantic"},
    ]

    # 记录已执行的操作类型，确保覆盖全面
    executed_operations = set()

    @staticmethod
    def get_available_operations(schema: Dict, version_num: int) -> List[dict]:
        """根据当前schema状态获取可用的演化操作"""
        available_ops = []

        for op in EnhancedSchemaEvolutionStrategy.EVOLUTION_OPERATIONS:
            # 检查操作是否可行（有足够的字段、合适的结构等）
            if EnhancedSchemaEvolutionStrategy._is_operation_viable(op["func"], schema):
                available_ops.append(op)

        # 优先选择尚未执行过的操作类型，确保覆盖全面
        for op in available_ops:
            if op["func"] not in EnhancedSchemaEvolutionStrategy.executed_operations:
                op["weight"] *= 2  # 提高未执行操作的权重

        return available_ops

    @staticmethod
    def _is_operation_viable(operation: str, schema: Dict) -> bool:
        """检查特定操作在当前schema下是否可行"""
        props = schema.get("properties", {})

        viability_checks = {
            "add_field": lambda: len(props) < 20,  # 防止字段过多
            "remove_field": lambda: len(props) > 3,  # 至少保留3个字段
            "rename_field": lambda: len(props) > 0,
            "change_field_type": lambda: any(
                field_def.get("type") in ["string", "number", "integer", "boolean"]
                for field_def in props.values()
            ),
            "nest_fields": lambda: len(props) >= 2,
            "unnest_field": lambda: any(
                field_def.get("type") == "object" and field_def.get("properties")
                for field_def in props.values()
            ),
            "promote_field": lambda: any(
                field_def.get("type") == "object" and field_def.get("properties")
                for field_def in props.values()
            ),
            "demote_field": lambda: len([
                k for k, v in props.items()
                if v.get("type") in ["string", "number", "integer", "boolean"]
            ]) >= 2,
            "change_required_constraint": lambda: "required" in schema and len(schema["required"]) > 0,
            "change_enum_options": lambda: any(
                "enum" in field_def for field_def in props.values()
            ),
            "change_min_max_constraint": lambda: any(
                field_def.get("type") in ["integer", "number"]
                for field_def in props.values()
            ),
            "change_pattern_constraint": lambda: any(
                field_def.get("type") == "string"
                for field_def in props.values()
            ),
            "split_field": lambda: any(
                field_def.get("type") == "string"
                for field_def in props.values()
            ),
            "merge_fields": lambda: len(props) >= 2,
            "add_conditional_validation": lambda: len(props) >= 2,
            "change_array_structure": lambda: any(
                field_def.get("type") == "array"
                for field_def in props.values()
            ),
        }

        return viability_checks.get(operation, lambda: True)()

    @staticmethod
    def evolve_schema(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """执行schema演化，可能包含多个操作"""
        available_ops = EnhancedSchemaEvolutionStrategy.get_available_operations(schema, version_num)

        if not available_ops:
            return schema, "无可用演化操作"

        # 加权随机选择操作
        weights = [op["weight"] for op in available_ops]
        selected_op = random.choices(available_ops, weights=weights, k=1)[0]

        # 执行选中的操作
        operation_func = getattr(EnhancedSchemaEvolutionStrategy, selected_op["func"])
        new_schema, description = operation_func(schema, version_num)

        # 记录已执行的操作
        EnhancedSchemaEvolutionStrategy.executed_operations.add(selected_op["func"])

        return new_schema, f"{selected_op['category']}: {description}"

    # ===== 结构演化操作 =====

    @staticmethod
    def add_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """添加新字段"""
        new_schema = deepcopy(schema)
        props = new_schema.setdefault("properties", {})
        required = new_schema.setdefault("required", [])

        # 随机选择字段类型和名称
        field_types = ["string", "integer", "number", "boolean", "object", "array"]
        field_type = random.choice(field_types)

        base_names = ["field", "property", "attr", "item", "running_case"]
        new_field = f"{random.choice(base_names)}_{version_num}"

        # 创建字段定义
        field_def = {"type": field_type}

        # 为特定类型添加额外属性
        if field_type == "string":
            formats = ["email", "date-time", "uri", "hostname", None]
            format_choice = random.choice(formats)
            if format_choice:
                field_def["format"] = format_choice

            if random.random() > 0.7:
                field_def["maxLength"] = random.randint(5, 100)

            if random.random() > 0.8:
                field_def["pattern"] = "^[A-Za-z0-9]+$"

        elif field_type == "number" or field_type == "integer":
            if random.random() > 0.5:
                field_def["minimum"] = random.randint(0, 100)
            if random.random() > 0.5:
                field_def["maximum"] = random.randint(101, 200)

        elif field_type == "array":
            item_types = ["string", "integer", "number", "boolean", "object"]
            field_def["items"] = {"type": random.choice(item_types)}
            if random.random() > 0.5:
                field_def["minItems"] = random.randint(1, 5)
            if random.random() > 0.5:
                field_def["maxItems"] = random.randint(6, 20)

        elif field_type == "object":
            field_def["properties"] = {
                f"nested_{version_num}_1": {"type": "string"},
                f"nested_{version_num}_2": {"type": "integer"}
            }

        # 添加字段
        props[new_field] = field_def

        # 随机决定是否设为必需字段
        if random.random() > 0.5:
            required.append(new_field)

        return new_schema, f"新增字段 '{new_field}' (类型: {field_type})"

    @staticmethod
    def remove_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """删除字段（优先选择非必需字段）"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if not props:
            return new_schema, "无字段可删除"

        # 优先删除非必需字段
        non_required = [k for k in props.keys() if k not in required]
        candidates = non_required if non_required else list(props.keys())

        if not candidates:
            return new_schema, "无字段可删除"

        field_to_remove = random.choice(candidates)
        props.pop(field_to_remove)

        if field_to_remove in required:
            required.remove(field_to_remove)

        return new_schema, f"删除字段 '{field_to_remove}'"

    @staticmethod
    def rename_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """重命名字段"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if not props:
            return new_schema, "无字段可重命名"

        field_to_rename = random.choice(list(props.keys()))
        new_name = f"{field_to_rename}_renamed_v{version_num}"

        props[new_name] = props.pop(field_to_rename)

        if field_to_rename in required:
            required.remove(field_to_rename)
            required.append(new_name)

        return new_schema, f"重命名字段 '{field_to_rename}' → '{new_name}'"

    @staticmethod
    def change_field_type(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改字段类型"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        # 找到可以修改类型的字段
        candidate_fields = [
            k for k, v in props.items()
            if v.get("type") in ["string", "integer", "number", "boolean"]
        ]

        if not candidate_fields:
            return new_schema, "无合适字段可修改类型"

        field_to_change = random.choice(candidate_fields)
        old_type = props[field_to_change].get("type", "unknown")

        # 定义类型转换规则
        type_conversions = {
            "string": ["integer", "number", "boolean"],
            "integer": ["string", "number"],
            "number": ["string", "integer"],
            "boolean": ["string"]
        }

        available_new_types = type_conversions.get(old_type, ["string"])
        new_type = random.choice(available_new_types)

        # 保存旧约束（可能部分适用）
        old_constraints = {k: v for k, v in props[field_to_change].items() if k != "type"}

        # 更新类型
        props[field_to_change] = {"type": new_type}

        # 保留可能仍然适用的约束
        if new_type == "string" and "maxLength" in old_constraints:
            props[field_to_change]["maxLength"] = old_constraints["maxLength"]

        return new_schema, f"修改字段 '{field_to_change}' 类型: {old_type} → {new_type}"

    @staticmethod
    def nest_fields(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将多个字段嵌套到一个新对象中"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if len(props) < 2:
            return new_schema, "字段不足，无法创建嵌套"

        # 选择要嵌套的字段（2-3个）
        fields_to_nest = random.sample(list(props.keys()), min(3, len(props)))
        nest_name = f"nested_object_v{version_num}"

        # 创建嵌套对象
        nested_props = {}
        for field in fields_to_nest:
            nested_props[field] = props.pop(field)
            if field in required:
                required.remove(field)

        props[nest_name] = {
            "type": "object",
            "properties": nested_props
        }

        # 随机决定嵌套对象是否为必需
        if random.random() > 0.5:
            required.append(nest_name)

        return new_schema, f"创建嵌套对象 '{nest_name}' 包含字段: {', '.join(fields_to_nest)}"

    @staticmethod
    def unnest_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将嵌套对象的字段提升到顶层"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        # 找到嵌套对象
        nested_objects = [
            k for k, v in props.items()
            if v.get("type") == "object" and v.get("properties")
        ]

        if not nested_objects:
            return new_schema, "无嵌套对象可解嵌套"

        object_to_unnest = random.choice(nested_objects)
        nested_props = props[object_to_unnest].get("properties", {})

        if not nested_props:
            return new_schema, f"嵌套对象 '{object_to_unnest}' 无属性可解嵌套"

        # 选择要提升的字段（至少一个）
        fields_to_promote = random.sample(
            list(nested_props.keys()),
            random.randint(1, len(nested_props))
        )

        # 提升字段到顶层
        for field in fields_to_promote:
            props[field] = nested_props.pop(field)

            # 如果原嵌套对象是必需的，提升的字段也设为必需
            if object_to_unnest in required and random.random() > 0.5:
                required.append(field)

        # 如果嵌套对象为空，删除它
        if not nested_props:
            props.pop(object_to_unnest)
            if object_to_unnest in required:
                required.remove(object_to_unnest)

        return new_schema, f"从 '{object_to_unnest}' 解嵌套字段: {', '.join(fields_to_promote)}"

    @staticmethod
    def change_array_structure(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改数组结构"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        # 找到数组字段
        array_fields = [k for k, v in props.items() if v.get("type") == "array"]

        if not array_fields:
            return new_schema, "无数组字段可修改"

        field_to_change = random.choice(array_fields)
        array_def = props[field_to_change]

        # 多种数组修改操作
        operations = [
            ("change_item_type", "修改数组项类型"),
            ("add_min_max_items", "添加最小/最大项数限制"),
            ("make_unique_items", "要求数组项唯一")
        ]

        op_name, op_desc = random.choice(operations)

        if op_name == "change_item_type":
            old_type = array_def.get("items", {}).get("type", "string")
            new_type = random.choice(["string", "integer", "number", "boolean", "object"])
            array_def.setdefault("items", {})["type"] = new_type
            return new_schema, f"修改数组 '{field_to_change}' 项类型: {old_type} → {new_type}"

        elif op_name == "add_min_max_items":
            if random.random() > 0.5:
                array_def["minItems"] = random.randint(1, 5)
            if random.random() > 0.5:
                array_def["maxItems"] = random.randint(6, 20)
            return new_schema, f"为数组 '{field_to_change}' 添加项数限制"

        elif op_name == "make_unique_items":
            array_def["uniqueItems"] = True
            return new_schema, f"设置数组 '{field_to_change}' 项必须唯一"

        return new_schema, f"修改数组 '{field_to_change}' 结构"

    @staticmethod
    def promote_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将嵌套字段提升到顶层"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        # 找到嵌套对象
        nested_objects = [
            k for k, v in props.items()
            if v.get("type") == "object" and v.get("properties")
        ]

        if not nested_objects:
            return new_schema, "无嵌套对象可提升字段"

        object_to_promote_from = random.choice(nested_objects)
        nested_props = props[object_to_promote_from].get("properties", {})

        if not nested_props:
            return new_schema, f"嵌套对象 '{object_to_promote_from}' 无属性可提升"

        # 选择要提升的字段
        field_to_promote = random.choice(list(nested_props.keys()))
        new_field_name = f"{object_to_promote_from}_{field_to_promote}"

        # 提升字段到顶层
        props[new_field_name] = nested_props.pop(field_to_promote)

        # 如果原嵌套对象是必需的，提升的字段也设为必需
        if object_to_promote_from in required and random.random() > 0.5:
            required.append(new_field_name)

        # 如果嵌套对象为空，删除它
        if not nested_props:
            props.pop(object_to_promote_from)
            if object_to_promote_from in required:
                required.remove(object_to_promote_from)

        return new_schema, f"将字段 '{field_to_promote}' 从 '{object_to_promote_from}' 提升为 '{new_field_name}'"

    @staticmethod
    def demote_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将顶层字段降级到嵌套对象中"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if len(props) < 2:
            return new_schema, "字段不足，无法降级"

        # 找到现有的嵌套对象或创建新对象
        nested_objects = [
            k for k, v in props.items()
            if v.get("type") == "object" and v.get("properties")
        ]

        if nested_objects:
            target_object = random.choice(nested_objects)
        else:
            # 创建新的嵌套对象
            target_object = f"nested_object_v{version_num}"
            props[target_object] = {
                "type": "object",
                "properties": {}
            }

        # 选择要降级的字段（不能是嵌套对象本身）
        candidate_fields = [
            k for k in props.keys()
            if k != target_object and props[k].get("type") != "object"
        ]

        if not candidate_fields:
            return new_schema, "无合适字段可降级"

        field_to_demote = random.choice(candidate_fields)

        # 降级字段
        props[target_object]["properties"][field_to_demote] = props.pop(field_to_demote)

        # 如果字段是必需的，从顶层移除并添加到嵌套对象
        if field_to_demote in required:
            required.remove(field_to_demote)
            # 随机决定是否在嵌套对象中设为必需
            if random.random() > 0.5:
                if "required" not in props[target_object]:
                    props[target_object]["required"] = []
                props[target_object]["required"].append(field_to_demote)

        return new_schema, f"将字段 '{field_to_demote}' 降级到嵌套对象 '{target_object}'"

    # ===== 约束演化操作 =====

    @staticmethod
    def change_required_constraint(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改字段的必需约束"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.setdefault("required", [])

        if not props:
            return new_schema, "无字段可修改约束"

        field_to_change = random.choice(list(props.keys()))

        if field_to_change in required:
            # 从必需改为可选
            required.remove(field_to_change)
            return new_schema, f"字段 '{field_to_change}' 从必需改为可选"
        else:
            # 从可选改为必需
            required.append(field_to_change)
            return new_schema, f"字段 '{field_to_change}' 从可选改为必需"

    @staticmethod
    def change_enum_options(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改枚举值选项"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        # 找到有枚举约束的字段
        enum_fields = [k for k, v in props.items() if "enum" in v]

        if not enum_fields:
            return new_schema, "无枚举字段可修改"

        field_to_change = random.choice(enum_fields)
        old_enum = props[field_to_change]["enum"]

        # 枚举操作：添加选项、删除选项或替换所有选项
        operations = ["add_option", "remove_option", "replace_all"]
        operation = random.choice(operations)

        if operation == "add_option" and len(old_enum) < 10:
            new_option = f"option_v{version_num}"
            props[field_to_change]["enum"] = old_enum + [new_option]
            return new_schema, f"为枚举字段 '{field_to_change}' 添加选项 '{new_option}'"

        elif operation == "remove_option" and len(old_enum) > 1:
            option_to_remove = random.choice(old_enum)
            props[field_to_change]["enum"] = [opt for opt in old_enum if opt != option_to_remove]
            return new_schema, f"从枚举字段 '{field_to_change}' 移除选项 '{option_to_remove}'"

        elif operation == "replace_all":
            new_options = [f"new_opt_{i}" for i in range(1, random.randint(2, 6))]
            props[field_to_change]["enum"] = new_options
            return new_schema, f"替换枚举字段 '{field_to_change}' 的所有选项"

        return new_schema, f"修改枚举字段 '{field_to_change}' 的选项"

    @staticmethod
    def change_min_max_constraint(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改数值字段的最小/最大约束"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        # 找到数值字段
        numeric_fields = [
            k for k, v in props.items()
            if v.get("type") in ["integer", "number"]
        ]

        if not numeric_fields:
            return new_schema, "无数值字段可修改约束"

        field_to_change = random.choice(numeric_fields)
        field_def = props[field_to_change]

        # 随机选择要修改的约束
        constraint_to_change = random.choice(["minimum", "maximum", "both"])

        if constraint_to_change in ["minimum", "both"]:
            if "minimum" in field_def:
                old_min = field_def["minimum"]
                new_min = old_min + random.randint(-10, 10)
                field_def["minimum"] = new_min
                min_change = f"minimum: {old_min} → {new_min}"
            else:
                field_def["minimum"] = random.randint(0, 10)
                min_change = f"添加minimum: {field_def['minimum']}"

        if constraint_to_change in ["maximum", "both"]:
            if "maximum" in field_def:
                old_max = field_def["maximum"]
                new_max = old_max + random.randint(-10, 10)
                field_def["maximum"] = new_max
                max_change = f"maximum: {old_max} → {new_max}"
            else:
                field_def["maximum"] = random.randint(100, 200)
                max_change = f"添加maximum: {field_def['maximum']}"

        if constraint_to_change == "minimum":
            change_desc = min_change
        elif constraint_to_change == "maximum":
            change_desc = max_change
        else:
            change_desc = f"{min_change}, {max_change}"

        return new_schema, f"修改字段 '{field_to_change}' 数值约束: {change_desc}"

    @staticmethod
    def change_pattern_constraint(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """修改字符串字段的模式约束"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        # 找到字符串字段
        string_fields = [k for k, v in props.items() if v.get("type") == "string"]

        if not string_fields:
            return new_schema, "无字符串字段可修改模式约束"

        field_to_change = random.choice(string_fields)
        field_def = props[field_to_change]

        # 定义一些常见模式
        patterns = [
            "^[A-Za-z]+$",  # 只允许字母
            "^[0-9]+$",  # 只允许数字
            "^[A-Za-z0-9]+$",  # 字母数字
            "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",  # 电子邮件
            "^(https?|ftp)://[^\\s/$.?#].[^\\s]*$",  # URL
        ]

        if "pattern" in field_def:
            # 修改现有模式
            old_pattern = field_def["pattern"]
            new_pattern = random.choice(patterns)
            field_def["pattern"] = new_pattern
            return new_schema, f"修改字段 '{field_to_change}' 模式: '{old_pattern}' → '{new_pattern}'"
        else:
            # 添加新模式约束
            new_pattern = random.choice(patterns)
            field_def["pattern"] = new_pattern
            return new_schema, f"为字段 '{field_to_change}' 添加模式约束: '{new_pattern}'"

    # ===== 语义演化操作 =====

    @staticmethod
    def split_field(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将一个字段拆分为多个字段"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if not props:
            return new_schema, "无字段可拆分"

        # 优先选择字符串字段进行拆分
        string_fields = [k for k, v in props.items() if v.get("type") == "string"]

        if not string_fields:
            return new_schema, "无字符串字段可拆分"

        field_to_split = random.choice(string_fields)

        # 创建两个新字段
        new_field1 = f"{field_to_split}_part1"
        new_field2 = f"{field_to_split}_part2"

        props[new_field1] = {"type": "string"}
        props[new_field2] = {"type": "string"}

        # 移除原字段
        props.pop(field_to_split)
        if field_to_split in required:
            required.remove(field_to_split)
            # 新字段可能都是必需的或都不是
            if random.random() > 0.5:
                required.extend([new_field1, new_field2])

        return new_schema, f"拆分字段 '{field_to_split}' → '{new_field1}', '{new_field2}'"

    @staticmethod
    def merge_fields(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """将多个字段合并为一个字段"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})
        required = new_schema.get("required", [])

        if len(props) < 2:
            return new_schema, "字段不足，无法合并"

        # 随机选择2-3个字段进行合并
        fields_to_merge = random.sample(list(props.keys()), min(3, len(props)))
        merged_field = f"merged_field_v{version_num}"

        # 创建合并后的字段
        props[merged_field] = {"type": "string"}

        # 移除原字段
        for field in fields_to_merge:
            props.pop(field)
            if field in required:
                required.remove(field)

        # 随机决定合并后的字段是否为必需
        if random.random() > 0.5:
            required.append(merged_field)

        return new_schema, f"合并字段 '{', '.join(fields_to_merge)}' → '{merged_field}'"

    @staticmethod
    def add_conditional_validation(schema: Dict, version_num: int) -> Tuple[Dict, str]:
        """添加条件验证逻辑"""
        new_schema = deepcopy(schema)
        props = new_schema.get("properties", {})

        if len(props) < 2:
            return new_schema, "字段不足，无法添加条件验证"

        # 随机选择两个字段创建条件关系
        field1, field2 = random.sample(list(props.keys()), 2)

        # 简单的条件验证：如果field1有特定值，则field2为必需
        condition_value = random.choice(["true", "false", "yes", "no", "required", "optional"])

        # 添加条件逻辑
        if "allOf" not in new_schema:
            new_schema["allOf"] = []

        new_schema["allOf"].append({
            "if": {
                "properties": {
                    field1: {"const": condition_value}
                }
            },
            "then": {
                "required": [field2]
            }
        })

        return new_schema, f"添加条件验证: 当 '{field1}' = '{condition_value}' 时, '{field2}' 为必需"


class SchemaEvolver:
    """主类：管理 JSON Schema 的演化与数据生成"""

    def __init__(self, output_dir: str = "./evolved_dataset",
                 num_versions: int = 10,
                 num_docs_per_version: int = 5):
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

        # 重置已执行操作记录
        EnhancedSchemaEvolutionStrategy.executed_operations = set()

        for v in range(1, self.num_versions + 1):
            # 演化 schema
            evolved_schema, desc = EnhancedSchemaEvolutionStrategy.evolve_schema(current_schema, v)
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
    # 创建演化器实例
    evolver = SchemaEvolver(
        output_dir="./enhanced_evolved_dataset",  # 输出目录
        num_versions=8,  # 生成8个版本
        num_docs_per_version=5  # 每个版本生成5个文档
    )

    # 方式1：从Hugging Face数据集处理
    evolver.process_schema_source(
        "epfl-dlab/JSONSchemaBench",
        max_schemas_per_subset=3
    )

    # 方式2：处理本地Schema文件
    # evolver.process_schema_source(
    #     "./my_schema.json",
    #     unique_id="custom_schema"
    # )