import re
from typing import Any, Union

# Base class for all expressions
class Expr:
    def compile(self) -> str:
        pass


def compile_value(value: Union[Expr, Any]) -> str:
    if isinstance(value, Expr):
        return value.compile()
    elif isinstance(value, str):
        return f"'{value}'"  # Surround strings with quotes
    else:
        return str(value)  # Keep numbers and other types as-is


# Implement Condition class for logical conditions
class Condition(Expr):
    def __init__(self, left: Expr, operator: str, right: Any):
        self.left = left
        self.operator = operator
        self.right = right

    def compile(self) -> str:
        return f"{compile_value(self.left)} {self.operator} {compile_value(self.right)}"


# Implement 'col' for column references
class col(Expr):
    def __init__(self, column_name: str):
        self.column_name = column_name

    def compile(self) -> str:
        return f"`{self.column_name}`"

    def __eq__(self, other: Any) -> Condition:
        return Condition(self, "=", other)

    def __gt__(self, other: Any) -> Condition:
        return Condition(self, ">", other)

    def __ge__(self, other: Any) -> Condition:
        return Condition(self, ">=", other)

    def __lt__(self, other: Any) -> Condition:
        return Condition(self, "<", other)

    def __le__(self, other: Any) -> Condition:
        return Condition(self, "<=", other)


# Implement 'when' for conditional logic
class when(Expr):
    def __init__(self, condition: Expr):
        self.condition = condition

    def then(self, value: Any) -> "when":
        self.true_value = value
        return self

    def otherwise(self, value: Any) -> "when":
        self.false_value = value
        return self

    def compile(self) -> str:
        return f"CASE WHEN {compile_value(self.condition)} THEN {compile_value(self.true_value)} ELSE {compile_value(self.false_value)} END"


# Implement AND logic
class and_(Expr):
    def __init__(self, *args: Expr):
        self.args = args

    def compile(self) -> str:
        return " AND ".join([compile_value(arg) for arg in self.args])


# Implement 'concat' for string concatenation
class concat(Expr):
    def __init__(self, *args: Expr):
        self.args = args

    def compile(self) -> str:
        return f"CONCAT({', '.join([compile_value(arg) for arg in self.args])})"


# Implement 'date_diff' to get the difference between two dates
class date_diff(Expr):
    def __init__(self, date1: Expr, date2: Expr):
        self.date1 = date1
        self.date2 = date2

    def compile(self) -> str:
        return f"DATEDIFF({compile_value(self.date1)}, {compile_value(self.date2)})"


# Implement addition
class add(Expr):
    def __init__(self, left: Expr, right: Expr):
        self.left = left
        self.right = right

    def compile(self) -> str:
        return f"{compile_value(self.left)} + {compile_value(self.right)}"


# Implement 'or_' for OR logic
class or_(Expr):
    def __init__(self, *args: Expr):
        self.args = args

    def compile(self) -> str:
        return " OR ".join([compile_value(arg) for arg in self.args])


# Implement subtraction
class sub(Expr):
    def __init__(self, left: Expr, right: Expr):
        self.left = left
        self.right = right

    def compile(self) -> str:
        return f"{compile_value(self.left)} - {compile_value(self.right)}"


# Implement multiplication
class mul(Expr):
    def __init__(self, left: Expr, right: Expr):
        self.left = left
        self.right = right

    def compile(self) -> str:
        return f"{compile_value(self.left)} * {compile_value(self.right)}"


# Implement division
class div(Expr):
    def __init__(self, left: Expr, right: Expr):
        self.left = left
        self.right = right

    def compile(self) -> str:
        return f"{compile_value(self.left)} / {compile_value(self.right)}"


def parse_col(text: str) -> col:
    column_name = re.findall(r"`([^`]+)`", text)[0]
    return col(column_name)


def parse_condition(text: str) -> Condition:
    # Attempt to match more general condition expressions
    match = re.match(r"(.+) ([=<>!]+) (.+)", text)
    if not match:
        raise ValueError("Invalid condition")

    left, operator, right = match.groups()
    return Condition(parse_value(left.strip()), operator, parse_value(right.strip()))


def parse_add(text: str) -> add:
    left, right = text.split(" + ")
    return add(parse_value(left), parse_value(right))


def parse_when(text: str) -> when:
    match = re.match(r"CASE WHEN (.+) THEN (.+) ELSE (.+) END", text)
    if not match:
        raise ValueError("Invalid when")

    condition, true_value, false_value = match.groups()
    return (
        when(parse_value(condition))
        .then(parse_value(true_value))
        .otherwise(parse_value(false_value))
    )


def parse_concat(text: str) -> concat:
    # This regular expression captures everything inside CONCAT's parentheses
    match = re.match(r"CONCAT\((.+)\)", text)
    if not match:
        raise ValueError("Invalid CONCAT expression")

    # Extracting arguments and splitting them by comma
    args_text = match.group(1)
    args = [arg.strip() for arg in args_text.split(",")]

    # Parsing each argument and creating a concat instance
    return concat(*[parse_value(arg) for arg in args])


def parse_value(text: str) -> Expr:
    text = text.strip()
    if re.match(r"CASE WHEN .+ THEN .+ ELSE .+ END", text):
        return parse_when(text)
    elif text.startswith("`") and text.endswith("`"):
        return parse_col(text)
    elif text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    elif " AND " in text:
        return and_(*[parse_value(sub_expr) for sub_expr in text.split(" AND ")])
    elif " OR " in text:
        return or_(*[parse_value(sub_expr) for sub_expr in text.split(" OR ")])
    elif text.isdigit():
        return int(text)
    elif re.match(r".+ [=<>!]+ .+", text):
        return parse_condition(text)
    elif " + " in text:
        return parse_add(text)
    elif " - " in text:
        left, right = text.split(" - ")
        return sub(parse_value(left), parse_value(right))
    elif " * " in text:
        left, right = text.split(" * ")
        return mul(parse_value(left), parse_value(right))
    elif " / " in text:
        left, right = text.split(" / ")
        return div(parse_value(left), parse_value(right))
    elif text == "True":
        return True
    elif text == "False":
        return False
    elif text.startswith("CONCAT(") and text.endswith(")"):
        return parse_concat(text)
    else:
        raise ValueError(f"Invalid expression: {text}")


def reconstruct_case_when(compiled: str) -> Expr:
    match = re.match(r"CASE WHEN (.+) THEN (.+) ELSE (.+) END", compiled)
    if not match:
        raise ValueError("Invalid CASE WHEN expression")

    condition, true_value, false_value = match.groups()
    return (
        when(parse_value(condition))
        .then(parse_value(true_value))
        .otherwise(parse_value(false_value))
    )


def reconstruct(compiled: str) -> Expr:
    if "CASE WHEN" in compiled:
        return reconstruct_case_when(compiled)
    else:
        return parse_value(compiled)
