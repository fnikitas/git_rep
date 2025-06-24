from fastapi import FastAPI, HTTPException
import re
import math

app = FastAPI()

# Временное хранилище выражений (в памяти)
expressions = {}
expression_counter = 1

# Вспомогательные функции для вычислений
def safe_divide(a: float, b: float) -> float:
    if math.isclose(b, 0, abs_tol=1e-9):
        raise HTTPException(status_code=400, detail="Division by zero")
    return a / b

def apply_operator(a: float, op: str, b: float) -> float:
    operations = {
        '+': lambda x, y: x + y,
        '-': lambda x, y: x - y,
        '*': lambda x, y: x * y,
        '/': safe_divide
    }
    if op not in operations:
        raise HTTPException(status_code=400, detail=f"Unsupported operator: {op}")
    return operations[op](a, b)

# Эндпоинты API
@app.post("/expression/")
async def create_expression(a: float, op: str, b: float) -> dict:
    """Создание простого выражения (a оператор b)"""
    global expression_counter
    expr_id = expression_counter
    expression_counter += 1
    
    result = apply_operator(a, op, b)
    expression_str = f"({a} {op} {b})"
    
    expressions[expr_id] = {
        "expression": expression_str,
        "value": result,
        "is_evaluated": True
    }
    
    return {"id": expr_id, "expression": expression_str, "result": result}

@app.post("/complex_expression/")
async def create_complex_expression(expression: str) -> dict:
    """Создание сложного выражения из строки"""
    global expression_counter
    expr_id = expression_counter
    expression_counter += 1
    
    # Проверка безопасности выражения
    if not re.match(r'^[\d\s+\-*/().]+$', expression):
        raise HTTPException(
            status_code=400,
            detail="Invalid characters in expression. Only digits, operators +-*/ and parentheses allowed"
        )
    
    try:
        # Безопасное вычисление выражения
        result = eval(expression, {"__builtins__": None}, {
            "sqrt": math.sqrt,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "log": math.log,
            "exp": math.exp
        })
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error evaluating expression: {str(e)}"
        )
    
    expressions[expr_id] = {
        "expression": expression,
        "value": result,
        "is_evaluated": True
    }
    
    return {"id": expr_id, "expression": expression, "result": result}

@app.get("/expression/{expr_id}")
async def get_expression(expr_id: int) -> dict:
    """Просмотр выражения по ID"""
    if expr_id not in expressions:
        raise HTTPException(status_code=404, detail="Expression not found")
    
    expr_data = expressions[expr_id]
    return {
        "id": expr_id,
        "expression": expr_data["expression"],
        "is_evaluated": expr_data["is_evaluated"],
        "result": expr_data["value"] if expr_data["is_evaluated"] else None
    }

@app.post("/expression/{expr_id}/evaluate")
async def evaluate_expression(expr_id: int) -> dict:
    """Выполнение выражения по ID"""
    if expr_id not in expressions:
        raise HTTPException(status_code=404, detail="Expression not found")
    
    expr_data = expressions[expr_id]
    if not expr_data["is_evaluated"]:
        try:
            result = eval(expr_data["expression"])
            expr_data["value"] = result
            expr_data["is_evaluated"] = True
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error evaluating expression: {str(e)}"
            )
    
    return {
        "id": expr_id,
        "expression": expr_data["expression"],
        "result": expr_data["value"]
    }

@app.post("/expression/{parent_id}/add_subexpression")
async def add_subexpression(parent_id: int, a: float, op: str, b: float) -> dict:
    """Добавление подвыражения к существующему выражению"""
    if parent_id not in expressions:
        raise HTTPException(status_code=404, detail="Parent expression not found")
    
    # Создаем новое подвыражение
    sub_result = apply_operator(a, op, b)
    sub_expr = f"({a} {op} {b})"
    
    # Обновляем родительское выражение
    parent = expressions[parent_id]
    new_expression = f"{parent['expression']} + {sub_expr}"
    
    expressions[parent_id] = {
        "expression": new_expression,
        "value": parent.get("value", 0) + sub_result,
        "is_evaluated": True
    }
    
    return {
        "id": parent_id,
        "expression": new_expression,
        "result": expressions[parent_id]["value"]
    }

# Базовые операции
@app.get("/add")
async def add(x: float, y: float) -> dict:
    return {"result": x + y}

@app.get("/subtract")
async def subtract(x: float, y: float) -> dict:
    return {"result": x - y}

@app.get("/multiply")
async def multiply(x: float, y: float) -> dict:
    return {"result": x * y}

@app.get("/divide")
async def divide(x: float, y: float) -> dict:
    return {"result": safe_divide(x, y)}