from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, validator, constr
import json
import os
import re

app = FastAPI()

# Создаем директорию для хранения обращений, если её нет
os.makedirs("complaints", exist_ok=True)

class ComplaintBase(BaseModel):
    surname: constr(min_length=1, max_length=50)
    name: constr(min_length=1, max_length=50)
    birthdate: date
    phone: str
    email: EmailStr

    # Валидация фамилии (только кириллица, первая буква заглавная)
    @validator('surname')
    def validate_surname(cls, v):
        if not re.match(r'^[А-ЯЁ][а-яё]*$', v):
            raise ValueError('Фамилия должна содержать только кириллицу и начинаться с заглавной буквы')
        return v

    # Валидация имени (только кириллица, первая буква заглавная)
    @validator('name')
    def validate_name(cls, v):
        if not re.match(r'^[А-ЯЁ][а-яё]*$', v):
            raise ValueError('Имя должно содержать только кириллицу и начинаться с заглавной буквы')
        return v

    # Валидация номера телефона
    @validator('phone')
    def validate_phone(cls, v):
        # Проверяем российские форматы номеров: +7..., 8..., и другие международные форматы
        if not re.match(r'^(\+7|7|8)?[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}$', v):
            raise ValueError('Неверный формат номера телефона')
        return v

@app.post("/submit-complaint/")
async def submit_complaint(complaint: ComplaintBase):
    try:
        # Преобразуем данные в словарь
        complaint_data = complaint.dict()
        complaint_data["birthdate"] = complaint_data["birthdate"].isoformat()
        
        # Генерируем уникальное имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"complaints/complaint_{timestamp}_{complaint.surname}.json"
        
        # Сохраняем в JSON-файл
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(complaint_data, f, ensure_ascii=False, indent=2)
            
        return {
            "status": "success",
            "message": "Обращение успешно сохранено",
            "filename": filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))