from fastapi import FastAPI, HTTPException, Depends, status, Cookie, Response, BackgroundTasks
from sqlalchemy import create_engine, Column, Integer, String, Float, func
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel
import csv
import secrets
from datetime import datetime, timedelta
import redis
import json
import os

# Инициализация FastAPI
app = FastAPI()

# Конфигурация Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Модель данных
Base = declarative_base()

class Student(Base):
    __tablename__ = 'students'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    surname = Column(String(50), nullable=False)
    name = Column(String(50), nullable=False)
    faculty = Column(String(50), nullable=False)
    course = Column(String(50), nullable=False)
    grade = Column(Integer, nullable=False)

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    password = Column(String(100), nullable=False)
    session_token = Column(String(100), nullable=True)
    token_expiry = Column(Integer, nullable=True)

# Pydantic модели
class StudentCreate(BaseModel):
    surname: str
    name: str
    faculty: str
    course: str
    grade: int

class StudentUpdate(BaseModel):
    surname: str | None = None
    name: str | None = None
    faculty: str | None = None
    course: str | None = None
    grade: int | None = None

class UserRegister(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class DeleteStudentsRequest(BaseModel):
    student_ids: list[int]

# Конфигурация базы данных
DATABASE_URL = "sqlite:///./students.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Создание таблиц
Base.metadata.create_all(bind=engine)

# Dependency для получения сессии базы данных
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Функции для работы с паролями
def hash_password(password: str) -> str:
    """Простая функция хеширования пароля (в реальном приложении используйте bcrypt)"""
    return password + "_hashed"

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password

# Функции для работы с кешем
def get_cache_key(endpoint: str, **kwargs) -> str:
    """Генерация ключа кеша на основе эндпойнта и параметров"""
    key = f"cache:{endpoint}"
    for k, v in sorted(kwargs.items()):
        key += f":{k}={v}"
    return key

def get_cached_response(cache_key: str):
    """Получение кешированного ответа из Redis"""
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    return None

def set_cached_response(cache_key: str, data, expire: int = 300):
    """Сохранение ответа в кеш Redis"""
    redis_client.setex(cache_key, expire, json.dumps(data))

def invalidate_cache(endpoint_prefix: str):
    """Инвалидация кеша для всех ключей с указанным префиксом"""
    keys = redis_client.keys(f"cache:{endpoint_prefix}:*")
    if keys:
        redis_client.delete(*keys)

# Класс для работы с базой данных
class StudentDatabase:
    def __init__(self, db: Session = Depends(get_db)):
        self.db = db
    
    # Методы для студентов
    def insert_student(self, student: StudentCreate):
        db_student = Student(**student.dict())
        self.db.add(db_student)
        self.db.commit()
        self.db.refresh(db_student)
        return db_student
    
    def get_student(self, student_id: int):
        student = self.db.query(Student).filter(Student.id == student_id).first()
        if not student:
            raise HTTPException(status_code=404, detail="Student not found")
        return student
    
    def get_all_students(self):
        return self.db.query(Student).all()
    
    def update_student(self, student_id: int, student_data: StudentUpdate):
        student = self.get_student(student_id)
        for key, value in student_data.dict(exclude_unset=True).items():
            setattr(student, key, value)
        self.db.commit()
        self.db.refresh(student)
        return student
    
    def delete_student(self, student_id: int):
        student = self.get_student(student_id)
        self.db.delete(student)
        self.db.commit()
        return {"message": "Student deleted successfully"}
    
    def load_from_csv(self, file_path: str):
        """Загрузка данных из CSV-файла в базу данных"""
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                student = Student(
                    surname=row['Фамилия'],
                    name=row['Имя'],
                    faculty=row['Факультет'],
                    course=row['Курс'],
                    grade=int(row['Оценка'])
                )
                self.db.add(student)
            self.db.commit()
    
    def delete_students_by_ids(self, student_ids: list[int]):
        """Удаление студентов по списку ID"""
        if not student_ids:
            return
        
        # Удаляем студентов в транзакции
        self.db.query(Student).filter(Student.id.in_(student_ids)).delete(
            synchronize_session=False
        )
        self.db.commit()
    
    def get_students_by_faculty(self, faculty_name: str):
        return self.db.query(Student).filter(
            Student.faculty == faculty_name
        ).all()
    
    def get_unique_courses(self):
        courses = self.db.query(Student.course).distinct().all()
        return [course[0] for course in courses]
    
    def get_average_grade_by_faculty(self, faculty_name: str):
        avg_grade = self.db.query(func.avg(Student.grade)).filter(
            Student.faculty == faculty_name
        ).scalar()
        return round(avg_grade, 2) if avg_grade else 0.0
    
    # Методы для работы с пользователями
    def register_user(self, user: UserRegister):
        existing_user = self.db.query(User).filter(
            User.username == user.username
        ).first()
        
        if existing_user:
            raise HTTPException(
                status_code=400,
                detail="Username already registered"
            )
        
        new_user = User(
            username=user.username,
            password=hash_password(user.password)
        self.db.add(new_user)
        self.db.commit()
        return new_user
    
    def login_user(self, user: UserLogin):
        db_user = self.db.query(User).filter(
            User.username == user.username
        ).first()
        
        if not db_user or not verify_password(user.password, db_user.password):
            raise HTTPException(
                status_code=401,
                detail="Incorrect username or password"
            )
        
        session_token = secrets.token_urlsafe(32)
        db_user.session_token = session_token
        db_user.token_expiry = int((datetime.now() + timedelta(days=1)).timestamp()
        self.db.commit()
        
        return db_user
    
    def logout_user(self, session_token: str):
        user = self.db.query(User).filter(
            User.session_token == session_token
        ).first()
        
        if user:
            user.session_token = None
            user.token_expiry = None
            self.db.commit()
        
        return {"message": "Logged out successfully"}
    
    def get_current_user(self, session_token: str):
        if not session_token:
            return None
        
        user = self.db.query(User).filter(
            User.session_token == session_token
        ).first()
        
        if user and user.token_expiry and datetime.now().timestamp() > user.token_expiry:
            user.session_token = None
            user.token_expiry = None
            self.db.commit()
            return None
        
        return user

# Dependency для получения текущего пользователя
def get_current_user(session_token: str = Cookie(None), db: Session = Depends(get_db)):
    if not session_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    db_service = StudentDatabase(db)
    user = db_service.get_current_user(session_token)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session token"
        )
    
    return user

# Фоновые задачи
def background_load_csv(db: Session, file_path: str):
    """Фоновая задача для загрузки данных из CSV"""
    try:
        print(f"Starting CSV import from {file_path}")
        db_service = StudentDatabase(db)
        db_service.load_from_csv(file_path)
        print("CSV import completed successfully")
        
        # Инвалидация кеша после обновления данных
        invalidate_cache("students")
        invalidate_cache("faculties")
        invalidate_cache("courses")
    except Exception as e:
        print(f"Error during CSV import: {str(e)}")

def background_delete_students(db: Session, student_ids: list[int]):
    """Фоновая задача для удаления студентов"""
    try:
        print(f"Starting deletion of {len(student_ids)} students")
        db_service = StudentDatabase(db)
        db_service.delete_students_by_ids(student_ids)
        print("Student deletion completed successfully")
        
        # Инвалидация кеша после обновления данных
        invalidate_cache("students")
        invalidate_cache("faculties")
    except Exception as e:
        print(f"Error during student deletion: {str(e)}")

# Эндпойнты аутентификации
@app.post("/auth/register")
def register_user(user: UserRegister, db: Session = Depends(get_db)):
    db_service = StudentDatabase(db)
    db_service.register_user(user)
    return {"message": "User registered successfully"}

@app.post("/auth/login")
def login_user(response: Response, user: UserLogin, db: Session = Depends(get_db)):
    db_service = StudentDatabase(db)
    db_user = db_service.login_user(user)
    
    response.set_cookie(
        key="session_token",
        value=db_user.session_token,
        httponly=True,
        max_age=86400
    )
    
    return {"message": "Login successful"}

@app.post("/auth/logout")
def logout_user(response: Response, 
                session_token: str = Cookie(None),
                db: Session = Depends(get_db),
                current_user: User = Depends(get_current_user)):
    db_service = StudentDatabase(db)
    db_service.logout_user(session_token)
    
    response.delete_cookie("session_token")
    
    return {"message": "Logout successful"}

# Эндпойнты для фоновых задач
@app.post("/students/load-csv")
def load_csv_from_file(
    file_path: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Запуск фоновой задачи для загрузки данных из CSV"""
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    background_tasks.add_task(background_load_csv, db, file_path)
    return {"message": "CSV import started in background"}

@app.delete("/students/bulk-delete")
def delete_students_bulk(
    request: DeleteStudentsRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Запуск фоновой задачи для удаления студентов"""
    if not request.student_ids:
        raise HTTPException(status_code=400, detail="No student IDs provided")
    
    background_tasks.add_task(background_delete_students, db, request.student_ids)
    return {"message": "Student deletion started in background"}

# Защищенные CRUD эндпойнты с кешированием
@app.post("/students/", response_model=Student)
def create_student(
    student: StudentCreate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    db_student = StudentDatabase(db).insert_student(student)
    
    # Инвалидация кеша после добавления новых данных
    invalidate_cache("students")
    invalidate_cache("faculties")
    
    return db_student

@app.get("/students/", response_model=list[Student])
def read_all_students(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Генерация ключа кеша
    cache_key = get_cache_key("students", skip=skip, limit=limit)
    
    # Проверка кеша
    cached = get_cached_response(cache_key)
    if cached is not None:
        return cached
    
    # Получение данных из базы, если нет в кеше
    students = StudentDatabase(db).get_all_students()
    result = students[skip : skip + limit]
    
    # Сохранение в кеш
    set_cached_response(cache_key, result)
    
    return result

@app.get("/students/{student_id}", response_model=Student)
def read_student(
    student_id: int, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    cache_key = get_cache_key("student", student_id=student_id)
    
    cached = get_cached_response(cache_key)
    if cached is not None:
        return cached
    
    student = StudentDatabase(db).get_student(student_id)
    
    set_cached_response(cache_key, student)
    
    return student

@app.put("/students/{student_id}", response_model=Student)
def update_student(
    student_id: int, 
    student_data: StudentUpdate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    updated = StudentDatabase(db).update_student(student_id, student_data)
    
    # Инвалидация кеша после обновления данных
    invalidate_cache("students")
    invalidate_cache("student", student_id=student_id)
    invalidate_cache("faculties")
    
    return updated

@app.delete("/students/{student_id}")
def delete_student(
    student_id: int, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    result = StudentDatabase(db).delete_student(student_id)
    
    # Инвалидация кеша после удаления данных
    invalidate_cache("students")
    invalidate_cache("student", student_id=student_id)
    invalidate_cache("faculties")
    
    return result

# Защищенные аналитические эндпойнты с кешированием
@app.get("/faculties/{faculty}/students", response_model=list[Student])
def get_faculty_students(
    faculty: str, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    cache_key = get_cache_key("faculty_students", faculty=faculty)
    
    cached = get_cached_response(cache_key)
    if cached is not None:
        return cached
    
    students = StudentDatabase(db).get_students_by_faculty(faculty)
    
    set_cached_response(cache_key, students)
    
    return students

@app.get("/courses/", response_model=list[str])
def get_unique_courses(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    cache_key = get_cache_key("unique_courses")
    
    cached = get_cached_response(cache_key)
    if cached is not None:
        return cached
    
    courses = StudentDatabase(db).get_unique_courses()
    
    set_cached_response(cache_key, courses)
    
    return courses

@app.get("/faculties/{faculty}/average_grade", response_model=float)
def get_faculty_average_grade(
    faculty: str, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    cache_key = get_cache_key("faculty_avg_grade", faculty=faculty)
    
    cached = get_cached_response(cache_key)
    if cached is not None:
        return cached
    
    avg_grade = StudentDatabase(db).get_average_grade_by_faculty(faculty)
    
    set_cached_response(cache_key, avg_grade)
    
    return avg_grade

# Инициализация данных
@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        # Создаем тестового пользователя
        if db.query(User).count() == 0:
            test_user = User(
                username="admin",
                password=hash_password("admin123")
            )
            db.add(test_user)
            db.commit()
            print("Test user created: admin/admin123")
        
        # Загрузка данных студентов
        if db.query(Student).count() == 0 and os.path.exists("students.csv"):
            print("Loading initial data from students.csv")
            StudentDatabase(db).load_from_csv("students.csv")
            db.commit()
            print("Database initialized with sample data")
        else:
            print("Database already contains data")
    finally:
        db.close()

# Проверка подключения к Redis
@app.on_event("startup")
def check_redis_connection():
    try:
        redis_client.ping()
        print("Successfully connected to Redis")
    except redis.ConnectionError:
        print("Could not connect to Redis")