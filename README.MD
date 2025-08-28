# Микросервис управления данными пользователей

## Описание

Этот микросервис предоставляет функциональность для:
- Аутентификации пользователей (JWT токены)
- Управления пользовательскими данными (резюме, требования, результаты обработки)
- Кэширования данных с использованием Redis
- Асинхронной обработки событий через Kafka
- Взаимодействия с PostgreSQL базой данных

## Основные функции
   - Хранение и управление данными:
     - Пользователи (`User`)
     - Резюме (`Resume`)
     - Требования (`Requirements`)
     - Результаты обработки (`Processing`)
   - Кеширование данных в Redis
   - Синхронизация данных между сервисами через Kafka

## Технологический стек

- **Python 3.13.3**
- **FastAPI** + **Uvicorn**
- **SQLAlchemy (async)** - работа с базой данных
- **PostgreSQL** - основное хранилище данных
- **Redis** - кэширование
- **Kafka** + **confluent-kafka**
- **Pydantic** - валидация данных
- **JWT** - аутентификация
- **Pytest** - тестирование

## Запуск сервиса

### Требования
- Docker и Docker Compose
- Python 3.10+

### Установка
1. Клонируйте репозиторий
2. Выполните: `pip install -r requirements.txt`
3. Создайте файл `.env` и `.test.env`(для тестирования) на основе `.env.example`
4. Заполните необходимые переменные окружения

### Запуск
```bash
docker-compose up -d
```

Сервис будет доступен по адресу: `http://localhost:8005`

## API Endpoints

### Работа с данными
|Метод| Эндпоинт                 | Описание                                        |
|-----|--------------------------|-------------------------------------------------|
| GET | `/me`                    | информация о текущем пользователе               |
| GET | `/get_resume`            | получение резюме                                |
| GET | `/get_requirements`      | получение требований                            |
| GET | `/get_processing`        | получение результатов обработки (краткая форма) |
| GET | `/get_processing_detail` | получение результатов обработки (полная форма)  |


### Health check
|Метод| Эндпоинт                 | Описание                                 |
|-----|--------------------------|------------------------------------------|
| GET | `/health`                | проверка работоспособности сервиса       |


## Конфигурация
Основные настройки сервиса находятся в:
- `src/config.py` - основные параметры сервиса
- `.env` - переменные окружения

## Тестирование
Для запуска тестов:
```bash
pytest
```
Для запуска тестов через Docker:
```bash
docker-compose --env-file .test.env up -d --build db redis zookeeper kafka
```
```bash
sleep 10
```
```bash
docker-compose --env-file .test.env up -d app  # пересоздаем основной сервис
```
```bash
docker-compose --env-file .test.env exec app pytest
```

Тесты покрывают:
- Unit-тесты основных функций
- Интеграционные тесты API
- Тесты работы с Kafka
- Тесты работы с Redis и PostgreSQL

## Логирование
Логи сохраняются в файл `logs/auth_service.log` с ротацией по дням.

## Данные получаемые с kafka:

- Читает по топику `uploading_data`
  - читаемые ключи: `new_user`, `new_resume`, `new_requirements`, `delete_processing`, `delete_requirements`, 
  `new_processing`

## Хранение Redis:

### 1. Данные пользователя
**Ключ:** `user:{user_id}`  
**Значение:**  
```json
{
  "user_id": int,
  "username": str,
  "full_name": str,
  "created_at": datetime (строка, требуется преобразование в datetime)
}
```  
**Источник:**  
- `access.py` (JWT аутентификация)  
- `kafka_dependencies.py` (создание через Kafka)  

**TTL:**  
- Для кэша аутентификации: `ACCESS_TOKEN_EXPIRE_MINUTES * 60`  
- Для данных из Kafka: без TTL  

---

### 2. Данные резюме
**Ключ:** `resume:{resume_id}`  
**Значение:**  
```json
{
  "resume_id": int,
  "user_id": int,
  "resume": str
}
```  
**Источник:**  
- `kafka_dependencies.py` (обработчик `handler_key_new_resume`)  
- `post.py` (эндпоинт добавления резюме)  

**TTL:**  
- Для резюме из Kafka: без TTL  
- Для временных резюме: `STORAGE_TIME_RESUME` (1 час)  

---

### 3. Данные требований
**Ключ:**  
- Все требования пользователя: `requirements:{user_id}`  
- Конкретное требование: `requirements:{requirements_id}`  

**Значение:**  
- Для списка требований:  
  ```json
  [
    {
      "requirements_id": int,
      "user_id": int,
      "requirements": str
    },
    ...
  ]
  ```  
- Для отдельного требования: строка (текст требований)  

**Источник:**  
- `kafka_dependencies.py` (обработчик `handler_key_new_requirements`)  
- `post.py` (эндпоинт добавления требований)  

**TTL:**  
- Для требований из Kafka: без TTL  
- Для временных требований: `STORAGE_TIME_REQUIREMENTS` (3 дня)  

---

### 4. Данные обработки
**Ключ:**  
- Все обработки пользователя: `processing:{user_id}`  
- Обработки для конкретного требования: `processing_requirements:{user_id}:{requirements_id}`  

**Значение:**  
```json
[
  {
    "processing_id": int,
    "resume_id": int,
    "requirements_id": int,
    "user_id": int,
    "create_at": str (ISO format),
    "score": int,
    "matches": list[str],
    "recommendation": str,
    "verdict": str,
    "resume": str (опционально),
    "requirements": str (опционально)
  },
  ...
]
```  
**Источник:**  
- `kafka_dependencies.py` (обработчик `handler_key_new_processing`)  

**TTL:** Без TTL  

---

### 5. Отметки об обработанных сообщениях Kafka
**Ключ:** `processed_message:{message_id}`  
**Значение:** `'_'` (метка)  
**Назначение:** Исключение повторной обработки сообщений.  
**Источник:**  
- `kafka_dependencies.py` (метод `worker_topic`)  

**TTL:** Без TTL