
## Хранение Redis:

### Данные об завершённых обработках с kafka 
**Ключ:** `storage:processed_message:{message_id}`  
**Значение:** `"_"`

**TTL:**  
- 5 часов

### Данные обработки по резюме
**Ключ:** `storage:processing_by_resume:{processing.resume_id}`  
**Значение:**  
```json
{
  "processing_id": "int",
  "resume_id": "int",
  "requirement_id": "int",
  "user_id": "int",

  "status": "ProcessingStatus",
  "success": "bool",

  "message_error": "str",
  "wait_seconds": "int",

  "score": "int",
  "matches": "list",
  "recommendation": "str",
  "verdict": "str",

  "created_at": "str"
}
``` 
**TTL:**  
- 1 день


### Данные о всех требованиях пользователя по его ID
**Ключ:** `storage:requirements_by_user:{user_id}`  
**Значение:**  
```json
[
    {
      "processing_id": "int",
      "user_id": "int",
      "requirement": "str",
      "created_at": "str"
    }
]
``` 
**TTL:**  
- 3 деня


### Данные о всех резюме по ID требования
**Ключ:** `storage:resumes_by_requirement:{requirement_id}`  
**Значение:**  
```json
[
    {
      "resume_id": "int",
      "user_id": "int",
      "requirement_id": "int",
      "resume": "str",
      "created_at": "str"
    }
]
``` 
**TTL:**  
- 1 день


### Данные о пользователе
**Ключ:** `storage:user:{user_id}`  
**Значение:**  
```json
{
  "user_id": "int",
  "username": "str",
  "full_name": "str",
  "created_at": "str"
}
``` 
**TTL:**  
- 1 день