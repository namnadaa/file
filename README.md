# File Downloader Service (Go, stdlib-only)

Мини-веб-сервис для пакетной загрузки файлов из интернета со статусами, паузой/резюмом и устойчивостью к перезапуску — **без Docker/БД/Nginx**, только стандартная библиотека Go.

## В чём суть

- Принимает **задачи** (`Task`) с набором ссылок.
- Скачивает файлы в локальную папку (`data/downloads/…`).
- Даёт **статус** задачи и каждого файла.
- **Переживает перезапуск**: состояние чекпоинтится в `data/tasks/*.json`, докачка продолжается через HTTP `Range` (если сервер поддерживает).
- **Грациозная остановка**: на SIGINT/SIGTERM — пауза планирования, сохранение прогресса, корректное завершение воркеров; HTTP завершает активные запросы.

## Быстрый старт

```bash
go run .
```

Переменные окружения:
	•	ADDR — адрес HTTP, по умолчанию :8080
	•	DOWNLOAD_CONCURRENCY — параллелизм загрузок (воркеров), по умолчанию 4
	•	DOWNLOAD_DIR — корень для файлов, по умолчанию data/downloads
	•	TASK_STATE_DIR — корень для состояния задач, по умолчанию data/tasks

## HTTP API

Создать задачу

```
POST /tasks
Content-Type: application/json

{
  "urls": ["https://example.com/a.jpg", "https://example.com/b.mp4"],
  "dir": "my_batch_001",         // опционально: поддиректория в DOWNLOAD_DIR
  "notes": "задача Саши"         // опционально
}
```

Получить статус задачи

```
GET /tasks/{id}
```

Список задач

```
GET /tasks
```

Пауза/продолжение обработки

```
POST /maintenance/pause
POST /maintenance/resume
```

Отмена задачи

```
POST /task/{id}/cancel
```

Статусы:

	•	Задача: queued | running | paused | completed | failed | canceled
	•	Элемент: queued | running | paused | completed | failed | canceled

