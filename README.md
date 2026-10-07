# Асинхронный сервис процессинга платежей

Микросервис принимает платёж, сохраняет его вместе с событием outbox и отвечает `202 Accepted`. Отдельный процесс публикует событие в RabbitMQ. Consumer эмулирует платёжный шлюз, обновляет статус и отправляет webhook.

## Стек

FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ через FastStream, Alembic, Docker Compose. Python 3.12.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

API: `http://localhost:8000`. Ключ по умолчанию: `dev-api-key` (переменная `API_KEY`).

RabbitMQ management: `http://localhost:15672` (`guest` / `guest`).

Миграции выполняются при старте `api` и `consumer`.

## Примеры

Создать платёж:

```bash
curl -s -D - -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-15" \
  -d '{
    "amount": "10.50",
    "currency": "RUB",
    "description": "Заказ 15",
    "metadata": {"order_id": "15"},
    "webhook_url": "https://merchant.example/hooks/payment"
  }'
```

Ответ `202`:

```json
{"payment_id": "…", "status": "pending", "created_at": "…"}
```

Получить платёж:

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key"
```

Повтор с тем же `Idempotency-Key` и тем же телом возвращает исходный платёж. Другое тело с тем же ключом — `409`.

## Как устроена доставка

1. `POST` пишет `payments` и `outbox` в одной транзакции. В брокер в этот момент ничего не уходит.
2. Фоновая задача в процессе API читает неопубликованные строки outbox и публикует их в exchange `payments` с ключом `payments.new`. Строка помечается опубликованной только после успешной отправки.
3. Consumer обрабатывает очередь `payments.new`:
   - шлюз отвечает 2–5 секунд, около 90% успех (`succeeded`) и 10% отказ (`failed`);
   - отказ шлюза — завершённый платёж, сообщение подтверждается;
   - webhook отправляется до 3 раз с экспоненциальной паузой;
   - если обработчик не смог завершиться, сообщение уходит в `payments.new.retry` (пауза 1с, 2с) и после 3-й неудачи — в `payments.new.dlq`.

Повторная доставка не списывает платёж ещё раз и не шлёт webhook повторно, если он уже ушёл.

## Тесты

Нужны PostgreSQL и RabbitMQ. Через Compose:

```bash
docker compose up -d postgres rabbitmq
```

Локально без Docker подойдут те же учётные данные. Для тестов — отдельная база `payments_test`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
export DATABASE_URL=postgresql+asyncpg://payments:payments@127.0.0.1:5432/payments_test
export RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
export API_KEY=test-api-key
pytest
```

`conftest.py` сам выставляет эти переменные и накатывает миграции на `payments_test`.

## Решения, которых нет в формулировке задания

- Тот же idempotency key и то же тело — повтор `202`. Другое тело — `409`.
- Отказ шлюза (10%) ставит статус `failed` и шлёт webhook. В DLQ попадают сбои обработки, а не отказ эмитента.
- Колонка `webhook_sent_at` нужна, чтобы повтор сообщения дослал webhook и не вызвал шлюз второй раз.
- Задержка шлюза и доля успеха настраиваются переменными окружения. В проде значения по умолчанию — 2–5 секунд и 0.9.
- Relay outbox живёт в процессе API, поэтому в Compose четыре сервиса: `postgres`, `rabbitmq`, `api`, `consumer`.
