
"""Orders — задания 1 и 2.

Задание 1:
- асинхронное оформление заказа;
- идемпотентная оплата;
- Circuit Breaker для Payment.

Задание 2:
- Transactional Outbox для order.paid;
- восстановление pending-заказов после перезапуска;
- идемпотентная обработка delivery.confirmed;
- бонусы в PostgreSQL.
"""

import queue
import http.client
import threading
import random
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL CHECK (points >= 0)
);

CREATE TABLE IF NOT EXISTS processed_deliveries (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    bonus_points BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
"""

order_queue = queue.Queue()

breaker_lock = threading.Lock()
payment_failures = 0
breaker_open_until = 0.0
breaker_probe_in_progress = False

FAILURES_TO_OPEN = 1
BREAKER_COOLDOWN = 10.0


def db():
    # autocommit=False: операции внутри with db() атомарны.
    return psycopg.connect(DATABASE_URL)


def init_db():
    for attempt in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("База недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("Не удалось подключиться к базе")


def call(method, url, body=None, headers=None, timeout=0.7):
    """HTTP-вызов соседнего сервиса с ограничением ожидания."""
    data = json.dumps(body).encode() if body is not None else None

    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")

    for name, value in (headers or {}).items():
        req.add_header(name, value)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, None


def reserve(order_id, items):
    code, _ = call(
        "POST",
        f"{INVENTORY_URL}/reservations",
        {
            "order_id": order_id,
            "items": [
                {"sku": item["sku"], "qty": item["qty"]}
                for item in items
            ],
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"inventory: {code}")


def release(order_id):
    code, _ = call(
        "DELETE",
        f"{INVENTORY_URL}/reservations/{order_id}",
    )
    if code not in (200, 204, 404):
        log.warning("Не удалось снять резерв %s: HTTP %s", order_id, code)


def wait_for_payment():
    global breaker_probe_in_progress

    with breaker_lock:
        now = time.monotonic()

        if breaker_open_until == 0.0:
            return True

        if now < breaker_open_until:
            return False

        # После паузы разрешаем только один пробный запрос.
        if breaker_probe_in_progress:
            return False

        breaker_probe_in_progress = True
        return True


def payment_result(success):
    global payment_failures
    global breaker_open_until
    global breaker_probe_in_progress

    with breaker_lock:
        # Ответ потерян: исход оплаты неизвестен.
        # Повторяем запрос с прежним ключом идемпотентности.
        if success is None:
            breaker_probe_in_progress = False
            return

        breaker_probe_in_progress = False

        if success:
            payment_failures = 0
            breaker_open_until = 0.0
            return

        payment_failures += 1

        if payment_failures >= FAILURES_TO_OPEN:
            breaker_open_until = time.monotonic() + BREAKER_COOLDOWN
            payment_failures = 0
            log.warning("Payment временно отключён Circuit Breaker")


def charge_once(order_id, amount):
    """Повторные попытки используют один ключ Idempotency-Key."""
    code, _ = call(
        "POST",
        f"{PAYMENT_URL}/payments",
        {
            "order_id": order_id,
            "amount_cents": amount,
            "currency": "RUB",
        },
        headers={"Idempotency-Key": order_id},
        timeout=3.0,
    )

    if code in (200, 201):
        return "paid"
    if code == 402:
        return "declined"
    if code == 500:
        return "retry"

    raise RuntimeError(f"Неопределённый результат Payment: HTTP {code}")


def update_status(order_id, status):
    with db() as conn:
        conn.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            (status, order_id),
        )


def mark_paid_and_enqueue_event(order_id):
    """Сохраняет paid и outbox-событие в одной транзакции."""
    event_id = str(uuid.uuid4())
    payload = {
        "id": event_id,
        "type": "order.paid",
        "order_id": order_id,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }

    with db() as conn:
        conn.execute(
            "UPDATE orders SET status = 'paid' WHERE id = %s",
            (order_id,),
        )
        conn.execute(
            """
            INSERT INTO outbox (event_id, order_id, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (event_id, order_id, json.dumps(payload)),
        )


def process_order(order_id, items, amount):
    job = (order_id, items, amount)

    try:
        reserve(order_id, items)
    except Exception as e:
        log.warning("Резерв %s: %s", order_id, e)
        time.sleep(0.2)
        order_queue.put(job)
        return

    if not wait_for_payment():
        with breaker_lock:
            delay = max(0.2, breaker_open_until - time.monotonic())
        time.sleep(min(delay, 1.0))
        order_queue.put(job)
        return

    try:
        result = charge_once(order_id, amount)

    except http.client.RemoteDisconnected as e:
        payment_result(None)
        log.warning(
            "Ответ Payment потерян для %s; повторяем с тем же ключом: %s",
            order_id,
            e,
        )
        time.sleep(0.1)
        order_queue.put(job)
        return

    except Exception as e:
        payment_result(False)
        log.warning("Оплата %s пока не подтверждена: %s", order_id, e)
        time.sleep(random.uniform(0.1, 0.4))
        order_queue.put(job)
        return

    payment_result(True)

    if result == "retry":
        time.sleep(random.uniform(0.05, 0.2))
        order_queue.put(job)
        return

    if result == "paid":
        # Важно: статус и событие фиксируются одной транзакцией.
        mark_paid_and_enqueue_event(order_id)
        log.info("Заказ %s оплачен; событие сохранено в outbox", order_id)
        return

    # Только однозначный отказ банка приводит к rejected.
    release(order_id)
    update_status(order_id, "rejected")
    log.info("Заказ %s отклонён банком", order_id)


def worker():
    while True:
        job = order_queue.get()
        try:
            process_order(*job)
        except Exception:
            log.exception("Ошибка фоновой обработки заказа")
            # При сбое БД или другом неожиданном исключении не теряем job.
            time.sleep(0.5)
            order_queue.put(job)
        finally:
            order_queue.task_done()


def restore_pending_orders():
    """Восстанавливает очередь из PostgreSQL после перезапуска."""
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, items, amount_cents
            FROM orders
            WHERE status = 'pending'
            ORDER BY created_at
            """
        ).fetchall()

    for order_id, items, amount in rows:
        if isinstance(items, str):
            items = json.loads(items)
        order_queue.put((order_id, items, amount))

    if rows:
        log.info("Восстановлено pending-заказов: %d", len(rows))


def outbox_worker():
    """Публикует сохранённые события и повторяет попытки при сбоях."""
    while True:
        try:
            with db() as conn:
                row = conn.execute(
                    """
                    SELECT event_id, payload
                    FROM outbox
                    WHERE published = FALSE
                    ORDER BY created_at
                    LIMIT 1
                    """
                ).fetchone()

            if row is None:
                time.sleep(0.2)
                continue

            event_id, payload = row
            if isinstance(payload, str):
                payload = json.loads(payload)

            if not EVENTS_URL:
                log.warning("EVENTS_URL не задан; событие %s ожидает", event_id)
                time.sleep(1)
                continue

            code, _ = call(
                "POST",
                f"{EVENTS_URL}/events",
                payload,
                timeout=0.7,
            )

            if code == 202:
                with db() as conn:
                    conn.execute(
                        """
                        UPDATE outbox
                        SET published = TRUE, published_at = now()
                        WHERE event_id = %s
                        """,
                        (event_id,),
                    )
                log.info("Событие %s опубликовано", event_id)
            else:
                log.warning(
                    "Событие %s не опубликовано: HTTP %s",
                    event_id,
                    code,
                )
                time.sleep(0.5)

        except Exception:
            log.exception("Ошибка публикации outbox")
            time.sleep(0.5)


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False

    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False

    for item in items:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("sku"), str) or not item["sku"]:
            return False
        for key, minimum in (("qty", 1), ("price_cents", 0)):
            value = item.get(key)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(
        item["qty"] * item["price_cents"]
        for item in body["items"]
    )

    with db() as conn:
        conn.execute(
            """
            INSERT INTO orders (id, user_id, status, amount_cents, items)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                order_id,
                body["user_id"],
                "pending",
                amount,
                json.dumps(body["items"]),
            ),
        )

    order_queue.put((order_id, body["items"], amount))
    return {"id": order_id, "status": "pending"}


def delivery_confirmed(event):
    """Начисляет бонусы ровно один раз на уникальный ID события."""
    with db() as conn:
        inserted = conn.execute(
            """
            INSERT INTO processed_deliveries (event_id, user_id, bonus_points)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event["id"], event["user_id"], event["bonus_points"]),
        ).fetchone()

        if inserted is not None:
            conn.execute(
                """
                INSERT INTO bonuses (user_id, points)
                VALUES (%s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET points = bonuses.points + EXCLUDED.points
                """,
                (event["user_id"], event["bonus_points"]),
            )


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute(
            "SELECT points FROM bonuses WHERE user_id = %s",
            (user_id,),
        ).fetchone()
    return {"points": row[0] if row else 0}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            """
            SELECT id, status, amount_cents
            FROM orders
            WHERE id = %s
            """,
            (order_id,),
        ).fetchone()

    if row is None:
        return None

    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send_json(200, {"status": "ok"})

        match = ORDER_PATH.match(self.path)
        if match:
            order = get_order(match.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)

        match = BONUS_PATH.match(self.path)
        if match:
            user_id = urllib.parse.unquote(match.group(1))
            return self.send_json(200, get_bonus(user_id))

        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})

        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"null")
        except (ValueError, TypeError):
            body = None

        if self.path == "/webhooks/delivery":
            return self.webhook(body)

        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})

        try:
            result = create_order(body)
            self.send_json(201, result)
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("id"), str)
            or not body["id"]
            or body.get("type") != "delivery.confirmed"
            or not isinstance(body.get("user_id"), str)
            or not body["user_id"]
            or not isinstance(body.get("bonus_points"), int)
            or isinstance(body.get("bonus_points"), bool)
            or body["bonus_points"] <= 0
        ):
            return self.send_json(400, {"error": "bad_request"})

        try:
            delivery_confirmed(body)
        except Exception:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal"})

        # Повтор уже обработанного события тоже подтверждаем 2xx.
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    restore_pending_orders()

    for _ in range(4):
        threading.Thread(target=worker, daemon=True).start()

    threading.Thread(target=outbox_worker, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True

    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
