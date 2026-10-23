"""Orders — наивная реализация.

Оформление заказа: зарезервировать товар в Inventory, списать деньги в
Payment, записать заказ в базу. Пока Payment и Inventory работают без отказов, всё работает.

Задание 2 (необязательное): об оплаченном заказе сервис сообщает в шину
событий (order.paid), а за подтверждённую доставку начисляет покупателю
бонусы. Пока шина работает и Orders не падает, тоже всё работает.
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
# Шина событий; пусто — события не публикуются.
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
    points  BIGINT NOT NULL
)
"""
# Очередь заказов, которые нужно обработать в фоне.
order_queue = queue.Queue()

# Состояние защиты от отказов Payment.
breaker_lock = threading.Lock()
payment_failures = 0
breaker_open_until = 0.0
breaker_probe_in_progress = False

FAILURES_TO_OPEN = 1
BREAKER_COOLDOWN = 10.0

def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for attempt in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


def call(method, url, body=None, headers=None, timeout=0.7):
    """HTTP-вызов соседнего сервиса с ограничением ожидания."""
    data = json.dumps(body).encode() if body is not None else None

    req = urllib.request.Request(
        url,
        data=data,
        method=method,
    )
    req.add_header("Content-Type", "application/json")

    for name, value in (headers or {}).items():
        req.add_header(name, value)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None

    except urllib.error.HTTPError as e:
        return e.code, None


class PaymentError(Exception):
    pass


def reserve(order_id, items):
    code, _ = call("POST", f"{INVENTORY_URL}/reservations", {
        "order_id": order_id,
        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
    })
    if code not in (200, 201):
        raise RuntimeError(f"inventory: {code}")


def release(order_id):
    call("DELETE", f"{INVENTORY_URL}/reservations/{order_id}")

def wait_for_payment():
    global breaker_probe_in_progress

    with breaker_lock:
        now = time.monotonic()

        # Circuit Breaker закрыт: запросы разрешены.
        if breaker_open_until == 0.0:
            return True

        # Пауза после отказа ещё не закончилась.
        if now < breaker_open_until:
            return False

        # После паузы пропускаем только один пробный запрос.
        if breaker_probe_in_progress:
            return False

        breaker_probe_in_progress = True
        return True


def payment_result(success):
    global payment_failures
    global breaker_open_until
    global breaker_probe_in_progress

    with breaker_lock:
        # Ответ потерян: не считаем это доказательством
        # отказа банка, но освобождаем пробную попытку.
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
            breaker_open_until = (
                time.monotonic() + BREAKER_COOLDOWN
            )
            payment_failures = 0
            log.warning(
                "Payment временно отключён Circuit Breaker"
            )


def update_status(order_id, status):
    """Сохраняем новый статус заказа."""
    with db() as conn:
        conn.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            (status, order_id),
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
        # Не отправляем запросы банку, пока Circuit Breaker открыт.
        with breaker_lock:
            delay = max(0.2, breaker_open_until - time.monotonic())

        time.sleep(min(delay, 1.0))
        order_queue.put(job)
        return

    try:
        result = charge_once(order_id, amount)

    except http.client.RemoteDisconnected as e:
        # Payment мог списать деньги, но потерял ответ.
        # Повторяем запрос с тем же Idempotency-Key.
        # Circuit Breaker не открываем: это не доказывает,
        # что сам Payment недоступен.
        payment_result(None)
        log.warning(
            "Ответ Payment потерян для %s; повторяем "
            "с тем же ключом идемпотентности: %s",
            order_id,
            e,
        )
        time.sleep(0.1)
        order_queue.put(job)
        return

    except Exception as e:
        # Например, тайм-аут при недоступном Payment.
        # Здесь сохраняем защиту Circuit Breaker для S4.
        payment_result(False)
        log.warning(
            "Оплата %s пока не подтверждена: %s",
            order_id,
            e,
        )
        time.sleep(random.uniform(0.1, 0.4))
        order_queue.put(job)
        return

    # Получен HTTP-ответ от Payment.
    payment_result(True)

    if result == "retry":
        time.sleep(random.uniform(0.05, 0.2))
        order_queue.put(job)
        return

    if result == "paid":
        update_status(order_id, "paid")
        log.info("Заказ %s оплачен", order_id)
        return

    # Отклоняем заказ только при однозначном отказе банка.
    release(order_id)
    update_status(order_id, "rejected")
    log.info("Заказ %s отклонён банком", order_id)

def worker():
    """Постоянно обрабатывает очередь заказов."""
    while True:
        job = order_queue.get()

        try:
            process_order(*job)
        except Exception:
            log.exception("Ошибка фоновой обработки заказа")
        finally:
            order_queue.task_done()
def charge_once(order_id, amount):
    """Одна попытка оплаты с постоянным ключом идемпотентности."""
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
        # Банк ответил быстро, но операция не выполнена.
        # Это повод повторить оплату, а не ждать 8 секунд.
        return "retry"

    # Тайм-аут и другие неопределённые результаты:
    # возможно, банк уже выполнил операцию.
    raise PaymentError(f"payment: {code}")


def charge(order_id, amount):
    """Оставляем совместимый интерфейс для фонового обработчика."""
    return charge_once(order_id, amount)


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(
        item["qty"] * item["price_cents"]
        for item in body["items"]
    )

    # Сначала сохраняем заказ в БД.
    with db() as conn:
        conn.execute(
            """
            INSERT INTO orders
                (id, user_id, status, amount_cents, items)
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

    # HTTP-обработчик только ставит работу в очередь.
    order_queue.put((order_id, body["items"], amount))

    return {"id": order_id, "status": "pending"}


def publish_paid(order_id):
    """Сообщает в шину, что заказ оплачен."""
    if not EVENTS_URL:
        return
    try:
        code, _ = call("POST", f"{EVENTS_URL}/events", {
            "id": str(uuid.uuid4()),
            "type": "order.paid",
            "order_id": order_id,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        if code != 202:
            log.warning("order %s: событие не опубликовано (%s)", order_id, code)
    except OSError as e:
        log.warning("order %s: событие не опубликовано (%s)", order_id, e)


def delivery_confirmed(event):
    """Начисляет бонусы за подтверждённую доставку."""
    with db() as conn:
        conn.execute(
            "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
            (event["user_id"], event["bonus_points"]),
        )


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
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
        m = ORDER_PATH.match(self.path)
        if m:
            order = get_order(m.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        m = BONUS_PATH.match(self.path)
        if m:
            return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if not isinstance(body, dict) or not isinstance(body.get("user_id"), str) \
                or not isinstance(body.get("bonus_points"), int) or body["bonus_points"] <= 0:
            return self.send_json(400, {"error": "bad_request"})
        try:
            delivery_confirmed(body)
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()

    # Поднимаем работников до начала приёма запросов.
    for _ in range(4):
        thread = threading.Thread(
            target=worker,
            daemon=True,
        )
        thread.start()

    server = ThreadingHTTPServer(
        ("0.0.0.0", PORT),
        Handler,
    )
    server.daemon_threads = True

    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
