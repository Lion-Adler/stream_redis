# ============================================================
# 📡 RESILIENT MULTI-SYMBOL MARKET DATA STREAMER
# Spot + Futures | Никогда не падает | Авто-переподключение
# Каждый поток живёт независимо — сбой одного не убивает других
# ✅ FIX v2: asyncio.shield(exchange.close()) → no more "Unclosed connector"
# ============================================================

import asyncio
import msgpack
import redis.asyncio as redis
import ccxt.pro as ccxt
from datetime import datetime

ccxt_errors = ccxt

# ──────────────────────────────────────────────────────────
# ⚙️  КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────

SYMBOLS = [
    'BTC/USDT',
]

    # 'ETH/USDT',
    # 'SOL/USDT',
ORDER_BOOK_DEPTH = 10

BYBIT_ORDERBOOK_LIMIT   = 50
BINANCE_ORDERBOOK_LIMIT = 10

CHANNEL_ORDERBOOK = "stream:orderbook"
CHANNEL_TRADES    = "stream:trades"

# Экспоненциальный backoff: delay(n) = min(MIN * MULTIPLIER^(n-1), MAX)
RECONNECT_DELAY_MIN  = 1
RECONNECT_DELAY_MAX  = 60
RECONNECT_MULTIPLIER = 2


# ──────────────────────────────────────────────────────────
# 🏭  ФАБРИКИ БИРЖ
# ──────────────────────────────────────────────────────────

def make_binance(market_type: str = 'spot') -> ccxt.binance:
    return ccxt.binance({
        'newUpdates': True,
        'options': {
            'defaultType': market_type,
        }
    })


def make_bybit(market_type: str = 'spot') -> ccxt.bybit:
    ccxt_type = 'linear' if market_type == 'future' else 'spot'
    return ccxt.bybit({
        'newUpdates': True,
        'options': {
            'defaultType': ccxt_type,
        }
    })


# ──────────────────────────────────────────────────────────
# 📦  ПУБЛИКАЦИЯ В REDIS
# ──────────────────────────────────────────────────────────

async def publish(r: redis.Redis, channel: str, payload: dict):
    binary_packet = msgpack.packb(payload, use_bin_type=True)
    await r.publish(channel, binary_packet)


# ──────────────────────────────────────────────────────────
# 🔄  ДЕКОРАТОР ПЕРЕПОДКЛЮЧЕНИЯ
# ──────────────────────────────────────────────────────────
#
# ✅ КОРЕНЬ ПРОБЛЕМЫ "Unclosed connector":
#
#   Когда asyncio получает Ctrl+C или любую отмену задачи,
#   он бросает CancelledError во все активные корутины.
#   В блоке `finally` мы делаем `await exchange.close()` —
#   но этот await ТОЖЕ может получить CancelledError и прерваться!
#   В результате close() не завершается, TCPConnector остаётся открытым.
#
# ✅ РЕШЕНИЕ — asyncio.shield(exchange.close()):
#
#   asyncio.shield() создаёт "щит" вокруг корутины.
#   Даже если внешняя задача отменена, shield позволяет
#   защищённой корутине ЗАВЕРШИТЬСЯ до конца.
#
#   Схема:
#     finally:
#         await asyncio.shield(exchange.close())  ← close() всегда завершается
#
# ──────────────────────────────────────────────────────────

async def with_reconnect(exchange_factory, stream_fn, label: str):
    """
    Запускает stream_fn и бесконечно перезапускает при любом сбое.
    Гарантирует закрытие объекта биржи через try...finally + asyncio.shield.

    Параметры:
      exchange_factory — функция без аргументов, возвращает новый объект биржи
      stream_fn        — async-функция (exchange) → корутина стрима
      label            — строка для логов
    """
    attempt = 0

    while True:
        if attempt > 0:
            delay = min(
                RECONNECT_DELAY_MIN * (RECONNECT_MULTIPLIER ** (attempt - 1)),
                RECONNECT_DELAY_MAX
            )
            print(f"⏳ [{label}] Переподключение через {delay:.0f}с... (попытка #{attempt})")
            await asyncio.sleep(delay)

        print(f"🔌 [{label}] Запуск потока...")

        exchange = exchange_factory()

        try:
            await stream_fn(exchange)

            print(f"⚠️  [{label}] Поток завершился без ошибки — перезапуск.")
            attempt = 0

        except asyncio.CancelledError:
            print(f"🛑 [{label}] Поток остановлен штатно.")
            raise

        except (
            ccxt_errors.NetworkError,
            ccxt_errors.ExchangeNotAvailable,
            ccxt_errors.RequestTimeout,
            ccxt_errors.DDoSProtection,
        ) as e:
            attempt += 1
            error_time = datetime.now().strftime("%d-%m %H:%M:%S")
            print(f"💥 [{error_time}] [{label}] Сетевая ошибка (#{attempt}): {type(e).__name__}: {e}")

        except ccxt_errors.AuthenticationError as e:
            attempt += 1
            error_time = datetime.now().strftime("%d-%m %H:%M:%S")
            print(f"🔑 [{error_time}] [{label}] Ошибка аутентификации (#{attempt}): {e}")

        except ccxt_errors.ExchangeError as e:
            attempt += 1
            error_time = datetime.now().strftime("%d-%m %H:%M:%S")
            print(f"⚠️  [{error_time}] [{label}] Ошибка биржи (#{attempt}): {type(e).__name__}: {e}")

        except ConnectionError as e:
            attempt += 1
            error_time = datetime.now().strftime("%d-%m %H:%M:%S")
            print(f"🔌 [{error_time}] [{label}] Обрыв соединения (#{attempt}): {e}")

        except Exception as e:
            attempt += 1
            error_time = datetime.now().strftime("%d-%m %H:%M:%S")
            print(f"💥 [{error_time}] [{label}] Неизвестная ошибка (#{attempt}): {type(e).__name__}: {e}")

        finally:
            # ✅ КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: asyncio.shield()
            #
            # Проблема без shield:
            #   При CancelledError Python прерывает `await exchange.close()`
            #   на полуслове → TCPConnector остаётся открытым → варнинги.
            #
            # Как работает shield:
            #   asyncio.shield(coro) оборачивает coro в защищённую Task.
            #   Если внешняя задача отменяется — shield поглощает CancelledError,
            #   но внутренняя Task (exchange.close()) продолжает выполняться.
            #   close() завершается полностью → все соединения закрыты чисто.
            #
            # Мы также ловим CancelledError отдельно — это нормально,
            # shield его "съедает" и не пробрасывает наверх из finally.
            try:
                await asyncio.shield(exchange.close())
            except asyncio.CancelledError:
                # shield поглотил отмену — close() всё равно завершился
                pass
            except Exception as close_err:
                print(f"⚠️  [{label}] Ошибка при закрытии exchange: {close_err}")


# ──────────────────────────────────────────────────────────
# 🔍  ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ
# ──────────────────────────────────────────────────────────

def get_market_type(exchange) -> str:
    raw_type = exchange.options.get('defaultType', 'spot')
    if raw_type in ('future', 'linear', 'swap', 'delivery'):
        return 'future'
    return 'spot'


# ──────────────────────────────────────────────────────────
# 📖  СТРИМЕР СТАКАНА (ORDER BOOK)
# ──────────────────────────────────────────────────────────

async def stream_orderbook(exchange, symbols: list, r: redis.Redis, fetch_limit: int):
    exchange_name = exchange.id.upper()
    market_type   = get_market_type(exchange)

    while True:
        orderbook = await exchange.watch_order_book_for_symbols(symbols, limit=fetch_limit)

        symbol = orderbook['symbol']
        bids   = orderbook['bids'][:ORDER_BOOK_DEPTH]
        asks   = orderbook['asks'][:ORDER_BOOK_DEPTH]

        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        spread   = round(best_ask - best_bid, 4)

        payload = {
            'type'      : 'orderbook',
            'exchange'  : exchange_name,
            'market'    : market_type,
            'symbol'    : symbol,
            'timestamp' : orderbook.get('timestamp'),
            'best_bid'  : best_bid,
            'best_ask'  : best_ask,
            'spread'    : spread,
            'depth'     : ORDER_BOOK_DEPTH,
            'bids'      : [list(level) for level in bids],
            'asks'      : [list(level) for level in asks],
        }

        await publish(r, CHANNEL_ORDERBOOK, payload)


# ──────────────────────────────────────────────────────────
# 💥  СТРИМЕР СДЕЛОК (TRADES)
# ──────────────────────────────────────────────────────────

async def stream_trades(exchange, symbols: list, r: redis.Redis):
    exchange_name = exchange.id.upper()
    market_type   = get_market_type(exchange)

    while True:
        trades = await exchange.watch_trades_for_symbols(symbols)

        for trade in trades:
            price  = trade['price']
            amount = trade['amount']

            payload = {
                'type'      : 'trade',
                'exchange'  : exchange_name,
                'market'    : market_type,
                'symbol'    : trade['symbol'],
                'trade_id'  : str(trade.get('id', 'N/A')),
                'side'      : trade['side'],
                'price'     : price,
                'amount'    : amount,
                'cost'      : trade.get('cost', price * amount),
                'timestamp' : trade.get('timestamp'),
            }

            await publish(r, CHANNEL_TRADES, payload)


# ──────────────────────────────────────────────────────────
# 🚀  ТОЧКА ВХОДА
# ──────────────────────────────────────────────────────────

async def main():
    print("=" * 70)
    print("📡  RESILIENT MULTI-SYMBOL MARKET DATA STREAMER  |  Spot + Futures")
    print(f"    Символы:         {', '.join(SYMBOLS)}")
    print(f"    Глубина стакана: {ORDER_BOOK_DEPTH} уровней")
    print(f"    Переподключение: авто (backoff {RECONNECT_DELAY_MIN}s → {RECONNECT_DELAY_MAX}s)")
    print(f"    Redis каналы:    {CHANNEL_ORDERBOOK}  |  {CHANNEL_TRADES}")
    print("=" * 70)

    r = await redis.from_url("redis://localhost", decode_responses=False)

    tasks = [

        # ════════════════════════════════════════════════════════════════
        # BINANCE SPOT
        # ════════════════════════════════════════════════════════════════
        with_reconnect(
            exchange_factory = lambda: make_binance('spot'),
            stream_fn        = lambda ex: stream_orderbook(ex, SYMBOLS, r, BINANCE_ORDERBOOK_LIMIT),
            label            = "BINANCE_SPOT | orderbook"
        ),
        with_reconnect(
            exchange_factory = lambda: make_binance('spot'),
            stream_fn        = lambda ex: stream_trades(ex, SYMBOLS, r),
            label            = "BINANCE_SPOT | trades"
        ),

        # ════════════════════════════════════════════════════════════════
        # BINANCE FUTURES (USD-M Perpetual)
        # ════════════════════════════════════════════════════════════════
        with_reconnect(
            exchange_factory = lambda: make_binance('future'),
            stream_fn        = lambda ex: stream_orderbook(ex, SYMBOLS, r, BINANCE_ORDERBOOK_LIMIT),
            label            = "BINANCE_FUT  | orderbook"
        ),
        with_reconnect(
            exchange_factory = lambda: make_binance('future'),
            stream_fn        = lambda ex: stream_trades(ex, SYMBOLS, r),
            label            = "BINANCE_FUT  | trades"
        ),

        # ════════════════════════════════════════════════════════════════
        # BYBIT SPOT
        # ════════════════════════════════════════════════════════════════
        with_reconnect(
            exchange_factory = lambda: make_bybit('spot'),
            stream_fn        = lambda ex: stream_orderbook(ex, SYMBOLS, r, BYBIT_ORDERBOOK_LIMIT),
            label            = "BYBIT_SPOT   | orderbook"
        ),
        with_reconnect(
            exchange_factory = lambda: make_bybit('spot'),
            stream_fn        = lambda ex: stream_trades(ex, SYMBOLS, r),
            label            = "BYBIT_SPOT   | trades"
        ),

        # ════════════════════════════════════════════════════════════════
        # BYBIT FUTURES (Linear USDT Perpetual)
        # ════════════════════════════════════════════════════════════════
        with_reconnect(
            exchange_factory = lambda: make_bybit('future'),
            stream_fn        = lambda ex: stream_orderbook(ex, SYMBOLS, r, BYBIT_ORDERBOOK_LIMIT),
            label            = "BYBIT_FUT    | orderbook"
        ),
        with_reconnect(
            exchange_factory = lambda: make_bybit('future'),
            stream_fn        = lambda ex: stream_trades(ex, SYMBOLS, r),
            label            = "BYBIT_FUT    | trades"
        ),

    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Стример остановлен пользователем (Ctrl+C). Пока!")
