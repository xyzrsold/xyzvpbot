# -*- coding: utf-8 -*-
"""
БОТ ДЛЯ ВЗАИМНОГО ПИАРА (ВП) - ОДИН БОТ НА НЕСКОЛЬКО КАНАЛОВ

Как работает (для каждого канала отдельно, со своими настройками):
  1. Впшеры кидают посты в рабочую группу ЭТОГО канала.
  2. До начала рабочего времени бот только собирает посты и предлагает автору выбрать время
     (кнопками или ответом на пост: "23:40").
  3. В начале рабочего времени бот расставляет время всем постам без времени (с шагом канала)
     и публикует их в канал строго по расписанию. Отложку Telegram НЕ использует.
  4. Смена = посты, отправленные от конца одной смены до конца следующей. Новая смена - новая пачка.
  5. Одинаковые по тексту посты на одной смене бот отклоняет (антидубль).
  6. Через заданное число часов после публикации бот удаляет свои посты из канала.

Кто есть кто:
  * ГЛАВНЫЙ ВЛАДЕЛЕЦ (SUPER_OWNER_ID в настройках ниже) - скрытый. Имеет полный доступ ко всем
    каналам, нигде в списках и сообщениях не показывается. Добавляет каналы, меняет их настройки,
    назначает владельцев каналов.
  * ВЛАДЕЛЕЦ КАНАЛА - назначается главным владельцем. Видит свой канал, добавляет и убирает админов.
  * АДМИН КАНАЛА - видит статистику своего канала, может менять и отменять любые посты своего канала.

Всё управление - в личке бота, команда /console.

Запуск:  pip install "aiogram>=3.7"   затем   python vp_bot_multi.py
"""

import asyncio
import hashlib
import html
import logging
import re
import sqlite3
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ContentType, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# =====================================================================
#  НАСТРОЙКИ - ЗАПОЛНИТЕ ЭТИ СТРОКИ (текст в кавычках, числа без кавычек)
# =====================================================================
BOT_TOKEN = ""                       # токен от @BotFather
SUPER_OWNER_ID = 5265325991          # ваш ID (узнать командой /id). Скрытый главный владелец
DB_FILE = "vp_bot_multi.db"          # файл с базой (создастся сам)
UTC_OFFSET_HOURS = 3                 # часовой пояс всех каналов: 3 = московское время

# --- значения для НОВЫХ каналов (потом меняются в консоли для каждого канала отдельно) ---
DEFAULT_WORK_START = "22:15"         # первый пост смены
DEFAULT_WORK_END = "09:50"           # последний возможный пост смены (смена закрывается на минуту позже)
DEFAULT_STEP_MIN = 5                 # интервал между постами (кд), минут
DEFAULT_OVERFLOW_TIME = "09:55"      # если слоты кончились, остаток уходит пачкой в это время
DEFAULT_DELETE_AFTER_H = 24          # через сколько часов бот удаляет пост из канала (0 = не удалять, максимум 47)

# --- служебные настройки (обычно трогать не нужно) ---
TICK_SECONDS = 10                    # как часто бот проверяет, не пора ли публиковать
MISSED_GRACE_MIN = 10                # если бот проспал время поста дольше - пост переставится на свободное время
OVERFLOW_GRACE_MIN = 60              # сколько после времени пачки бот ещё дожимает её, потом смена закрывается
SEND_LIMIT_PER_SEC = 20              # общий лимит бота на отправку (у Telegram около 30 в секунду на всех)
CONSOLE_LINK = "openmessage"         # вид ссылки на ники в консоли: "openmessage" или "user"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
router = Router()
MSK = timezone(timedelta(hours=UTC_OFFSET_HOURS))

# =====================================================================
#  БАЗА ДАННЫХ (SQLite - простой файл, ничего устанавливать не нужно)
# =====================================================================
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row


def q(sql, args=()):
    """Прочитать данные из базы."""
    return conn.execute(sql, args).fetchall()


def run(sql, args=()):
    """Записать данные в базу."""
    cur = conn.execute(sql, args)
    conn.commit()
    return cur


def init_db():
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS channels (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT NOT NULL,        -- название канала (для консоли и статистики)
            channel_chat   INTEGER NOT NULL,     -- ID канала, куда бот публикует
            group_id       INTEGER NOT NULL UNIQUE,  -- ID рабочей группы впшеров этого канала
            owner_id       INTEGER,              -- владелец канала (назначает главный владелец)
            owner_name     TEXT,
            work_start     TEXT NOT NULL,        -- 'ЧЧ:ММ' - первый пост смены
            work_end       TEXT NOT NULL,        -- 'ЧЧ:ММ' - последний возможный пост смены
            step_min       INTEGER NOT NULL,     -- шаг между постами, минут
            overflow_time  TEXT NOT NULL,        -- время пачки для постов, которым не хватило слотов
            delete_after_h INTEGER NOT NULL DEFAULT 0,  -- 0 = не удалять
            active         INTEGER NOT NULL DEFAULT 1,  -- 0 = пауза
            work_days      TEXT NOT NULL DEFAULT '0123456',  -- дни недели начала смены: 0=Пн ... 6=Вс
            created_at     INTEGER
        );
        CREATE TABLE IF NOT EXISTS channel_admins (
            channel_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            name       TEXT,
            added_at   INTEGER,
            PRIMARY KEY (channel_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS posts (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id     INTEGER NOT NULL,
            shift          TEXT NOT NULL,      -- дата смены (день, в который смена началась)
            chat_id        INTEGER NOT NULL,   -- рабочая группа
            author_id      INTEGER,
            author_name    TEXT,
            media_group_id TEXT,               -- для постов-альбомов (несколько фото)
            created_at     INTEGER,
            scheduled_at   INTEGER,            -- время публикации (unix), пусто = ещё не назначено
            manual         INTEGER DEFAULT 0,  -- 1 = время выбрал сам автор
            status         TEXT DEFAULT 'pending',  -- pending / sent / failed / cancelled / expired / duplicate
            prompt_msg_id  INTEGER,            -- сообщение бота с кнопками выбора времени
            sent_at        INTEGER,
            text_key       TEXT,               -- отпечаток текста поста (антидубль)
            fwd            INTEGER DEFAULT 0,
            out_chat       INTEGER,            -- в какой канал реально ушёл пост
            channel_msgs   TEXT,               -- id сообщений поста в канале (через запятую)
            deleted        INTEGER DEFAULT 0   -- 1 = удалён из канала, 2 = удалить не удалось
        );
        CREATE TABLE IF NOT EXISTS post_messages (
            post_id    INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            UNIQUE (post_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS shifts (
            channel_id INTEGER NOT NULL,
            shift      TEXT NOT NULL,
            started    INTEGER DEFAULT 0,      -- 1 = рабочее время уже началось, расписание объявлено
            closed     INTEGER DEFAULT 0,      -- 1 = смена завершена, итог отправлен
            PRIMARY KEY (channel_id, shift)
        );
        CREATE TABLE IF NOT EXISTS shift_stats (
            channel_id INTEGER NOT NULL,
            shift      TEXT NOT NULL,
            vp_count   INTEGER NOT NULL DEFAULT 0,  -- сколько ВП (опубликованных постов) было за смену
            PRIMARY KEY (channel_id, shift)
        );
        CREATE INDEX IF NOT EXISTS idx_posts_ch_shift ON posts (channel_id, shift, status);
        CREATE INDEX IF NOT EXISTS idx_posts_dedupe ON posts (channel_id, shift, text_key);
        CREATE INDEX IF NOT EXISTS idx_posts_delete ON posts (channel_id, status, deleted, sent_at);
        CREATE INDEX IF NOT EXISTS idx_posts_chat_msg ON post_messages (message_id);
        """
    )
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(channels)")}
    if "work_days" not in cols:  # база, созданная до появления дней недели
        conn.execute("ALTER TABLE channels ADD COLUMN work_days TEXT NOT NULL DEFAULT '0123456'")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()


# =====================================================================
#  КАНАЛЫ И РОЛИ
# =====================================================================
def get_channel(cid):
    rows = q("SELECT * FROM channels WHERE id=?", (cid,))
    return rows[0] if rows else None


def channel_by_group(group_id):
    """Канал, к которому привязана эта рабочая группа (только если канал не на паузе)."""
    rows = q("SELECT * FROM channels WHERE group_id=? AND active=1", (group_id,))
    return rows[0] if rows else None


def is_super(user_id):
    """Главный (скрытый) владелец."""
    return SUPER_OWNER_ID != 0 and user_id == SUPER_OWNER_ID


def is_ch_owner(user_id, ch):
    return ch["owner_id"] is not None and ch["owner_id"] == user_id


def is_ch_admin(user_id, ch):
    return bool(q("SELECT 1 FROM channel_admins WHERE channel_id=? AND user_id=?", (ch["id"], user_id)))


def has_access(user_id, ch):
    """Доступ к каналу: главный владелец, владелец канала или админ канала."""
    return is_super(user_id) or is_ch_owner(user_id, ch) or is_ch_admin(user_id, ch)


def can_manage_admins(user_id, ch):
    """Добавлять и убирать админов: главный владелец или владелец канала."""
    return is_super(user_id) or is_ch_owner(user_id, ch)


def accessible_channels(user_id):
    if is_super(user_id):
        return q("SELECT * FROM channels ORDER BY id")
    return q(
        "SELECT c.* FROM channels c WHERE c.owner_id=? OR EXISTS "
        "(SELECT 1 FROM channel_admins a WHERE a.channel_id=c.id AND a.user_id=?) ORDER BY c.id",
        (user_id, user_id),
    )


def can_edit(user_id, post):
    """Менять/отменять пост: автор, главный владелец, владелец или админ этого канала."""
    if user_id == post["author_id"]:
        return True
    ch = get_channel(post["channel_id"])
    return bool(ch) and has_access(user_id, ch)


# Ссылка tg://user?id=... в личке с ботом Telegram показывает ссылкой только для тех, кто хоть раз
# писал боту или нажимал его кнопку. В консоли (личка) ники читает админ, а впшеры могли никогда не
# писать боту - поэтому там ссылка другого вида (см. CONSOLE_LINK).
def mention(user_id, name, scheme="user"):
    """Кликабельный ник (HTML). ID в тексте не показывается."""
    label = html.escape((name or "").strip() or "впшер")
    if scheme == "openmessage":
        return f'<a href="tg://openmessage?user_id={user_id}">{label}</a>'
    return f'<a href="tg://user?id={user_id}">{label}</a>'


def post_author(post):
    """Подпись автора поста: кликабельный ник по ID."""
    if post["author_id"] is None:
        return html.escape(post["author_name"] or "впшер")
    return mention(post["author_id"], post["author_name"])


def get_post(pid):
    rows = q("SELECT * FROM posts WHERE id=?", (pid,))
    return rows[0] if rows else None


# =====================================================================
#  РАБОТА СО ВРЕМЕНЕМ И СМЕНАМИ (у каждого канала свои часы)
# =====================================================================
def now_msk():
    return datetime.now(MSK)


def hhmm(text):
    h, m = text.split(":")
    return dtime(int(h), int(m))


def parse_hhmm(text):
    m = re.fullmatch(r"\s*([01]?\d|2[0-3])[:.]([0-5]\d)\s*", text or "")
    return dtime(int(m.group(1)), int(m.group(2))) if m else None


def t_start(ch):
    return hhmm(ch["work_start"])


def t_end(ch):
    return hhmm(ch["work_end"])


def t_border(ch):
    """Граница смен: минута после конца работы (09:50 -> 09:51)."""
    e = t_end(ch)
    n = e.hour * 60 + e.minute + 1
    return dtime(n // 60 % 24, n % 60)


def t_over(ch):
    return hhmm(ch["overflow_time"])


def check_settings(start, end, step, over):
    """Проверка времени канала. Возвращает текст ошибки или None."""
    if not (1 <= step <= 180):
        return "Шаг должен быть от 1 до 180 минут."
    n = end.hour * 60 + end.minute + 1
    border = dtime(n // 60 % 24, n % 60)
    if not (end < start and border <= start):
        return ("Рабочее окно должно переходить через полночь: начало (вечер) позже конца (утро), "
                "например 22:15 - 09:50.")
    if not (border <= over < start):
        return f"Время пачки должно быть между {border:%H:%M} и {start:%H:%M} (после конца работы)."
    return None


WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def is_work_day(shift, ch):
    """Работает ли канал в смену, которая началась в этот день (по дню недели начала смены)."""
    return str(shift.weekday()) in ch["work_days"]


def days_text(ch):
    if len(ch["work_days"]) == 7:
        return "каждый день"
    return ", ".join(WEEKDAYS[int(d)] for d in sorted(ch["work_days"]))


def shift_of(dt, ch):
    """
    ВАЖНАЯ ФУНКЦИЯ: к какой смене канала относится момент времени.
    Смена заканчивается на границе (конец работы + 1 минута). Возвращает дату начала смены.
    Пример (22:15-09:50): 05.03 в 23:00 -> смена 05.03; 06.03 в 02:00 -> тоже 05.03; 06.03 в 09:51 -> 06.03.
    """
    border = datetime.combine(dt.date(), t_border(ch), tzinfo=MSK)
    return dt.date() if dt >= border else dt.date() - timedelta(days=1)


def window_bounds(shift, ch):
    """Рабочее окно смены: от начала работы до границы следующего дня (граница не включается)."""
    start = datetime.combine(shift, t_start(ch), tzinfo=MSK)
    end_excl = datetime.combine(shift + timedelta(days=1), t_border(ch), tzinfo=MSK)
    return start, end_excl


def all_slots(shift, ch):
    """Все возможные времена публикации смены: начало, +шаг, +шаг ... до конца работы."""
    start, end_excl = window_bounds(shift, ch)
    step = timedelta(minutes=ch["step_min"])
    slots, t = [], start
    while t < end_excl:
        slots.append(t)
        t += step
    return slots


def shift_state(shift, now, ch):
    """'before' - рабочее время ещё не началось, 'active' - идёт, 'closed' - закончилось."""
    start, end_excl = window_bounds(shift, ch)
    if now < start:
        return "before"
    if now < end_excl:
        return "active"
    return "closed"


def fmt_ts(ts):
    return datetime.fromtimestamp(ts, MSK).strftime("%H:%M")


def shift_title(shift):
    return f"ночь {shift:%d.%m} → {shift + timedelta(days=1):%d.%m}"


def week_start(shift, offset=0):
    """
    Понедельник недели, в которую входит смена. offset=0 - эта неделя, 1 - прошлая.
    Неделя = смены с понедельника по воскресенье (по дню начала смены).
    """
    return shift - timedelta(days=shift.weekday()) - timedelta(days=7 * offset)


# =====================================================================
#  РАССТАНОВКА ВРЕМЕНИ ДЛЯ ПОСТОВ
# =====================================================================
def free_slots(shift, now, ch, exclude_post=None):
    """Свободные времена смены. В рабочее время прошедшие минуты не предлагаются."""
    state = shift_state(shift, now, ch)
    if state == "closed":
        return []
    rows = q(
        "SELECT scheduled_at FROM posts WHERE channel_id=? AND shift=? AND scheduled_at IS NOT NULL "
        "AND status IN ('pending','sent') AND id != ?",
        (ch["id"], shift.isoformat(), exclude_post or -1),
    )
    busy = {r["scheduled_at"] for r in rows}
    floor = now.replace(second=0, microsecond=0)
    result = []
    for s in all_slots(shift, ch):
        if int(s.timestamp()) in busy:
            continue
        if state == "active" and s < floor:
            continue
        result.append(s)
    return result


def overflow_dt(shift, ch):
    """Время, когда уходит пачка постов, которым не хватило слотов (на следующие сутки)."""
    return datetime.combine(shift + timedelta(days=1), t_over(ch), tzinfo=MSK)


def set_overflow(pid, shift, ch):
    """Поставить пост в пачку (manual=0, чтобы он мог переехать в слот, если тот освободится)."""
    run("UPDATE posts SET scheduled_at=?, manual=0 WHERE id=?", (int(overflow_dt(shift, ch).timestamp()), pid))


def is_overflow(post, shift, ch):
    return post["scheduled_at"] == int(overflow_dt(shift, ch).timestamp())


def when_text(post, shift, ch):
    """Фраза про время выхода поста."""
    if is_overflow(post, shift, ch):
        return f"в {ch['overflow_time']} (свободные места на ночь закончились, уйдёт пачкой вместе с остальными)"
    return f"в {fmt_ts(post['scheduled_at'])}"


def plan_shift(shift, now, ch):
    """
    ВАЖНАЯ ФУНКЦИЯ: автоматическая расстановка времени.
    Берёт посты смены без времени (в порядке поступления) и ставит каждому
    ближайшее свободное время с шагом канала. Если мест не хватило - пост
    уходит в пачку (время пачки канала), вместе с остальными такими же.
    Если слот освободился (например, отменили пост) - посты из пачки возвращаются в очередь.
    Возвращает (расставленные, ушедшие в пачку).
    """
    placed, overflow = [], []
    free = free_slots(shift, now, ch)
    if free:  # есть место - пачку разбираем заново, по порядку поступления
        run("UPDATE posts SET scheduled_at=NULL WHERE channel_id=? AND shift=? AND status='pending' "
            "AND manual=0 AND scheduled_at=?", (ch["id"], shift.isoformat(), int(overflow_dt(shift, ch).timestamp())))
    rows = q(
        "SELECT * FROM posts WHERE channel_id=? AND shift=? AND status='pending' AND scheduled_at IS NULL ORDER BY id",
        (ch["id"], shift.isoformat()),
    )
    for r in rows:
        if free:
            slot = free.pop(0)
            run("UPDATE posts SET scheduled_at=? WHERE id=?", (int(slot.timestamp()), r["id"]))
            placed.append((r, slot))
        else:
            set_overflow(r["id"], shift, ch)
            overflow.append(r)
    return placed, overflow


def reschedule_missed(shift, now, ch):
    """Если бот был выключен и время поста давно прошло - сбросить время, чтобы пост встал заново."""
    limit = int((now - timedelta(minutes=MISSED_GRACE_MIN)).timestamp())
    run(
        "UPDATE posts SET scheduled_at=NULL WHERE channel_id=? AND shift=? AND status='pending' "
        "AND scheduled_at IS NOT NULL AND scheduled_at < ?",
        (ch["id"], shift.isoformat(), limit),
    )


def set_manual_time(pid, slot):
    run("UPDATE posts SET scheduled_at=?, manual=1 WHERE id=?", (int(slot.timestamp()), pid))


def channel_busy(ch, now):
    """Идёт ли у канала смена (рабочее время или дожим пачки). В это время менять время работы нельзя."""
    for s in q("SELECT shift FROM shifts WHERE channel_id=? AND closed=0", (ch["id"],)):
        if shift_state(date.fromisoformat(s["shift"]), now, ch) != "before":
            return True
    return False


def reset_pending_times(channel_id):
    """После смены времени работы: ожидающие посты теряют назначенное время (расставятся заново)."""
    run("UPDATE posts SET scheduled_at=NULL, manual=0 WHERE channel_id=? AND status='pending'", (channel_id,))


# =====================================================================
#  ПРИЁМ ПОСТОВ ИЗ ГРУППЫ
# =====================================================================
_INVISIBLE_RE = re.compile("[\u200b-\u200d\u2060\ufeff]")


def make_text_key(message):
    """
    Отпечаток содержания поста для антидубля: текст (или подпись к фото/видео) без учёта
    регистра, пробелов и переносов строк + адреса скрытых ссылок (текст со ссылкой внутри).
    Пост без текста (просто картинка) не сравнивается - вернёт None.
    """
    text = message.text or message.caption or ""
    norm = " ".join(_INVISIBLE_RE.sub("", text).split()).casefold()
    if not norm:
        return None
    ents = message.entities or message.caption_entities or []
    urls = sorted(e.url.strip().lower() for e in ents if e.type == "text_link" and e.url)
    return hashlib.sha256((norm + "\n" + "\n".join(urls)).encode("utf-8")).hexdigest()


def find_duplicate(channel_id, shift_str, text_key, pid):
    """Есть ли на этой смене канала такой же пост (живой: ожидает или уже опубликован)?"""
    if not text_key:
        return None
    rows = q(
        "SELECT * FROM posts WHERE channel_id=? AND shift=? AND text_key=? AND id != ? "
        "AND status IN ('pending','sent') ORDER BY id LIMIT 1",
        (channel_id, shift_str, text_key, pid),
    )
    return rows[0] if rows else None


def has_custom_emoji(message):
    """Есть ли в сообщении премиум (кастомные) эмодзи."""
    ents = list(message.entities or []) + list(message.caption_entities or [])
    return any(e.type == "custom_emoji" for e in ents)


def register_message(message, ch, shift_str, text_key=None):
    """
    ВАЖНАЯ ФУНКЦИЯ: записать сообщение впшера как пост канала.
    Альбом (несколько фото одним постом) склеивается в один пост.
    Возвращает (id поста, это новый пост?).
    """
    mg = message.media_group_id
    fwd = 1 if has_custom_emoji(message) else 0
    if mg:
        rows = q("SELECT id FROM posts WHERE chat_id=? AND media_group_id=?", (message.chat.id, str(mg)))
        if rows:
            run("INSERT OR IGNORE INTO post_messages (post_id, message_id) VALUES (?, ?)",
                (rows[0]["id"], message.message_id))
            if fwd:
                run("UPDATE posts SET fwd=1 WHERE id=?", (rows[0]["id"],))
            return rows[0]["id"], False
    cur = run(
        "INSERT INTO posts (channel_id, shift, chat_id, author_id, author_name, media_group_id, created_at, "
        "text_key, fwd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ch["id"], shift_str, message.chat.id, message.from_user.id, message.from_user.full_name,
         str(mg) if mg else None, int(now_msk().timestamp()), text_key, fwd),
    )
    pid = cur.lastrowid
    run("INSERT INTO post_messages (post_id, message_id) VALUES (?, ?)", (pid, message.message_id))
    run("INSERT OR IGNORE INTO shifts (channel_id, shift) VALUES (?, ?)", (ch["id"], shift_str))
    return pid, True


def find_post_by_message(chat_id, message_id):
    """Найти пост по сообщению (само сообщение поста или сообщение бота с кнопками)."""
    rows = q(
        "SELECT p.* FROM posts p JOIN post_messages m ON m.post_id = p.id "
        "WHERE p.chat_id=? AND m.message_id=?",
        (chat_id, message_id),
    )
    if rows:
        return rows[0]
    rows = q("SELECT * FROM posts WHERE chat_id=? AND prompt_msg_id=?", (chat_id, message_id))
    return rows[0] if rows else None


# =====================================================================
#  ОТПРАВКА С ОБЩИМ ЛИМИТОМ (чтобы много каналов не упёрлись в лимиты Telegram)
# =====================================================================
_rl_lock = None
_rl_next = 0.0


async def throttle():
    """Общая очередь: не больше SEND_LIMIT_PER_SEC запросов в секунду на все каналы вместе."""
    global _rl_lock, _rl_next
    if _rl_lock is None:
        _rl_lock = asyncio.Lock()
    async with _rl_lock:
        now = time.monotonic()
        wait = _rl_next - now
        if wait > 0:
            await asyncio.sleep(wait)
        _rl_next = max(now, _rl_next) + 1.0 / SEND_LIMIT_PER_SEC


async def send_msg(bot, chat_id, text, **kw):
    for _ in range(3):
        await throttle()
        try:
            return await bot.send_message(chat_id, text, **kw)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
    return None


async def send_long(bot, chat_id, text):
    """Отправить длинный текст несколькими сообщениями (лимит Telegram - 4096 символов)."""
    try:
        chunk_text = ""
        for line in text.split("\n"):
            if len(chunk_text) + len(line) + 1 > 3800:
                await send_msg(bot, chat_id, chunk_text)
                chunk_text = ""
            chunk_text += line + "\n"
        if chunk_text.strip():
            await send_msg(bot, chat_id, chunk_text)
    except Exception:
        logging.exception("Не удалось отправить сообщение в чат")


async def reply_safe(message, text, **kw):
    """Ответ на сообщение с повтором при лимите Telegram (при больших сменах группа получает много ответов)."""
    for _ in range(3):
        try:
            return await message.reply(text, **kw)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (TelegramBadRequest, TelegramForbiddenError):
            return None  # например, исходное сообщение удалено
    return None


# =====================================================================
#  КЛАВИАТУРЫ ВЫБОРА ВРЕМЕНИ
# =====================================================================
def chunk(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]


def prompt_kb(pid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏰ Выбрать время", callback_data=f"vp:pick:{pid}"),
        InlineKeyboardButton(text="🤖 Пусть бот решит", callback_data=f"vp:auto:{pid}"),
    ]])


def hours_kb(pid, free):
    by_hour = {}
    for s in free:
        by_hour.setdefault(s.hour, []).append(s)
    buttons = [InlineKeyboardButton(text=f"{h:02d}:xx", callback_data=f"vp:hour:{pid}:{h}") for h in by_hour]
    rows = chunk(buttons, 4)
    rows.append([InlineKeyboardButton(text="🤖 Пусть бот решит", callback_data=f"vp:auto:{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def minutes_kb(pid, slots):
    buttons = [InlineKeyboardButton(text=s.strftime("%H:%M"), callback_data=f"vp:slot:{pid}:{int(s.timestamp())}")
               for s in slots]
    rows = chunk(buttons, 4)
    rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data=f"vp:pick:{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================================
#  КОМАНДЫ В РАБОЧЕЙ ГРУППЕ
# =====================================================================
async def work_group(message: Message):
    """Фильтр: сообщение пришло из рабочей группы какого-то активного канала. Кладёт канал в `ch`."""
    ch = channel_by_group(message.chat.id)
    return {"ch": ch} if ch else False


def help_text(ch):
    return (
        "Как работает бот:\n"
        "1) Отправьте пост в рабочую группу.\n"
        f"2) До {ch['work_start']} МСК бот просто собирает посты. Время можно выбрать кнопкой или ответом на "
        "свой пост, например: 23:40. Если ничего не выбрать - бот поставит ближайшее свободное время сам.\n"
        f"3) С {ch['work_start']} до {ch['work_end']} бот публикует посты в канал с интервалом "
        f"{ch['step_min']} мин.\n"
        "4) Пост, отправленный ночью, тоже уйдёт в эту же ночь.\n"
        f"5) Рабочие дни (по дню начала смены): {days_text(ch)}.\n\n"
        "Команды:\n"
        "/queue - расписание текущей смены\n"
        "/cancel - ответьте этой командой на пост, чтобы отменить его\n"
    )


@router.message(Command("start", "help"), work_group)
async def cmd_help_group(message: Message, ch):
    await message.answer(help_text(ch))


@router.message(Command("start", "help"))
async def cmd_help(message: Message):
    await message.answer("Я бот взаимного пиара. Консоль управления: /console (в личных сообщениях).")


@router.message(Command("id"))
async def cmd_id(message: Message):
    """Помогает узнать ID группы/канала для настроек (работает в любом чате)."""
    uid = message.from_user.id if message.from_user else "?"
    await message.answer(f"ID этого чата: {message.chat.id}\nВаш ID: {uid}")


@router.message(Command("queue"), work_group)
async def cmd_queue(message: Message, ch):
    """Показать расписание текущей смены."""
    now = now_msk()
    shift = shift_of(now, ch)
    rows = q(
        "SELECT * FROM posts WHERE channel_id=? AND shift=? AND status IN ('pending','sent') "
        "ORDER BY (scheduled_at IS NULL), scheduled_at, id",
        (ch["id"], shift.isoformat()),
    )
    if not rows:
        await message.answer(f"В смене ({shift_title(shift)}) постов пока нет.")
        return
    lines = [f"<b>Смена: {shift_title(shift)}</b>"]
    for r in rows:
        if r["status"] == "sent":
            lines.append(f"✅ {fmt_ts(r['scheduled_at'])} - {post_author(r)} (опубликован)")
        elif r["scheduled_at"]:
            lines.append(f"🕒 {fmt_ts(r['scheduled_at'])} - {post_author(r)}")
        else:
            lines.append(f"⏳ время назначит бот - {post_author(r)}")
    await send_long(message.bot, message.chat.id, "\n".join(lines))


@router.message(Command("cancel"), work_group)
async def cmd_cancel(message: Message, ch):
    """Отменить пост: ответьте на него командой /cancel."""
    if not message.reply_to_message:
        await message.reply("Ответьте командой /cancel на тот пост, который нужно отменить.")
        return
    post = find_post_by_message(message.chat.id, message.reply_to_message.message_id)
    if not post:
        await message.reply("Не нашёл такой пост.")
        return
    if not can_edit(message.from_user.id, post):
        await message.reply("Отменить пост может только его автор или админ.")
        return
    if post["status"] != "pending":
        await message.reply("Этот пост уже нельзя отменить (опубликован или обработан).")
        return
    run("UPDATE posts SET status='cancelled' WHERE id=?", (post["id"],))
    await message.reply("<b>🚫 Пост отменён</b>, в канал он не уйдёт.")


# --- управление людьми прямо из рабочей группы (кроме консоли) ---
def target_from_command(message):
    """Кого назначаем/снимаем: автор сообщения, на которое ответили, либо ID после команды."""
    reply = message.reply_to_message
    if reply and reply.from_user and not reply.from_user.is_bot:
        return reply.from_user.id, reply.from_user.full_name
    parts = (message.text or "").split()
    if len(parts) > 1 and parts[1].lstrip("-").isdigit():
        return int(parts[1]), None
    return None


def add_admin(ch, uid, name):
    """Добавить админа канала. Главный владелец скрыт: его в список не записываем."""
    if is_super(uid) or is_ch_owner(uid, ch):
        return
    run("INSERT INTO channel_admins (channel_id, user_id, name, added_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(channel_id, user_id) DO UPDATE SET name=COALESCE(excluded.name, name)",
        (ch["id"], uid, name, int(now_msk().timestamp())))


@router.message(Command("addadmin", "deladmin"), work_group)
async def cmd_admin_manage(message: Message, ch):
    """Назначить/снять админа канала: главный владелец или владелец канала. Молча игнорирует остальных."""
    uid = message.from_user.id if message.from_user else None
    if uid is None or not can_manage_admins(uid, ch):
        return
    adding = message.text.split()[0].lower().startswith("/addadmin")
    target = target_from_command(message)
    if not target:
        cmd = "/addadmin" if adding else "/deladmin"
        await message.reply(f"Укажите ID: {cmd} 123456789 - или ответьте командой {cmd} на сообщение человека.")
        return
    tid, name = target
    if adding:
        add_admin(ch, tid, name)
        await message.reply(f"✅ {mention(tid, name) if name else tid} назначен админом канала.")
    else:
        cur = run("DELETE FROM channel_admins WHERE channel_id=? AND user_id=?", (ch["id"], tid))
        await message.reply("✅ Админ снят." if cur.rowcount else "Такого админа нет.")


@router.message(Command("setowner"), work_group)
async def cmd_set_owner(message: Message, ch):
    """Назначить владельца канала (только главный владелец): ответом на сообщение человека или /setowner ID."""
    if not message.from_user or not is_super(message.from_user.id):
        return
    target = target_from_command(message)
    if not target:
        await message.reply("Ответьте командой /setowner на сообщение человека или укажите ID: /setowner 123456789")
        return
    tid, name = target
    if is_super(tid):
        await message.reply("Главный владелец и так имеет полный доступ ко всем каналам.")
        return
    run("DELETE FROM channel_admins WHERE channel_id=? AND user_id=?", (ch["id"], tid))
    run("UPDATE channels SET owner_id=?, owner_name=COALESCE(?, owner_name) WHERE id=?", (tid, name, ch["id"]))
    await message.reply(f"✅ {mention(tid, name) if name else tid} назначен владельцем канала.")


# =====================================================================
#  ВРЕМЯ, ЗАДАННОЕ ТЕКСТОМ (ответом на пост: "23:40")
# =====================================================================
TIME_RE = re.compile(r"^\s*([01]?\d|2[0-3])[:.]([0-5]\d)\s*$")


@router.message(F.reply_to_message, F.text.regexp(TIME_RE.pattern), work_group)
async def on_time_reply(message: Message, ch):
    """
    ВАЖНАЯ ФУНКЦИЯ: впшер отвечает на свой пост временем (например 23:40).
    Если время занято или не попадает в сетку - ставим на ближайшее свободное после него.
    """
    post = find_post_by_message(message.chat.id, message.reply_to_message.message_id)
    if not post:
        return
    if not can_edit(message.from_user.id, post):
        await message.reply("Менять время может только автор поста или админ.")
        return
    if post["status"] != "pending":
        await message.reply("Для этого поста время уже менять нельзя.")
        return

    m = TIME_RE.match(message.text)
    t = dtime(int(m.group(1)), int(m.group(2)))
    shift = date.fromisoformat(post["shift"])
    now = now_msk()
    if shift_state(shift, now, ch) == "closed":
        await message.reply("Смена уже закончилась.")
        return

    day = shift if t >= t_start(ch) else shift + timedelta(days=1)
    want = datetime.combine(day, t, tzinfo=MSK)
    start, end_excl = window_bounds(shift, ch)
    if want < start or want >= end_excl:
        last = all_slots(shift, ch)[-1]
        await message.reply(f"Время должно быть между {start:%H:%M} и {last:%H:%M} (МСК).")
        return

    slot = next((s for s in free_slots(shift, now, ch, exclude_post=post["id"]) if s >= want), None)
    if slot is None:
        set_overflow(post["id"], shift, ch)
        await message.reply(f"<b>⚠️ Свободных мест нет.</b> Пост уйдёт пачкой в {ch['overflow_time']}.")
        return
    set_manual_time(post["id"], slot)
    if slot == want:
        await message.reply(f"<b>✅ Пост выйдет в {slot:%H:%M}.</b>")
    else:
        await message.reply(f"<b>✅ Время изменено.</b> {want:%H:%M} занято или не попадает в сетку "
                            f"{ch['step_min']} минут. Поставил на ближайшее свободное: {slot:%H:%M}.")


# =====================================================================
#  КНОПКИ ВЫБОРА ВРЕМЕНИ
# =====================================================================
async def safe_edit(cb: CallbackQuery, text, kb=None):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except (TelegramBadRequest, AttributeError):
        pass  # например, текст не изменился - не страшно


@router.callback_query(F.data.startswith("vp:"))
async def on_callback(cb: CallbackQuery):
    parts = cb.data.split(":")
    action, pid = parts[1], int(parts[2])
    post = get_post(pid)
    if not post:
        await cb.answer("Пост не найден", show_alert=True)
        return
    if not can_edit(cb.from_user.id, post):
        await cb.answer("Это не ваш пост", show_alert=True)
        return
    if post["status"] != "pending":
        await cb.answer("Этот пост уже обработан", show_alert=True)
        return
    ch = get_channel(post["channel_id"])
    if not ch:
        await cb.answer("Канал не найден", show_alert=True)
        return

    shift = date.fromisoformat(post["shift"])
    now = now_msk()
    free = free_slots(shift, now, ch, exclude_post=pid)

    if action == "auto":
        # ближайшее свободное время; если мест нет - пачка
        if free:
            slot = free[0]
            set_manual_time(pid, slot)
            await safe_edit(cb, f"<b>✅ Пост выйдет в {slot:%H:%M}</b> (ближайшее свободное время).")
            await cb.answer(f"Выйдет в {slot:%H:%M}")
        else:
            set_overflow(pid, shift, ch)
            await safe_edit(cb, f"<b>⚠️ Свободных мест не осталось.</b> Пост уйдёт пачкой в {ch['overflow_time']}.")
            await cb.answer(f"Выйдет в {ch['overflow_time']}")
        return
    elif action == "pick":
        if not free:
            await safe_edit(cb, "Свободных мест нет.")
        else:
            await safe_edit(cb, "Выберите час (МСК):", hours_kb(pid, free))
    elif action == "hour":
        hour = int(parts[3])
        slots = [s for s in free if s.hour == hour]
        if not slots:
            await safe_edit(cb, "В этом часу свободных мест нет. Выберите другой час:", hours_kb(pid, free))
        else:
            await safe_edit(cb, f"Выберите время в {hour:02d}:xx:", minutes_kb(pid, slots))
    elif action == "slot":
        slot = datetime.fromtimestamp(int(parts[3]), MSK)
        if slot not in free:
            await cb.answer("Это время уже занято, выберите другое", show_alert=True)
            await safe_edit(cb, "Выберите час (МСК):", hours_kb(pid, free))
            return
        set_manual_time(pid, slot)
        await safe_edit(cb, f"<b>✅ Пост выйдет в {slot:%H:%M}.</b>")
    await cb.answer()


# =====================================================================
#  КОНСОЛЬ (только в личке бота: /console)
# =====================================================================
# Ожидание текстового ответа от человека в личке: {id человека: (что_вводит, id_канала, данные)}
waiting = {}


def week_label(ch, offset):
    mon = week_start(shift_of(now_msk(), ch), offset)
    return f"{mon:%d.%m} Пн – {mon + timedelta(days=6):%d.%m} Вс"


def week_name(offset):
    return "текущая неделя" if offset == 0 else "прошлая неделя"


def authors_stats_text(ch, offset):
    """
    Статистика впшеров канала за неделю: кто писал в рабочий чат и сколько ВП сделал
    (ВП = пост, который бот опубликовал в канал). Неделя: смены с понедельника по воскресенье.
    """
    mon = week_start(shift_of(now_msk(), ch), offset)
    rows = q(
        "SELECT author_id, author_name, status FROM posts WHERE channel_id=? AND shift >= ? AND shift <= ? "
        "ORDER BY created_at, id",
        (ch["id"], mon.isoformat(), (mon + timedelta(days=6)).isoformat()),
    )
    stats = {}  # author_id -> [имя, кол-во ВП]
    for r in rows:
        if r["author_id"] is None:
            continue
        item = stats.setdefault(r["author_id"], [None, 0])
        item[0] = r["author_name"] or item[0]
        if r["status"] == "sent":
            item[1] += 1
    head = (f"<b>📊 Статистика впшеров - {week_name(offset)}</b>\n{html.escape(ch['name'])}\n"
            f"{week_label(ch, offset)}\n")
    if not stats:
        return head + "\nДанных пока нет."
    ordered = sorted(stats.items(), key=lambda kv: (-kv[1][1], (kv[1][0] or "").lower()))
    lines, size = [], len(head)
    for i, (uid, (name, cnt)) in enumerate(ordered, 1):
        line = f"{i}. {mention(uid, name, CONSOLE_LINK)} - {cnt} вп"
        if size + len(line) > 3800:  # лимит Telegram - 4096 символов
            lines.append(f"…и ещё {len(ordered) - i + 1}")
            break
        lines.append(line)
        size += len(line) + 1
    total = sum(v[1] for v in stats.values())
    return head + "\n" + "\n".join(lines) + f"\n\nВсего вп: {total}"


def vp_stats_text(ch, offset):
    """Статистика ВП канала по сменам недели: «09.10 Пт - 103 вп». Итог смены записывается ботом при её закрытии."""
    now = now_msk()
    cur = shift_of(now, ch)
    mon = week_start(cur, offset)
    lines, total = [], 0
    for i in range(7):
        d = mon + timedelta(days=i)
        if d > cur:
            break  # будущие смены не показываем
        rec = q("SELECT vp_count FROM shift_stats WHERE channel_id=? AND shift=?", (ch["id"], d.isoformat()))
        note = ""
        if rec:
            n = rec[0]["vp_count"]
        else:  # итога ещё нет: считаем по постам (идущая смена или смена без постов)
            n = q("SELECT COUNT(*) AS c FROM posts WHERE channel_id=? AND shift=? AND status='sent'",
                  (ch["id"], d.isoformat()))[0]["c"]
            if d == cur:
                note = " (смена идёт)" if shift_state(d, now, ch) == "active" else " (смена ещё не началась)"
        if n == 0 and not is_work_day(d, ch):
            continue  # в нерабочий день смены нет
        total += n
        lines.append(f"{d:%d.%m} {WEEKDAYS[d.weekday()]} - {n} вп{note}")
    head = (f"<b>📈 Статистика ВП - {week_name(offset)}</b>\n{html.escape(ch['name'])}\n"
            f"{week_label(ch, offset)}\n")
    return head + "\n" + "\n".join(lines) + f"\n\nИтого за неделю: {total} вп"


def btn(text, data):
    return InlineKeyboardButton(text=text, callback_data=data)


def main_menu(uid):
    """Главное меню консоли: список каналов, к которым у человека есть доступ."""
    chans = accessible_channels(uid)
    rows = [[btn(("⏸ " if not c["active"] else "📺 ") + c["name"][:40], f"cn:ch:{c['id']}")] for c in chans]
    if is_super(uid):
        rows.append([btn("➕ Добавить канал", "cn:add")])
        text = "<b>🛠 Консоль ВП</b>\n\n" + ("Выберите канал:" if chans else "Каналов пока нет. Добавьте первый.")
    else:
        text = "<b>🛠 Консоль ВП</b>\n\nВыберите канал:"
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def channel_menu(uid, ch):
    text = f"<b>📺 {html.escape(ch['name'])}</b>"
    if not ch["active"]:
        text += "\n⏸ Канал на паузе: посты не принимаются и не публикуются."
    cid = ch["id"]
    rows = [
        [btn("📊 Статистика впшеров", f"cn:a:{cid}:0")],
        [btn("📈 Статистика ВП", f"cn:v:{cid}:0")],
    ]
    if can_manage_admins(uid, ch):
        rows.append([btn("👥 Админы канала", f"cn:adm:{cid}")])
    if is_super(uid):
        rows.append([btn("⚙ Настройки канала", f"cn:set:{cid}")])
    rows.append([btn("🏠 К списку каналов", "cn:menu")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def stats_kb(kind, cid, offset):
    if offset == 0:
        switch = btn("⬅ Прошлая неделя", f"cn:{kind}:{cid}:1")
    else:
        switch = btn("Текущая неделя ➡", f"cn:{kind}:{cid}:0")
    return InlineKeyboardMarkup(inline_keyboard=[
        [switch],
        [btn("⬅ К каналу", f"cn:ch:{cid}"), btn("🏠 В меню", "cn:menu")],
    ])


def admins_view(ch):
    """Список админов канала. Главный владелец здесь не показывается никогда."""
    rows = q("SELECT user_id, name FROM channel_admins WHERE channel_id=? ORDER BY added_at, user_id", (ch["id"],))
    owner = (mention(ch["owner_id"], ch["owner_name"], CONSOLE_LINK) + f" (ID {ch['owner_id']})"
             if ch["owner_id"] is not None else "не назначен")
    lines = [f"<b>👥 Админы канала «{html.escape(ch['name'])}»</b>\n\nВладелец: {owner}"]
    buttons = []
    if rows:
        lines.append("")
        for i, r in enumerate(rows, 1):
            label = r["name"] or str(r["user_id"])
            lines.append(f"{i}. {mention(r['user_id'], r['name'], CONSOLE_LINK)} (ID {r['user_id']})")
            buttons.append([btn(f"🗑 Убрать: {label}"[:60], f"cn:del:{ch['id']}:{r['user_id']}")])
    else:
        lines.append("\nАдминов пока нет.")
    lines.append("\nДобавить: кнопка ниже, либо ответьте командой /addadmin на сообщение человека в рабочей группе.")
    buttons.insert(0, [btn("➕ Добавить админа", f"cn:aadd:{ch['id']}")])
    buttons.append([btn("⬅ К каналу", f"cn:ch:{ch['id']}"), btn("🏠 В меню", "cn:menu")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


def settings_view(ch):
    """Настройки канала (видит только главный владелец)."""
    cid = ch["id"]
    slots = len(all_slots(now_msk().date(), ch))
    owner = (f"{html.escape(ch['owner_name'] or '')} (ID {ch['owner_id']})" if ch["owner_id"] is not None
             else "не назначен")
    auto = f"через {ch['delete_after_h']} ч" if ch["delete_after_h"] else "выключено"
    text = (
        f"<b>⚙ Настройки «{html.escape(ch['name'])}»</b>\n\n"
        f"Канал (ID): {ch['channel_chat']}\n"
        f"Рабочая группа (ID): {ch['group_id']}\n"
        f"Владелец канала: {owner}\n"
        f"Рабочее время: {ch['work_start']} - {ch['work_end']} (МСК)\n"
        f"Шаг между постами: {ch['step_min']} мин (мест за смену: {slots})\n"
        f"Время пачки: {ch['overflow_time']}\n"
        f"Рабочие дни: {days_text(ch)}\n"
        f"Автоудаление постов: {auto}\n"
        f"Статус: {'✅ работает' if ch['active'] else '⏸ пауза'}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("✏ Название", f"cn:e:{cid}:name"), btn("👑 Владелец", f"cn:e:{cid}:owner")],
        [btn("📢 Канал", f"cn:e:{cid}:channel"), btn("💬 Группа", f"cn:e:{cid}:group")],
        [btn("🕒 Начало", f"cn:e:{cid}:start"), btn("🕘 Конец", f"cn:e:{cid}:end")],
        [btn("⏱ Шаг (кд)", f"cn:e:{cid}:step"), btn("📦 Время пачки", f"cn:e:{cid}:over")],
        [btn("📅 Дни недели", f"cn:days:{cid}"), btn("🗑 Автоудаление", f"cn:e:{cid}:autodel")],
        [btn("▶ Включить" if not ch["active"] else "⏸ Пауза", f"cn:tog:{cid}"),
         btn("❌ Удалить канал", f"cn:rm:{cid}")],
        [btn("⬅ К каналу", f"cn:ch:{cid}"), btn("🏠 В меню", "cn:menu")],
    ])
    return text, kb


EDIT_PROMPTS = {
    "name": "Введите новое название канала:",
    "owner": "Отправьте ID нового владельца канала или перешлите сюда его сообщение. 0 - убрать владельца.",
    "channel": "Перешлите сюда любое сообщение из канала либо отправьте его @username или ID (-100…). "
               "Бот должен быть админом канала.",
    "group": "Отправьте ID рабочей группы (число вида -100…). Узнать: добавьте бота в группу и отправьте там /id.",
    "start": "Введите время начала работы (МСК), например 22:15",
    "end": "Введите время конца работы - последнего возможного поста (МСК), например 09:50",
    "step": "Введите шаг между постами в минутах (1-180), например 5",
    "over": "Введите время пачки (куда уходят посты, когда не хватило мест), например 09:55",
    "autodel": "Через сколько часов удалять опубликованные посты из канала? Число от 1 до 47, "
               "0 - не удалять (Telegram не даёт боту удалять посты старше 48 часов).",
}
TIME_FIELDS = {"start", "end", "step", "over"}


def days_view(ch):
    cid = ch["id"]
    on = ch["work_days"]
    row1 = [btn(("✅ " if str(i) in on else "▫ ") + WEEKDAYS[i], f"cn:dt:{cid}:{i}") for i in range(4)]
    row2 = [btn(("✅ " if str(i) in on else "▫ ") + WEEKDAYS[i], f"cn:dt:{cid}:{i}") for i in range(4, 7)]
    text = (f"<b>📅 Рабочие дни «{html.escape(ch['name'])}»</b>\n\n"
            "Нажимайте на дни, чтобы включать и выключать. День считается по началу смены: смена с пятницы "
            "на субботу - это пятница. В выключенный день бот не принимает посты.\n\n"
            f"Сейчас: {days_text(ch)}")
    return text, InlineKeyboardMarkup(inline_keyboard=[row1, row2, [btn("⬅ К настройкам", f"cn:set:{cid}")]])


def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Отмена", "cn:cancel")]])


async def lookup_name(bot, chat_id, uid):
    """Имя человека по ID (если он есть в группе), иначе None."""
    try:
        member = await bot.get_chat_member(chat_id, uid)
        return member.user.full_name
    except Exception:
        return None


def user_from_message(message):
    """Человек из пересланного сообщения или из числа в тексте: (id, имя) или None."""
    origin = getattr(message, "forward_origin", None)
    u = getattr(origin, "sender_user", None)
    if u is not None:
        return u.id, u.full_name
    text = (message.text or "").strip()
    if re.fullmatch(r"-?\d+", text):
        return int(text), None
    return None


def parse_chat(value):
    value = str(value).strip()
    return int(value) if value.lstrip("-").isdigit() else value


async def resolve_channel(bot, message):
    """Канал из пересланного сообщения или из @username/ID. Возвращает (id, название) или (None, ошибка)."""
    origin = getattr(message, "forward_origin", None)
    chat = getattr(origin, "chat", None)
    if chat is None:
        text = (message.text or "").strip()
        if not text:
            return None, "Перешлите сообщение из канала или отправьте @username / ID канала."
        try:
            chat = await bot.get_chat(parse_chat(text))
        except Exception as e:
            return None, f"Не нашёл такой канал ({html.escape(str(e))}). Проверьте, что бот добавлен в канал."
    if chat.type != "channel":
        return None, "Это не канал. Нужен именно канал, куда бот будет публиковать."
    return chat.id, chat.title or str(chat.id)


async def channel_warnings(bot, chat_id):
    """Проверка прав бота в канале. Возвращает список предупреждений."""
    w = []
    try:
        me = await bot.me()
        member = await bot.get_chat_member(chat_id, me.id)
        if member.status == "creator":
            return w
        if member.status != "administrator":
            return ["бот не администратор канала (нужны права «Публикация» и «Удаление сообщений»)"]
        if not getattr(member, "can_post_messages", False):
            w.append("у бота нет права публиковать сообщения в канале")
        if not getattr(member, "can_delete_messages", False):
            w.append("у бота нет права удалять сообщения в канале (автоудаление не сработает)")
    except Exception:
        w.append("не удалось проверить права бота в канале")
    return w


async def group_check(bot, group_id):
    """Проверка группы. Возвращает (ошибка или None, список предупреждений)."""
    try:
        chat = await bot.get_chat(group_id)
    except Exception as e:
        return f"Не нашёл такую группу ({html.escape(str(e))}). Добавьте бота в группу и проверьте ID.", []
    if chat.type not in ("group", "supergroup"):
        return "Это не группа. Нужна рабочая группа впшеров.", []
    w = []
    try:
        me = await bot.me()
        member = await bot.get_chat_member(group_id, me.id)
        if member.status not in ("administrator", "creator"):
            w.append("бот не админ группы: чтобы он видел все посты, сделайте его админом "
                     "или отключите режим приватности в @BotFather")
    except Exception:
        w.append("не удалось проверить права бота в группе")
    return None, w


async def apply_setting(message, uid, cid, field):
    """Применить введённое значение настройки. Возвращает (готово, текст). Не готово - ждём ввод заново."""
    ch = get_channel(cid)
    if not ch or not is_super(uid):
        return True, "⛔ Нет доступа."
    bot = message.bot
    text = (message.text or "").strip()
    note = ""
    if field == "name":
        if not text or len(text) > 60:
            return False, "Название: от 1 до 60 символов. Попробуйте ещё раз."
        run("UPDATE channels SET name=? WHERE id=?", (text, cid))
    elif field == "owner":
        if text == "0":
            run("UPDATE channels SET owner_id=NULL, owner_name=NULL WHERE id=?", (cid,))
        else:
            target = user_from_message(message)
            if not target:
                return False, "Не понял. Отправьте числовой ID или перешлите сообщение человека (0 - убрать владельца)."
            tid, name = target
            if is_super(tid):
                return False, "Главный владелец скрыт и так имеет полный доступ - его владельцем канала не назначить."
            name = name or await lookup_name(bot, ch["group_id"], tid)
            run("DELETE FROM channel_admins WHERE channel_id=? AND user_id=?", (cid, tid))
            run("UPDATE channels SET owner_id=?, owner_name=? WHERE id=?", (tid, name, cid))
    elif field == "channel":
        chat_id, title = await resolve_channel(bot, message)
        if chat_id is None:
            return False, title
        if q("SELECT 1 FROM channels WHERE channel_chat=? AND id != ?", (chat_id, cid)):
            return False, "Этот канал уже подключен к другой записи."
        run("UPDATE channels SET channel_chat=? WHERE id=?", (chat_id, cid))
        w = await channel_warnings(bot, chat_id)
        note = "".join(f"\n⚠️ {x}" for x in w)
    elif field == "group":
        if not re.fullmatch(r"-?\d+", text):
            return False, "Нужен числовой ID группы (вида -100…)."
        gid = int(text)
        if q("SELECT 1 FROM channels WHERE group_id=? AND id != ?", (gid, cid)):
            return False, "Эта группа уже привязана к другому каналу."
        err, w = await group_check(bot, gid)
        if err:
            return False, err
        run("UPDATE channels SET group_id=? WHERE id=?", (gid, cid))
        note = "".join(f"\n⚠️ {x}" for x in w)
    elif field in TIME_FIELDS:
        if channel_busy(ch, now_msk()):
            return True, "Сейчас идёт смена этого канала - менять время можно после её закрытия."
        start, end, over, step = t_start(ch), t_end(ch), t_over(ch), ch["step_min"]
        if field == "step":
            if not re.fullmatch(r"\d{1,3}", text):
                return False, "Нужно целое число минут, например 5."
            step = int(text)
        else:
            t = parse_hhmm(text)
            if t is None:
                return False, "Не понял время. Формат: ЧЧ:ММ, например 22:15."
            if field == "start":
                start = t
            elif field == "end":
                end = t
            else:
                over = t
        err = check_settings(start, end, step, over)
        if err:
            return False, err
        run("UPDATE channels SET work_start=?, work_end=?, step_min=?, overflow_time=? WHERE id=?",
            (f"{start:%H:%M}", f"{end:%H:%M}", step, f"{over:%H:%M}", cid))
        reset_pending_times(cid)
        note = "\nВремя уже принятых, но не опубликованных постов сброшено - бот расставит его заново."
    elif field == "autodel":
        if not re.fullmatch(r"\d{1,2}", text) or int(text) > 47:
            return False, "Нужно число от 0 до 47 (0 - не удалять)."
        run("UPDATE channels SET delete_after_h=? WHERE id=?", (int(text), cid))
    else:
        return True, "Неизвестная настройка."
    return True, "✅ Сохранено." + note


async def finish_add_channel(message, uid, data):
    """Последний шаг добавления канала: проверки и запись."""
    bot = message.bot
    gid = data["group"]
    err, gw = await group_check(bot, gid)
    if err:
        return False, err
    cw = await channel_warnings(bot, data["channel"])
    now_ts = int(now_msk().timestamp())
    cur = run(
        "INSERT INTO channels (name, channel_chat, group_id, work_start, work_end, step_min, overflow_time, "
        "delete_after_h, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
        (data["name"], data["channel"], gid, DEFAULT_WORK_START, DEFAULT_WORK_END, DEFAULT_STEP_MIN, DEFAULT_OVERFLOW_TIME,
         DEFAULT_DELETE_AFTER_H, now_ts),
    )
    warn = "".join(f"\n⚠️ {x}" for x in cw + gw)
    return cur.lastrowid, f"✅ Канал «{html.escape(data['name'])}» добавлен.{warn}"


@router.message(Command("console"))
async def cmd_console(message: Message):
    uid = message.from_user.id if message.from_user else None
    if uid is None:
        return
    if message.chat.type != "private":
        if accessible_channels(uid):
            await message.reply("Консоль открывается в личных сообщениях с ботом.")
        return
    chans = accessible_channels(uid)
    if not chans and not is_super(uid):
        await message.answer("⛔ У вас нет доступа к консоли.")
        return
    waiting.pop(uid, None)
    # обновляем имя человека в списках админов/владельцев (чтобы ссылка показывала актуальный ник)
    run("UPDATE channel_admins SET name=? WHERE user_id=?", (message.from_user.full_name, uid))
    run("UPDATE channels SET owner_name=? WHERE owner_id=?", (message.from_user.full_name, uid))
    text, kb = main_menu(uid)
    await message.answer(text, reply_markup=kb)


async def has_waiting(message: Message):
    return bool(message.from_user) and message.chat.type == "private" and message.from_user.id in waiting


@router.message(F.chat.type == "private", has_waiting)
async def on_private_input(message: Message):
    """Ответ человека в личке на вопрос консоли (название канала, время, ID и т.п.)."""
    uid = message.from_user.id
    state = waiting.get(uid)
    if not state:
        return
    kind, cid, data = state
    text = (message.text or "").strip()
    if text.lower() in ("отмена", "/cancel"):
        waiting.pop(uid, None)
        menu, kb = main_menu(uid)
        await message.answer("Отменено.\n\n" + menu, reply_markup=kb)
        return

    # --- добавление канала (только главный владелец) ---
    if kind in ("add_name", "add_channel", "add_group"):
        if not is_super(uid):
            waiting.pop(uid, None)
            return
        if kind == "add_name":
            if not text or len(text) > 60:
                await message.answer("Название: от 1 до 60 символов. Попробуйте ещё раз.", reply_markup=cancel_kb())
                return
            waiting[uid] = ("add_channel", None, {"name": text})
            await message.answer("Теперь канал: перешлите сюда любое сообщение из канала либо отправьте его "
                                 "@username или ID (-100…). Бот должен быть админом канала.",
                                 reply_markup=cancel_kb())
        elif kind == "add_channel":
            chat_id, title = await resolve_channel(message.bot, message)
            if chat_id is None:
                await message.answer(title, reply_markup=cancel_kb())
                return
            if q("SELECT 1 FROM channels WHERE channel_chat=?", (chat_id,)):
                await message.answer("Этот канал уже добавлен.", reply_markup=cancel_kb())
                return
            data["channel"] = chat_id
            waiting[uid] = ("add_group", None, data)
            await message.answer(f"Канал: {html.escape(title)}\n\nТеперь ID рабочей группы впшеров этого канала "
                                 "(число вида -100…). Узнать: добавьте бота в группу и отправьте там /id.",
                                 reply_markup=cancel_kb())
        else:
            if not re.fullmatch(r"-?\d+", text):
                await message.answer("Нужен числовой ID группы (вида -100…).", reply_markup=cancel_kb())
                return
            gid = int(text)
            if q("SELECT 1 FROM channels WHERE group_id=?", (gid,)):
                await message.answer("Эта группа уже привязана к другому каналу.", reply_markup=cancel_kb())
                return
            data["group"] = gid
            new_id, msg = await finish_add_channel(message, uid, data)
            if not new_id:
                await message.answer(msg, reply_markup=cancel_kb())
                return
            waiting.pop(uid, None)
            ch = get_channel(new_id)
            stext, kb = settings_view(ch)
            await message.answer(msg + "\n\n" + stext, reply_markup=kb)
        return

    # --- добавление админа (владелец канала или главный владелец) ---
    if kind == "aadd":
        ch = get_channel(cid)
        if not ch or not can_manage_admins(uid, ch):
            waiting.pop(uid, None)
            return
        target = user_from_message(message)
        if not target:
            await message.answer("Не понял. Отправьте числовой ID или перешлите сообщение человека.",
                                 reply_markup=cancel_kb())
            return
        tid, name = target
        name = name or await lookup_name(message.bot, ch["group_id"], tid)
        if is_ch_owner(tid, ch):
            await message.answer("Владелец и так имеет полный доступ.", reply_markup=cancel_kb())
            return
        add_admin(ch, tid, name)  # главного владельца молча не записывает - он скрыт и так имеет доступ
        waiting.pop(uid, None)
        vtext, kb = admins_view(ch)
        await message.answer("✅ Админ добавлен.\n\n" + vtext, reply_markup=kb)
        return

    # --- настройки канала (только главный владелец) ---
    if kind == "edit":
        field = data
        done, msg = await apply_setting(message, uid, cid, field)
        if not done:
            await message.answer(msg, reply_markup=cancel_kb())
            return
        waiting.pop(uid, None)
        ch = get_channel(cid)
        if ch:
            stext, kb = settings_view(ch)
            await message.answer(msg + "\n\n" + stext, reply_markup=kb)
        else:
            await message.answer(msg)


@router.callback_query(F.data.startswith("cn:"))
async def on_console(cb: CallbackQuery):
    uid = cb.from_user.id
    parts = cb.data.split(":")
    action = parts[1]

    if action == "menu" or action == "cancel":
        waiting.pop(uid, None)
        if not accessible_channels(uid) and not is_super(uid):
            await cb.answer("Нет доступа", show_alert=True)
            return
        text, kb = main_menu(uid)
        await safe_edit(cb, text, kb)
        await cb.answer()
        return

    if action == "add":
        if not is_super(uid):
            await cb.answer("Нет доступа", show_alert=True)
            return
        waiting[uid] = ("add_name", None, {})
        await safe_edit(cb, "<b>➕ Новый канал</b>\n\nВведите название канала (для консоли и статистики):", cancel_kb())
        await cb.answer()
        return

    # дальше все действия относятся к конкретному каналу
    cid = int(parts[2])
    ch = get_channel(cid)
    if not ch or not has_access(uid, ch):
        await cb.answer("Нет доступа", show_alert=True)
        return

    if action == "ch":
        waiting.pop(uid, None)
        text, kb = channel_menu(uid, ch)
    elif action in ("a", "v"):
        offset = 1 if parts[3] == "1" else 0
        text = authors_stats_text(ch, offset) if action == "a" else vp_stats_text(ch, offset)
        kb = stats_kb(action, cid, offset)
    elif action in ("adm", "aadd", "del"):
        if not can_manage_admins(uid, ch):
            await cb.answer("Нет доступа", show_alert=True)
            return
        if action == "del":
            run("DELETE FROM channel_admins WHERE channel_id=? AND user_id=?", (cid, int(parts[3])))
        if action == "aadd":
            waiting[uid] = ("aadd", cid, None)
            await safe_edit(cb, "Отправьте числовой ID нового админа или перешлите сюда его сообщение.", cancel_kb())
            await cb.answer()
            return
        text, kb = admins_view(ch)
    elif action in ("set", "e", "tog", "rm", "rmy", "days", "dt"):
        if not is_super(uid):
            await cb.answer("Нет доступа", show_alert=True)
            return
        if action == "set":
            waiting.pop(uid, None)
            text, kb = settings_view(ch)
        elif action == "e":
            field = parts[3]
            if field not in EDIT_PROMPTS:
                await cb.answer()
                return
            if field in TIME_FIELDS and channel_busy(ch, now_msk()):
                await cb.answer("Сейчас идёт смена этого канала - менять время можно после её закрытия.",
                                show_alert=True)
                return
            waiting[uid] = ("edit", cid, field)
            await safe_edit(cb, EDIT_PROMPTS[field], cancel_kb())
            await cb.answer()
            return
        elif action in ("days", "dt"):
            if action == "dt":
                d = parts[3]
                days = set(ch["work_days"])
                if d in days:
                    if len(days) == 1:
                        await cb.answer("Должен остаться хотя бы один рабочий день", show_alert=True)
                        return
                    days.discard(d)
                else:
                    days.add(d)
                run("UPDATE channels SET work_days=? WHERE id=?", ("".join(sorted(days)), cid))
                ch = get_channel(cid)
            text, kb = days_view(ch)
        elif action == "tog":
            run("UPDATE channels SET active=1-active WHERE id=?", (cid,))
            text, kb = settings_view(get_channel(cid))
        elif action == "rm":
            text = (f"<b>❌ Удалить канал «{html.escape(ch['name'])}»?</b>\n\n"
                    "Вместе с ним удалятся его посты, смены и статистика. Бот перестанет работать с этим каналом и группой.")
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [btn("✅ Да, удалить", f"cn:rmy:{cid}")],
                [btn("⬅ Нет, назад", f"cn:set:{cid}")],
            ])
        else:  # rmy
            delete_channel(cid)
            text, kb = main_menu(uid)
            text = "✅ Канал удалён.\n\n" + text
    else:
        await cb.answer()
        return
    await safe_edit(cb, text, kb)
    await cb.answer()


def delete_channel(cid):
    run("DELETE FROM post_messages WHERE post_id IN (SELECT id FROM posts WHERE channel_id=?)", (cid,))
    run("DELETE FROM posts WHERE channel_id=?", (cid,))
    run("DELETE FROM shifts WHERE channel_id=?", (cid,))
    run("DELETE FROM shift_stats WHERE channel_id=?", (cid,))
    run("DELETE FROM channel_admins WHERE channel_id=?", (cid,))
    run("DELETE FROM channels WHERE id=?", (cid,))


# =====================================================================
#  ПРИЁМ ПОСТОВ (обработчик обычных сообщений в группе - стоит последним!)
# =====================================================================
POST_TYPES = {
    ContentType.TEXT, ContentType.PHOTO, ContentType.VIDEO, ContentType.ANIMATION,
    ContentType.DOCUMENT, ContentType.AUDIO, ContentType.VOICE, ContentType.VIDEO_NOTE,
}


@router.message(F.content_type.in_(POST_TYPES), work_group)
async def on_post(message: Message, ch):
    """
    ВАЖНАЯ ФУНКЦИЯ: любое сообщение впшера в рабочей группе считается постом этого канала.
    - До начала работы: бот предлагает выбрать время.
    - В рабочее время: сразу ставит ближайшее свободное время.
    - Если места кончились: пост уходит пачкой.
    - Если рабочее окно смены уже закрыто: сообщает, что пост не уйдёт.
    - Если на этой смене уже есть пост с таким же текстом: отклоняет его (никуда не планируется).
    """
    if message.from_user is None or message.from_user.is_bot:
        return
    if message.text and message.text.startswith("/"):
        return

    now = now_msk()
    shift = shift_of(message.date.astimezone(MSK), ch)
    if not is_work_day(shift, ch):
        if not message.media_group_id:  # у альбома отвечаем только раз - на первое сообщение
            await reply_safe(message, f"<b>🚫 В {shift_title(shift)} смены нет</b> (работаем: {days_text(ch)}). "
                                      "Пост не принят.")
        return
    text_key = make_text_key(message)
    pid, is_new = register_message(message, ch, shift.isoformat(), text_key)
    if not is_new:
        return  # продолжение альбома - отвечать не нужно

    # антидубль (между записью поста и проверкой нет await - два одинаковых поста не проскочат)
    dup = find_duplicate(ch["id"], shift.isoformat(), text_key, pid)
    if dup:
        run("UPDATE posts SET status='duplicate' WHERE id=?", (pid,))
        when = f", выйдет в {fmt_ts(dup['scheduled_at'])}" if dup["scheduled_at"] and dup["status"] == "pending" else ""
        await reply_safe(
            message,
            f"<b>🔁 Такой пост уже есть на этой смене</b> (от {post_author(dup)}{when}). "
            "Дубль отклонён и в канал не уйдёт.",
        )
        return

    state = shift_state(shift, now, ch)
    if state == "before":
        sent = await reply_safe(
            message,
            f"<b>✅ Пост принят ({shift_title(shift)})</b>\n"
            "Хотите выбрать время сами?",
            reply_markup=prompt_kb(pid),
        )
        if sent:
            run("UPDATE posts SET prompt_msg_id=? WHERE id=?", (sent.message_id, pid))
    elif state == "active":
        plan_shift(shift, now, ch)
        post = get_post(pid)
        await reply_safe(message, f"<b>✅ Пост принят</b>, выйдет {when_text(post, shift, ch)}.")
    else:
        run("UPDATE posts SET status='expired' WHERE id=?", (pid,))
        await reply_safe(message, "<b>⛔ Рабочее окно этой смены уже закрыто</b>, пост не будет опубликован.")


# =====================================================================
#  ПУБЛИКАЦИЯ В КАНАЛ
# =====================================================================
async def send_post(bot, post, ch):
    """
    ВАЖНАЯ ФУНКЦИЯ: публикация поста в канал.
    Пост пересылается (forward) из группы в канал.
    """
    mids = sorted(r["message_id"] for r in q("SELECT message_id FROM post_messages WHERE post_id=?", (post["id"],)))
    group = ch["group_id"]
    try:
        # всегда пересылка (forward): оформление, в том числе премиум-эмодзи, остаётся как в оригинале
        await throttle()
        result = await bot.forward_messages(chat_id=ch["channel_chat"], from_chat_id=post["chat_id"],
                                            message_ids=mids)
    except TelegramRetryAfter as e:
        logging.warning("Telegram просит подождать %s сек", e.retry_after)
        await asyncio.sleep(e.retry_after)
        return  # пост остался в очереди, попробуем на следующей проверке
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        run("UPDATE posts SET status='failed' WHERE id=?", (post["id"],))
        await send_long(bot, group, f"<b>❌ Не удалось опубликовать пост</b> от {post_author(post)}: "
                                    f"{html.escape(str(e))}")
        return

    if not result:
        run("UPDATE posts SET status='failed' WHERE id=?", (post["id"],))
        await send_long(bot, group,
                        f"<b>❌ Пост не опубликован</b> (автор {post_author(post)}): сообщение не найдено "
                        "(возможно, удалено).")
        return
    ch_ids = ",".join(str(m.message_id) for m in result)
    run("UPDATE posts SET status='sent', sent_at=?, channel_msgs=?, out_chat=? WHERE id=?",
        (int(now_msk().timestamp()), ch_ids, ch["channel_chat"], post["id"]))
    if len(result) < len(mids):
        await send_long(bot, group,
                        f"<b>⚠️ Пост опубликован не полностью</b> (автор {post_author(post)}; "
                        "часть сообщений удалена).")


async def delete_old_posts(bot, ch, now):
    """
    Удаление своих постов из канала через настроенное число часов после публикации.
    В статистике такие посты остаются засчитанными (статус 'sent' не меняется).
    Нужно право бота в канале: «Удаление сообщений».
    """
    hours = ch["delete_after_h"]
    if not hours:
        return
    limit = int((now - timedelta(hours=hours)).timestamp())
    rows = q(
        "SELECT id, channel_msgs, out_chat FROM posts WHERE channel_id=? AND status='sent' AND deleted=0 "
        "AND channel_msgs IS NOT NULL AND sent_at <= ? ORDER BY sent_at",
        (ch["id"], limit),
    )
    for r in rows:
        ids = [int(x) for x in r["channel_msgs"].split(",") if x]
        try:
            await throttle()
            await bot.delete_messages(chat_id=r["out_chat"] or ch["channel_chat"], message_ids=ids)
            run("UPDATE posts SET deleted=1 WHERE id=?", (r["id"],))
        except TelegramRetryAfter as e:
            logging.warning("Telegram просит подождать %s сек", e.retry_after)
            await asyncio.sleep(e.retry_after)
            return
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            # уже удалено вручную, нет прав, старше 48 часов - не пытаемся снова каждые 10 секунд
            logging.warning("Не удалось удалить пост %s из канала: %s", r["id"], e)
            run("UPDATE posts SET deleted=2 WHERE id=?", (r["id"],))


async def send_due(bot, ch, shift, now):
    """Опубликовать все посты смены канала, время которых наступило."""
    due = q(
        "SELECT * FROM posts WHERE channel_id=? AND shift=? AND status='pending' AND scheduled_at IS NOT NULL "
        "AND scheduled_at <= ? ORDER BY scheduled_at, id",
        (ch["id"], shift.isoformat(), int(now.timestamp())),
    )
    for post in due:
        await send_post(bot, post, ch)


async def announce_schedule(bot, ch, shift, overflow):
    """Сообщение в группу в начале рабочего времени: расписание смены."""
    rows = q("SELECT * FROM posts WHERE channel_id=? AND shift=? AND status IN ('pending','sent') "
             "ORDER BY scheduled_at, id", (ch["id"], shift.isoformat()))
    if not rows:
        return
    lines = [f"<b>🚀 Рабочее время началось ({shift_title(shift)})</b>\nРасписание:"]
    for r in rows:
        if r["scheduled_at"]:
            lines.append(f"{fmt_ts(r['scheduled_at'])} - {post_author(r)}")
    if overflow:
        lines.append(f"\n<b>⚠️ Не хватило мест</b>, эти посты уйдут пачкой в {ch['overflow_time']}: "
                     + ", ".join(post_author(r) for r in overflow))
    await send_long(bot, ch["group_id"], "\n".join(lines))


async def close_shift(bot, ch, shift):
    """
    ВАЖНАЯ ФУНКЦИЯ: завершение смены канала.
    Все неопубликованные посты закрываются (в следующую смену они НЕ переходят),
    в группу уходит итог.
    """
    sh = shift.isoformat()
    cid = ch["id"]
    run("UPDATE posts SET status='expired' WHERE channel_id=? AND shift=? AND status='pending'", (cid, sh))
    run("UPDATE shifts SET closed=1 WHERE channel_id=? AND shift=?", (cid, sh))
    # сохраняем итог смены для статистики: сколько ВП (опубликованных постов) за смену
    vp = q("SELECT COUNT(*) AS c FROM posts WHERE channel_id=? AND shift=? AND status='sent'", (cid, sh))[0]["c"]
    run("INSERT OR REPLACE INTO shift_stats (channel_id, shift, vp_count) VALUES (?, ?, ?)", (cid, sh, vp))
    rows = q("SELECT status, author_id, author_name FROM posts WHERE channel_id=? AND shift=?", (cid, sh))
    if not rows:
        return
    count = lambda st: sum(1 for r in rows if r["status"] == st)
    text = (f"<b>🌅 Смена завершена ({shift_title(shift)})</b>\n"
            f"✅ Опубликовано: {count('sent')}\n"
            f"❌ Ошибки: {count('failed')}\n"
            f"⏳ Не успели выйти: {count('expired')}\n"
            f"🚫 Отменено: {count('cancelled')}")
    if count("duplicate"):
        text += f"\n🔁 Дублей отклонено: {count('duplicate')}"
    missed = [post_author(r) for r in rows if r["status"] == "expired"]
    if missed:
        text += "\nНе опубликованы: " + ", ".join(missed)
    await send_long(bot, ch["group_id"], text)


# =====================================================================
#  ПЛАНИРОВЩИК - ГЛАВНЫЙ ЦИКЛ БОТА
# =====================================================================
_last_cleanup = 0.0


def cleanup_old():
    """
    Хранение данных: только прошлая и текущая недели (две недели, Пн-Вс) - отдельно для каждого канала.
    Всё, что старше понедельника прошлой недели, удаляется. Проверка раз в час.
    """
    global _last_cleanup
    if time.time() - _last_cleanup < 3600:
        return
    _last_cleanup = time.time()
    for ch in q("SELECT * FROM channels"):
        cutoff = week_start(shift_of(now_msk(), ch), 1).isoformat()
        cid = ch["id"]
        run("DELETE FROM post_messages WHERE post_id IN (SELECT id FROM posts WHERE channel_id=? AND shift < ?)",
            (cid, cutoff))
        run("DELETE FROM posts WHERE channel_id=? AND shift < ?", (cid, cutoff))
        run("DELETE FROM shifts WHERE channel_id=? AND shift < ?", (cid, cutoff))
        run("DELETE FROM shift_stats WHERE channel_id=? AND shift < ?", (cid, cutoff))


async def tick_channel(bot, ch):
    """
    Одна проверка одного канала (каждые 10 секунд).
    До начала работы ничего не делает. В рабочее время: расставляет время, объявляет расписание
    и публикует посты, когда их время пришло. После конца работы закрывает смену.
    """
    try:
        now = now_msk()
        await delete_old_posts(bot, ch, now)
        for s in q("SELECT shift, started FROM shifts WHERE channel_id=? AND closed=0 ORDER BY shift", (ch["id"],)):
            shift = date.fromisoformat(s["shift"])
            state = shift_state(shift, now, ch)
            if state == "before":
                continue
            if state == "closed":
                # окно закончилось, но пачка ещё может ждать отправки
                if now <= overflow_dt(shift, ch) + timedelta(minutes=OVERFLOW_GRACE_MIN):
                    plan_shift(shift, now, ch)  # нераспределённые посты (если такие остались) - тоже в пачку
                    await send_due(bot, ch, shift, now)
                    left = q("SELECT COUNT(*) AS c FROM posts WHERE channel_id=? AND shift=? AND status='pending'",
                             (ch["id"], shift.isoformat()))
                    if left[0]["c"] == 0:
                        await close_shift(bot, ch, shift)
                else:
                    await close_shift(bot, ch, shift)
                continue
            # рабочее время идёт
            reschedule_missed(shift, now, ch)
            placed, overflow = plan_shift(shift, now, ch)
            # сначала публикация (она важнее по времени), потом объявление расписания в группу
            await send_due(bot, ch, shift, now)
            if not s["started"]:
                run("UPDATE shifts SET started=1 WHERE channel_id=? AND shift=?", (ch["id"], s["shift"]))
                await announce_schedule(bot, ch, shift, overflow)
    except Exception:
        logging.exception("Ошибка в канале «%s»", ch["name"])  # ошибка одного канала не мешает остальным


async def tick(bot):
    """Одна проверка всех каналов одновременно."""
    cleanup_old()
    chans = q("SELECT * FROM channels WHERE active=1")
    if chans:
        await asyncio.gather(*(tick_channel(bot, ch) for ch in chans))


async def scheduler(bot):
    while True:
        try:
            await tick(bot)
        except Exception:
            logging.exception("Ошибка в планировщике")
        await asyncio.sleep(TICK_SECONDS)


# =====================================================================
#  ЗАПУСК
# =====================================================================
async def main():
    if ":" not in BOT_TOKEN:
        print("Вставьте токен бота в строку BOT_TOKEN в начале файла.")
        return
    init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    task = asyncio.create_task(scheduler(bot))  # noqa: F841 (держим ссылку, чтобы задача не пропала)
    logging.info("Бот запущен")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
