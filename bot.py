import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

import gspread
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from google.oauth2.service_account import Credentials
from telegram import BotCommand, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
BOT_TOKEN = "8635637632:AAHbN2u39OVfV3-G_xJU0x5TjYVc-hBwfZQ"
ADMIN_ID = 7577571032
SPREADSHEET_ID = "1q8J4nDCkuLrh-pJIJJrc1-X6QtZWA_2lrXTy6jo-0k4"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

# ─── Conversation states ───────────────────────────────────────────────────────
REG_LAST_NAME = 0
REG_FIRST_NAME = 1
TOUCHES = 2
APPOINTMENTS = 3
REGISTRATIONS = 4
OBS_ADD_ID = 5
OBS_ADD_NAME = 6
OBS_REMOVE_ID = 7
FIX_ID = 8
FIX_NAME = 9
MANAGE_ACTION = 10
MANAGE_ID = 11

DB_PATH = "bot.db"

MONTH_NAMES_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            role TEXT,
            status TEXT DEFAULT 'active'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            report_date TEXT,
            touches INTEGER,
            appointments INTEGER,
            registrations INTEGER,
            UNIQUE(user_id, report_date)
        )
    """)
    conn.commit()
    try:
        conn.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.close()


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Moscow time helpers ───────────────────────────────────────────────────────

def moscow_today():
    return datetime.now(MOSCOW_TZ).date()

# ─── Google Sheets helpers ─────────────────────────────────────────────────────

def get_sheet_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set")
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_agents_sheet(gc):
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet("Агенты")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="Агенты", rows=1000, cols=3)
        ws.append_row(["ID", "Имя", "Роль"])
    return ws


def get_or_create_month_sheet(gc):
    today = moscow_today()
    sheet_title = f"{MONTH_NAMES_RU[today.month]} {today.year}"
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(sheet_title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_title, rows=1000, cols=4)
        ws.append_row(["Агент", "Касания", "Назначения", "Регистрации"])
    return ws


def save_user_to_sheet(user_id, full_name, role):
    try:
        gc = get_sheet_client()
        ws = get_or_create_agents_sheet(gc)
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and str(row[0]) == str(user_id):
                ws.update(f"A{i + 1}:C{i + 1}", [[str(user_id), full_name, role]])
                return
        ws.append_row([str(user_id), full_name, role])
    except Exception as e:
        logger.error(f"save_user_to_sheet error: {e}")


def remove_user_from_sheet(user_id):
    try:
        gc = get_sheet_client()
        ws = get_or_create_agents_sheet(gc)
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and str(row[0]) == str(user_id):
                ws.delete_rows(i + 1)
                return
    except Exception as e:
        logger.error(f"remove_user_from_sheet error: {e}")


def set_agent_status(user_id, status):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE users SET status = ? WHERE user_id = ?", (status, user_id))
    conn.commit()
    conn.close()


def get_agent_status(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row["status"] if row and row["status"] else "active"


def load_users_from_sheet():
    try:
        gc = get_sheet_client()
        ws = get_or_create_agents_sheet(gc)
        all_values = ws.get_all_values()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        loaded = 0
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if not row or not row[0]:
                continue
            try:
                uid = int(row[0])
                name = row[1] if len(row) > 1 else ""
                role = row[2] if len(row) > 2 else "agent"
                c.execute(
                    "INSERT OR REPLACE INTO users (user_id, full_name, role) VALUES (?, ?, ?)",
                    (uid, name, role),
                )
                loaded += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"load_users_from_sheet skip row {i}: {e}")
        conn.commit()
        conn.close()
        logger.info(f"Loaded {loaded} users from sheet into SQLite")
    except Exception as e:
        logger.error(f"load_users_from_sheet error: {e}")


def update_google_sheet():
    try:
        today_str = moscow_today().isoformat()
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            "SELECT r.user_id, u.full_name, r.touches, r.appointments, r.registrations "
            "FROM reports r JOIN users u ON r.user_id = u.user_id "
            "WHERE r.report_date = ? AND u.role = 'agent'",
            (today_str,),
        )
        rows = c.fetchall()
        conn.close()

        if not rows:
            logger.info("No reports for today, skipping sheet update")
            return

        gc = get_sheet_client()
        ws = get_or_create_month_sheet(gc)
        all_values = ws.get_all_values()

        name_to_row_idx = {}
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0]:
                name_to_row_idx[row[0]] = i + 1  # 1-based sheet row

        for user_id, full_name, touches, appointments, registrations in rows:
            try:
                if full_name in name_to_row_idx:
                    row_idx = name_to_row_idx[full_name]
                    existing = all_values[row_idx - 1]
                    try:
                        ex_t = int(existing[1]) if len(existing) > 1 and existing[1] else 0
                        ex_a = int(existing[2]) if len(existing) > 2 and existing[2] else 0
                        ex_r = int(existing[3]) if len(existing) > 3 and existing[3] else 0
                    except (ValueError, IndexError):
                        ex_t = ex_a = ex_r = 0
                    ws.update(
                        f"A{row_idx}:D{row_idx}",
                        [[full_name, ex_t + touches, ex_a + appointments, ex_r + registrations]],
                    )
                else:
                    ws.append_row([full_name, touches, appointments, registrations])
            except Exception as e:
                logger.error(f"update_google_sheet agent {user_id} error: {e}")
    except Exception as e:
        logger.error(f"update_google_sheet error: {e}")

# ─── DB query helpers ──────────────────────────────────────────────────────────

def is_registered(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None


def get_user(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result


def get_all_agents():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE role = 'agent'")
    result = c.fetchall()
    conn.close()
    return result


def get_all_observers():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE role = 'observer'")
    result = c.fetchall()
    conn.close()
    return result


def get_today_report(user_id):
    today_str = moscow_today().isoformat()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM reports WHERE user_id = ? AND report_date = ?",
        (user_id, today_str),
    )
    result = c.fetchone()
    conn.close()
    return result


def save_report(user_id, touches, appointments, registrations):
    today_str = moscow_today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO reports (user_id, report_date, touches, appointments, registrations) "
        "VALUES (?, ?, ?, ?, ?)",
        (user_id, today_str, touches, appointments, registrations),
    )
    conn.commit()
    conn.close()


def get_agent_week_stats(user_id):
    today = moscow_today()
    monday = today - timedelta(days=today.weekday())
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT COALESCE(SUM(touches),0), COALESCE(SUM(appointments),0), COALESCE(SUM(registrations),0) "
        "FROM reports WHERE user_id = ? AND report_date >= ? AND report_date <= ?",
        (user_id, monday.isoformat(), today.isoformat()),
    )
    result = c.fetchone()
    conn.close()
    return result[0], result[1], result[2]


def get_agent_month_stats(user_id):
    today = moscow_today()
    month_start = today.replace(day=1)
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT COALESCE(SUM(touches),0), COALESCE(SUM(appointments),0), COALESCE(SUM(registrations),0) "
        "FROM reports WHERE user_id = ? AND report_date >= ? AND report_date <= ?",
        (user_id, month_start.isoformat(), today.isoformat()),
    )
    result = c.fetchone()
    conn.close()
    return result[0], result[1], result[2]


def get_agents_without_report_today():
    today_str = moscow_today().isoformat()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT u.user_id, u.full_name FROM users u "
        "WHERE u.role = 'agent' AND u.user_id NOT IN "
        "(SELECT user_id FROM reports WHERE report_date = ?)",
        (today_str,),
    )
    result = c.fetchall()
    conn.close()
    return result

# ─── Summary builders ──────────────────────────────────────────────────────────

def build_summary():
    today = moscow_today()
    today_str = today.isoformat()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT u.full_name, r.touches, r.appointments, r.registrations "
        "FROM users u LEFT JOIN reports r ON u.user_id = r.user_id AND r.report_date = ? "
        "WHERE u.role = 'agent' ORDER BY u.full_name",
        (today_str,),
    )
    rows = c.fetchall()
    conn.close()

    lines = [f"📋 Сводка за {today.strftime('%d.%m.%Y')}\n"]
    total_t = total_a = total_r = 0
    submitted_lines = []
    not_submitted_lines = []

    for row in rows:
        name = row[0]
        if row[1] is not None:
            t, a, r = row[1], row[2], row[3]
            total_t += t
            total_a += a
            total_r += r
            submitted_lines.append(f"✅ {name}: касания={t}, назначения={a}, регистрации={r}")
        else:
            not_submitted_lines.append(f"❌ {name}")

    if submitted_lines:
        lines.append("Сдали отчёт:")
        lines.extend(submitted_lines)
    else:
        lines.append("Никто не сдал отчёт.")

    lines.append(f"\n📊 Итого: касания={total_t}, назначения={total_a}, регистрации={total_r}")

    if not_submitted_lines:
        lines.append("\nНе сдали отчёт:")
        lines.extend(not_submitted_lines)

    return "\n".join(lines)


def build_week_summary():
    today = moscow_today()
    monday = today - timedelta(days=today.weekday())
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT u.user_id, u.full_name, "
        "COALESCE(SUM(r.touches),0), COALESCE(SUM(r.appointments),0), COALESCE(SUM(r.registrations),0) "
        "FROM users u LEFT JOIN reports r ON u.user_id = r.user_id "
        "AND r.report_date >= ? AND r.report_date <= ? "
        "WHERE u.role = 'agent' "
        "GROUP BY u.user_id, u.full_name ORDER BY u.full_name",
        (monday.isoformat(), today.isoformat()),
    )
    rows = c.fetchall()
    conn.close()

    lines = [f"📊 Статистика за неделю\n({monday.strftime('%d.%m')} — {today.strftime('%d.%m.%Y')})\n"]
    total_t = total_a = total_r = 0

    for row in rows:
        name, t, a, r = row[1], row[2], row[3], row[4]
        total_t += t
        total_a += a
        total_r += r
        lines.append(f"👤 {name}: касания={t}, назначения={a}, регистрации={r}")

    lines.append(f"\n📊 Итого: касания={total_t}, назначения={total_a}, регистрации={total_r}")
    return "\n".join(lines)

# ─── Keyboards ─────────────────────────────────────────────────────────────────

def get_agent_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["📊 Сдать отчёт", "✏️ Изменить отчёт"],
            ["📈 Моя статистика", "📋 Мой отчёт сегодня"],
            ["❌ Отмена"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["📋 Сводка сегодня", "📊 Статистика за неделю"],
            ["👥 Список команды", "❓ Кто не сдал"],
            ["➕ Добавить наблюдателя", "➖ Удалить наблюдателя"],
            ["✏️ Изменить имя агента"],
            ["👤 Управление агентами"],
            ["❌ Отмена"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def get_observer_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["📋 Статистика за день", "📊 Статистика за неделю"],
            ["❓ Кто не сдал?"],
            ["❌ Отмена"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

# ─── Registration handlers ─────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "👋 Привет, Администратор!", reply_markup=get_admin_keyboard()
        )
        return ConversationHandler.END

    user = get_user(user_id)
    if user:
        if user["role"] == "observer":
            await update.message.reply_text(
                f"👋 Привет, {user['full_name']}!",
                reply_markup=get_observer_keyboard()
            )
        else:
            await update.message.reply_text(
                f"👋 Привет, {user['full_name']}!", reply_markup=get_agent_keyboard()
            )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 Добро пожаловать! Давай зарегистрируемся.\n\nВведи свою фамилию:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return REG_LAST_NAME


async def reg_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text:
        await update.message.reply_text("Пожалуйста, введи фамилию:")
        return REG_LAST_NAME
    context.user_data["last_name"] = text
    await update.message.reply_text("Теперь введи своё имя:")
    return REG_FIRST_NAME


async def reg_first_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if not text:
        await update.message.reply_text("Пожалуйста, введи имя:")
        return REG_FIRST_NAME

    last_name = context.user_data.get("last_name", "")
    full_name = f"{last_name} {text}"

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO users (user_id, full_name, role) VALUES (?, ?, ?)",
        (user_id, full_name, "agent"),
    )
    conn.commit()
    conn.close()

    try:
        save_user_to_sheet(user_id, full_name, "agent")
    except Exception as e:
        logger.error(f"save_user_to_sheet on registration error: {e}")

    await update.message.reply_text(
        f"✅ Регистрация завершена!\nТвоё имя: {full_name}",
        reply_markup=get_agent_keyboard(),
    )
    return ConversationHandler.END

# ─── Cancel ────────────────────────────────────────────────────────────────────

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data.clear()
    if user_id == ADMIN_ID:
        await update.message.reply_text("Отменено.", reply_markup=get_admin_keyboard())
    else:
        user = get_user(user_id)
        if user:
            if user["role"] == "observer":
                await update.message.reply_text("Отменено.", reply_markup=get_observer_keyboard())
            else:
                await update.message.reply_text("Отменено.", reply_markup=get_agent_keyboard())
        else:
            await update.message.reply_text("Отменено.")
    return ConversationHandler.END

# ─── Report handlers ───────────────────────────────────────────────────────────

async def report_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_registered(user_id):
        await update.message.reply_text("Сначала зарегистрируйся через /start")
        return ConversationHandler.END

    if get_agent_status(user_id) == 'blocked':
        await update.message.reply_text("⛔ Вы заблокированы и не можете сдавать отчёты.")
        return ConversationHandler.END

    if get_today_report(user_id):
        await update.message.reply_text(
            "Ты уже сдал(а) отчёт! Используй ✏️ Изменить отчёт",
            reply_markup=get_agent_keyboard(),
        )
        return ConversationHandler.END

    context.user_data["edit_mode"] = False
    await update.message.reply_text(
        "📊 Сдача отчёта\n\nСколько было касаний сегодня? (введи число)",
        reply_markup=ReplyKeyboardRemove(),
    )
    return TOUCHES


async def report_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_registered(user_id):
        await update.message.reply_text("Сначала зарегистрируйся через /start")
        return ConversationHandler.END

    if get_agent_status(user_id) == 'blocked':
        await update.message.reply_text("⛔ Вы заблокированы и не можете сдавать отчёты.")
        return ConversationHandler.END

    context.user_data["edit_mode"] = True
    await update.message.reply_text(
        "✏️ Изменение отчёта\n\nСколько было касаний сегодня? (введи число)",
        reply_markup=ReplyKeyboardRemove(),
    )
    return TOUCHES


async def report_touches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        val = int(text)
        if val < 0:
            raise ValueError("negative")
    except ValueError:
        await update.message.reply_text("Введи корректное число (0 или больше):")
        return TOUCHES
    context.user_data["touches"] = val
    await update.message.reply_text("Сколько назначений было сегодня? (введи число)")
    return APPOINTMENTS


async def report_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        val = int(text)
        if val < 0:
            raise ValueError("negative")
    except ValueError:
        await update.message.reply_text("Введи корректное число (0 или больше):")
        return APPOINTMENTS
    context.user_data["appointments"] = val
    await update.message.reply_text("Сколько регистраций было сегодня? (введи число)")
    return REGISTRATIONS


async def report_registrations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    try:
        val = int(text)
        if val < 0:
            raise ValueError("negative")
    except ValueError:
        await update.message.reply_text("Введи корректное число (0 или больше):")
        return REGISTRATIONS

    touches = context.user_data.get("touches", 0)
    appointments = context.user_data.get("appointments", 0)
    registrations = val

    save_report(user_id, touches, appointments, registrations)
    context.user_data.clear()

    user = get_user(user_id)
    name = user["full_name"] if user else "Агент"
    today_str = moscow_today().strftime("%d.%m.%Y")

    await update.message.reply_text(
        f"✅ Отчёт сохранён!\n\n"
        f"📅 Дата: {today_str}\n"
        f"👤 {name}\n"
        f"Касания: {touches}\n"
        f"Назначения: {appointments}\n"
        f"Регистрации: {registrations}",
        reply_markup=get_agent_keyboard(),
    )
    return ConversationHandler.END

# ─── Agent info handlers ───────────────────────────────────────────────────────

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_registered(user_id):
        await update.message.reply_text("Сначала зарегистрируйся через /start")
        return

    user = get_user(user_id)
    name = user["full_name"] if user else "Агент"

    w_t, w_a, w_r = get_agent_week_stats(user_id)
    m_t, m_a, m_r = get_agent_month_stats(user_id)

    today = moscow_today()
    monday = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    text = (
        f"📈 Статистика для {name}\n\n"
        f"📅 Неделя ({monday.strftime('%d.%m')} — {today.strftime('%d.%m')}):\n"
        f"  Касания: {w_t}\n"
        f"  Назначения: {w_a}\n"
        f"  Регистрации: {w_r}\n\n"
        f"📆 Месяц ({month_start.strftime('%d.%m')} — {today.strftime('%d.%m')}):\n"
        f"  Касания: {m_t}\n"
        f"  Назначения: {m_a}\n"
        f"  Регистрации: {m_r}"
    )
    await update.message.reply_text(text, reply_markup=get_agent_keyboard())


async def my_today_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_registered(user_id):
        await update.message.reply_text("Сначала зарегистрируйся через /start")
        return

    report = get_today_report(user_id)
    if not report:
        await update.message.reply_text("📋 Отчёт ещё не сдан", reply_markup=get_agent_keyboard())
        return

    today_str = moscow_today().strftime("%d.%m.%Y")
    text = (
        f"📋 Твой отчёт за {today_str}:\n\n"
        f"Касания: {report['touches']}\n"
        f"Назначения: {report['appointments']}\n"
        f"Регистрации: {report['registrations']}"
    )
    await update.message.reply_text(text, reply_markup=get_agent_keyboard())

# ─── Admin handlers ────────────────────────────────────────────────────────────

async def admin_summary_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_summary(), reply_markup=get_admin_keyboard())


async def admin_week_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_week_summary(), reply_markup=get_admin_keyboard())


async def admin_agents_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    agents = get_all_agents()
    observers = get_all_observers()

    lines = ["👥 Список команды\n\nАгенты:"]
    if agents:
        for a in agents:
            lines.append(f"  ID: {a['user_id']} — {a['full_name']}")
    else:
        lines.append("  (нет агентов)")

    lines.append("\nНаблюдатели:")
    if observers:
        for o in observers:
            lines.append(f"  ID: {o['user_id']} — {o['full_name']}")
    else:
        lines.append("  (нет наблюдателей)")

    await update.message.reply_text("\n".join(lines), reply_markup=get_admin_keyboard())


async def admin_who_didnt_submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    not_submitted = get_agents_without_report_today()
    if not not_submitted:
        text = "✅ Все агенты сдали отчёт сегодня!"
    else:
        lines = ["❓ Не сдали отчёт сегодня:\n"]
        for row in not_submitted:
            lines.append(f"  ❌ {row['full_name']} (ID: {row['user_id']})")
        text = "\n".join(lines)
    await update.message.reply_text(text, reply_markup=get_admin_keyboard())


# Observer add flow
async def admin_add_observer_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    await update.message.reply_text(
        "Введи Telegram ID нового наблюдателя:", reply_markup=ReplyKeyboardRemove()
    )
    return OBS_ADD_ID


async def admin_add_observer_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        uid = int(text)
    except ValueError:
        await update.message.reply_text("Введи корректный ID (только цифры):")
        return OBS_ADD_ID
    context.user_data["obs_id"] = uid
    await update.message.reply_text("Введи имя наблюдателя (Фамилия Имя):")
    return OBS_ADD_NAME


async def admin_add_observer_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    uid = context.user_data.get("obs_id")
    if not name or not uid:
        await update.message.reply_text("Ошибка. Попробуй снова.", reply_markup=get_admin_keyboard())
        return ConversationHandler.END

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO users (user_id, full_name, role) VALUES (?, ?, ?)",
        (uid, name, "observer"),
    )
    conn.commit()
    conn.close()

    try:
        save_user_to_sheet(uid, name, "observer")
    except Exception as e:
        logger.error(f"save observer to sheet error: {e}")

    context.user_data.clear()
    await update.message.reply_text(
        f"✅ Наблюдатель {name} (ID: {uid}) добавлен.", reply_markup=get_admin_keyboard()
    )
    return ConversationHandler.END


# Observer remove flow
async def admin_remove_observer_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    await update.message.reply_text(
        "Введи Telegram ID наблюдателя для удаления:", reply_markup=ReplyKeyboardRemove()
    )
    return OBS_REMOVE_ID


async def admin_remove_observer_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        uid = int(text)
    except ValueError:
        await update.message.reply_text("Введи корректный ID (только цифры):")
        return OBS_REMOVE_ID

    user = get_user(uid)
    if not user or user["role"] != "observer":
        await update.message.reply_text(
            f"Наблюдатель с ID {uid} не найден.", reply_markup=get_admin_keyboard()
        )
        return ConversationHandler.END

    name = user["full_name"]
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE user_id = ?", (uid,))
    conn.commit()
    conn.close()

    try:
        remove_user_from_sheet(uid)
    except Exception as e:
        logger.error(f"remove observer from sheet error: {e}")

    await update.message.reply_text(
        f"✅ Наблюдатель {name} (ID: {uid}) удалён.", reply_markup=get_admin_keyboard()
    )
    return ConversationHandler.END


# Fix agent name flow
async def admin_fix_name_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    await update.message.reply_text(
        "Введи Telegram ID агента, имя которого нужно изменить:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return FIX_ID


async def admin_fix_name_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        uid = int(text)
    except ValueError:
        await update.message.reply_text("Введи корректный ID (только цифры):")
        return FIX_ID

    user = get_user(uid)
    if not user:
        await update.message.reply_text(
            f"Пользователь с ID {uid} не найден.", reply_markup=get_admin_keyboard()
        )
        return ConversationHandler.END

    context.user_data["fix_id"] = uid
    context.user_data["fix_old_name"] = user["full_name"]
    await update.message.reply_text(
        f"Текущее имя: {user['full_name']}\n\nВведи новое имя (Фамилия Имя):"
    )
    return FIX_NAME


async def admin_fix_name_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text.strip()
    uid = context.user_data.get("fix_id")
    old_name = context.user_data.get("fix_old_name", "")

    if not new_name or not uid:
        await update.message.reply_text("Ошибка. Попробуй снова.", reply_markup=get_admin_keyboard())
        return ConversationHandler.END

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE users SET full_name = ? WHERE user_id = ?", (new_name, uid))
    conn.commit()
    conn.close()

    try:
        gc = get_sheet_client()
        ws = get_or_create_agents_sheet(gc)
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and str(row[0]) == str(uid):
                ws.update(f"B{i + 1}", [[new_name]])
                break
    except Exception as e:
        logger.error(f"admin_fix_name sheet update error: {e}")

    context.user_data.clear()
    await update.message.reply_text(
        f"✅ Имя агента изменено:\n{old_name} → {new_name}", reply_markup=get_admin_keyboard()
    )
    return ConversationHandler.END


# ─── Manage agents handlers ─────────────────────────────────────────────

async def admin_manage_agents_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END
    keyboard = ReplyKeyboardMarkup(
        [["🗑 Удалить агента", "🚫 Заблокировать агента"],
         ["✅ Разблокировать агента", "◀️ Назад"]],
        resize_keyboard=True
    )
    await update.message.reply_text("Выбери действие:", reply_markup=keyboard)
    return MANAGE_ACTION


async def admin_manage_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "◀️ Назад":
        await update.message.reply_text("Главное меню", reply_markup=get_admin_keyboard())
        return ConversationHandler.END
    action_map = {
        "🗑 Удалить агента": "delete",
        "🚫 Заблокировать агента": "block",
        "✅ Разблокировать агента": "unblock"
    }
    if text not in action_map:
        await update.message.reply_text("Выбери действие из меню:")
        return MANAGE_ACTION
    context.user_data["manage_action"] = action_map[text]
    prompts = {
        "delete": "Введи Telegram ID агента для удаления:",
        "block": "Введи Telegram ID агента для блокировки:",
        "unblock": "Введи Telegram ID агента для разблокировки:"
    }
    await update.message.reply_text(prompts[action_map[text]], reply_markup=ReplyKeyboardRemove())
    return MANAGE_ID


async def admin_manage_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        uid = int(text)
    except ValueError:
        await update.message.reply_text("Введи корректный ID (только цифры):")
        return MANAGE_ID

    user = get_user(uid)
    if not user or user["role"] != "agent":
        await update.message.reply_text(f"Агент с ID {uid} не найден.", reply_markup=get_admin_keyboard())
        return ConversationHandler.END

    action = context.user_data.get("manage_action")
    name = user["full_name"]

    if action == "delete":
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM users WHERE user_id = ?", (uid,))
        conn.commit()
        conn.close()
        try:
            remove_user_from_sheet(uid)
        except Exception as e:
            logger.error(f"remove agent from sheet error: {e}")
        msg = f"✅ Агент {name} (ID: {uid}) удалён."
    elif action == "block":
        set_agent_status(uid, "blocked")
        msg = f"🚫 Агент {name} (ID: {uid}) заблокирован."
    elif action == "unblock":
        set_agent_status(uid, "active")
        msg = f"✅ Агент {name} (ID: {uid}) разблокирован."
    else:
        msg = "Неизвестное действие."

    context.user_data.clear()
    await update.message.reply_text(msg, reply_markup=get_admin_keyboard())
    return ConversationHandler.END

# ─── Observer handlers ─────────────────────────────────────────────────────────

async def observer_stats_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    if not user or user["role"] != "observer":
        return
    await update.message.reply_text(build_summary(), reply_markup=get_observer_keyboard())


async def observer_stats_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    if not user or user["role"] != "observer":
        return
    await update.message.reply_text(build_week_summary(), reply_markup=get_observer_keyboard())


async def observer_who_didnt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    if not user or user["role"] != "observer":
        return
    not_submitted = get_agents_without_report_today()
    if not not_submitted:
        text = "✅ Все агенты сдали отчёт сегодня!"
    else:
        lines = ["❓ Не сдали отчёт сегодня:\n"]
        for row in not_submitted:
            lines.append(f"  ❌ {row['full_name']}")
        text = "\n".join(lines)
    await update.message.reply_text(text, reply_markup=get_observer_keyboard())

# ─── Admin commands (/summary, /week, /agents) ─────────────────────────────────

async def cmd_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_summary(), reply_markup=get_admin_keyboard())


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(build_week_summary(), reply_markup=get_admin_keyboard())


async def cmd_agents(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await admin_agents_list(update, context)

# ─── Unknown / fallback for unregistered ──────────────────────────────────────

async def unknown_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_registered(user_id) and user_id != ADMIN_ID:
        await update.message.reply_text(
            "Привет! Для начала работы введи /start и пройди регистрацию."
        )

# ─── Scheduler jobs ────────────────────────────────────────────────────────────

async def _send_reminder_to_unsent(bot, message: str):
    agents_without = get_agents_without_report_today()
    for row in agents_without:
        try:
            await bot.send_message(chat_id=row["user_id"], text=message)
        except Exception as e:
            logger.error(f"send_reminder to {row['user_id']} error: {e}")


def make_scheduler_jobs(bot):
    """Return all scheduler job coroutine factories bound to the given bot."""

    async def job_reminder_23():
        try:
            await _send_reminder_to_unsent(bot, "⏰ До конца дня 1 час. Не забудь сдать отчёт!")
        except Exception as e:
            logger.error(f"job_reminder_23 error: {e}")

    async def job_reminder_2330():
        try:
            await _send_reminder_to_unsent(bot, "⏰ До конца дня 30 минут. Успей сдать отчёт!")
        except Exception as e:
            logger.error(f"job_reminder_2330 error: {e}")

    async def job_reminder_2355():
        try:
            await _send_reminder_to_unsent(
                bot,
                "🚨 До конца дня 5 минут! Срочно сдай отчёт командой 📊 Сдать отчёт",
            )
        except Exception as e:
            logger.error(f"job_reminder_2355 error: {e}")

    async def job_daily_summary():
        try:
            text = build_summary()
            await bot.send_message(chat_id=ADMIN_ID, text=text)
            for obs in get_all_observers():
                try:
                    await bot.send_message(chat_id=obs["user_id"], text=text)
                except Exception as e:
                    logger.error(f"job_daily_summary observer {obs['user_id']} error: {e}")
            update_google_sheet()
        except Exception as e:
            logger.error(f"job_daily_summary error: {e}")

    async def job_weekly_stats():
        try:
            today = moscow_today()
            monday = today - timedelta(days=today.weekday())
            for agent in get_all_agents():
                try:
                    w_t, w_a, w_r = get_agent_week_stats(agent["user_id"])
                    text = (
                        f"📊 Твоя статистика за неделю\n"
                        f"({monday.strftime('%d.%m')} — {today.strftime('%d.%m.%Y')})\n\n"
                        f"Касания: {w_t}\n"
                        f"Назначения: {w_a}\n"
                        f"Регистрации: {w_r}"
                    )
                    await bot.send_message(chat_id=agent["user_id"], text=text)
                except Exception as e:
                    logger.error(f"job_weekly_stats agent {agent['user_id']} error: {e}")

            week_text = build_week_summary()
            await bot.send_message(chat_id=ADMIN_ID, text=week_text)
            for obs in get_all_observers():
                try:
                    await bot.send_message(chat_id=obs["user_id"], text=week_text)
                except Exception as e:
                    logger.error(f"job_weekly_stats observer {obs['user_id']} error: {e}")
        except Exception as e:
            logger.error(f"job_weekly_stats error: {e}")

    async def job_monday_motivation():
        try:
            text = "💪 Новая неделя — новые результаты! Не забывай сдавать отчёт каждый день до 00:00"
            for agent in get_all_agents():
                try:
                    await bot.send_message(chat_id=agent["user_id"], text=text)
                except Exception as e:
                    logger.error(f"job_monday_motivation agent {agent['user_id']} error: {e}")
        except Exception as e:
            logger.error(f"job_monday_motivation error: {e}")

    return (
        job_reminder_23,
        job_reminder_2330,
        job_reminder_2355,
        job_daily_summary,
        job_weekly_stats,
        job_monday_motivation,
    )

# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    init_db()
    load_users_from_sheet()

    application = Application.builder().token(BOT_TOKEN).build()

    # ── Registration ConversationHandler ──
    registration_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REG_LAST_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_last_name)],
            REG_FIRST_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_first_name)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
        ],
        allow_reentry=True,
    )

    # ── Report ConversationHandler ──
    report_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^📊 Сдать отчёт$"), report_start),
            MessageHandler(filters.Regex("^✏️ Изменить отчёт$"), report_edit_start),
        ],
        states={
            TOUCHES: [
                MessageHandler(filters.Regex("^📊 Сдать отчёт$"), report_start),
                MessageHandler(filters.Regex("^✏️ Изменить отчёт$"), report_edit_start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_touches),
            ],
            APPOINTMENTS: [
                MessageHandler(filters.Regex("^📊 Сдать отчёт$"), report_start),
                MessageHandler(filters.Regex("^✏️ Изменить отчёт$"), report_edit_start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_appointments),
            ],
            REGISTRATIONS: [
                MessageHandler(filters.Regex("^📊 Сдать отчёт$"), report_start),
                MessageHandler(filters.Regex("^✏️ Изменить отчёт$"), report_edit_start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_registrations),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
        ],
        allow_reentry=True,
    )

    # ── Admin ConversationHandler ──
    admin_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^➕ Добавить наблюдателя$"), admin_add_observer_start),
            MessageHandler(filters.Regex("^➖ Удалить наблюдателя$"), admin_remove_observer_start),
            MessageHandler(filters.Regex("^✏️ Изменить имя агента$"), admin_fix_name_start),
        ],
        states={
            OBS_ADD_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_observer_id)],
            OBS_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_observer_name)],
            OBS_REMOVE_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_observer_id)],
            FIX_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_fix_name_id)],
            FIX_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_fix_name_new)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
        ],
        allow_reentry=True,
    )

    # ── Manage agents ConversationHandler ──
    manage_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^👤 Управление агентами$"), admin_manage_agents_start),
        ],
        states={
            MANAGE_ACTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_manage_action)],
            MANAGE_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_manage_id)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
        ],
        allow_reentry=True,
    )

    application.add_handler(registration_handler)
    application.add_handler(report_handler)
    application.add_handler(admin_conv_handler)
    application.add_handler(manage_conv_handler)

    # ── Standalone message handlers ──
    application.add_handler(MessageHandler(filters.Regex("^❌ Отмена$"), cancel))
    application.add_handler(MessageHandler(filters.Regex("^📈 Моя статистика$"), my_stats))
    application.add_handler(MessageHandler(filters.Regex("^📋 Мой отчёт сегодня$"), my_today_report))
    application.add_handler(MessageHandler(filters.Regex("^📋 Сводка сегодня$"), admin_summary_today))
    application.add_handler(MessageHandler(filters.Regex("^📊 Статистика за неделю$"), admin_week_stats))
    application.add_handler(MessageHandler(filters.Regex("^👥 Список команды$"), admin_agents_list))
    application.add_handler(MessageHandler(filters.Regex("^❓ Кто не сдал$"), admin_who_didnt_submit))
    application.add_handler(MessageHandler(filters.Regex("^📋 Статистика за день$"), observer_stats_today))
    application.add_handler(MessageHandler(filters.Regex("^📊 Статистика за неделю$"), observer_stats_week))
    application.add_handler(MessageHandler(filters.Regex("^❓ Кто не сдал\\?$"), observer_who_didnt))

    # ── Admin slash commands ──
    application.add_handler(CommandHandler("summary", cmd_summary))
    application.add_handler(CommandHandler("week", cmd_week))
    application.add_handler(CommandHandler("agents", cmd_agents))

    # ── Fallback for unregistered users ──
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_user))

    # ── Scheduler setup ──
    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

    async def post_init(app: Application):
        (
            job_reminder_23,
            job_reminder_2330,
            job_reminder_2355,
            job_daily_summary,
            job_weekly_stats,
            job_monday_motivation,
        ) = make_scheduler_jobs(app.bot)

        scheduler.add_job(job_reminder_23, "cron", hour=23, minute=0,
                          misfire_grace_time=300, coalesce=True)
        scheduler.add_job(job_reminder_2330, "cron", hour=23, minute=30,
                          misfire_grace_time=300, coalesce=True)
        scheduler.add_job(job_reminder_2355, "cron", hour=23, minute=55,
                          misfire_grace_time=300, coalesce=True)
        scheduler.add_job(job_daily_summary, "cron", hour=0, minute=10,
                          misfire_grace_time=300, coalesce=True)
        scheduler.add_job(job_weekly_stats, "cron", day_of_week="sun", hour=9, minute=0,
                          misfire_grace_time=300, coalesce=True)
        scheduler.add_job(job_monday_motivation, "cron", day_of_week="mon", hour=9, minute=0,
                          misfire_grace_time=300, coalesce=True)

        scheduler.start()
        logger.info("Scheduler started with all jobs")

        await app.bot.set_my_commands([
            BotCommand("start", "Главное меню"),
            BotCommand("cancel", "Отменить действие"),
        ])

    async def post_shutdown(app: Application):
        if scheduler.running:
            scheduler.shutdown()
            logger.info("Scheduler stopped")

    application.post_init = post_init
    application.post_shutdown = post_shutdown

    logger.info("Bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

