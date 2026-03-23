import logging
import sqlite3
import os
import json
from datetime import date, timedelta
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
import gspread
from google.oauth2.service_account import Credentials

BOT_TOKEN = "8635637632:AAHbN2u39OVfV3-G_xJU0x5TjYVc-hBwfZQ"
ADMIN_ID = 7577571032
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
SPREADSHEET_ID = "1q8J4nDCkuLrh-pJIJJrc1-X6QtZWA_2lrXTy6jo-0k4"

TOUCHES, APPOINTMENTS, REGISTRATIONS = range(3)

logging.basicConfig(level=logging.INFO)


# ─── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheet_client():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise Exception("GOOGLE_CREDENTIALS не найден")
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_month_sheet(gc):
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    month_name = date.today().strftime('%B %Y')
    ru_months = {
        'January': 'Январь', 'February': 'Февраль', 'March': 'Март',
        'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
        'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь',
        'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
    }
    for en, ru in ru_months.items():
        month_name = month_name.replace(en, ru)

    try:
        sheet = spreadsheet.worksheet(month_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=month_name, rows=100, cols=10)
        sheet.append_row(['Агент', 'Взято лидов', 'Назначения', 'Регистрации'])

    return sheet


def update_google_sheet():
    try:
        gc = get_sheet_client()
        sheet = get_or_create_month_sheet(gc)
        agents = get_users_by_role('agent')

        # Получаем все данные за текущий месяц
        today = date.today()
        month_start = today.replace(day=1).isoformat()

        conn = sqlite3.connect('bot.db')
        for agent_id, full_name in agents:
            row = conn.execute('''
                SELECT SUM(touches), SUM(appointments), SUM(registrations)
                FROM reports
                WHERE user_id = ? AND report_date >= ?
            ''', (agent_id, month_start)).fetchone()

            touches = row[0] or 0
            appointments = row[1] or 0
            registrations = row[2] or 0

            # Ищем строку агента в таблице
            cell = sheet.find(full_name)
            if cell:
                sheet.update(f'B{cell.row}:D{cell.row}', [[touches, appointments, registrations]])
            else:
                sheet.append_row([full_name, touches, appointments, registrations])

        conn.close()
        logging.info("Google Sheets обновлён успешно")
    except Exception as e:
        logging.error(f"Ошибка обновления Google Sheets: {e}")


# ─── База данных ───────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect('bot.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        full_name TEXT,
        role TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        report_date TEXT,
        touches INTEGER,
        appointments INTEGER,
        registrations INTEGER
    )''')
    conn.commit()
    conn.close()


def add_user(user_id, full_name, role):
    conn = sqlite3.connect('bot.db')
    conn.execute('INSERT OR REPLACE INTO users VALUES (?, ?, ?)', (user_id, full_name, role))
    conn.commit()
    conn.close()


def remove_user(user_id):
    conn = sqlite3.connect('bot.db')
    conn.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()


def get_user(user_id):
    conn = sqlite3.connect('bot.db')
    row = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    conn.close()
    return row


def get_users_by_role(role):
    conn = sqlite3.connect('bot.db')
    rows = conn.execute('SELECT user_id, full_name FROM users WHERE role = ?', (role,)).fetchall()
    conn.close()
    return rows


def has_submitted_today(user_id):
    today = date.today().isoformat()
    conn = sqlite3.connect('bot.db')
    row = conn.execute('SELECT id FROM reports WHERE user_id = ? AND report_date = ?', (user_id, today)).fetchone()
    conn.close()
    return row is not None


def save_report(user_id, touches, appointments, registrations):
    today = date.today().isoformat()
    conn = sqlite3.connect('bot.db')
    existing = conn.execute('SELECT id FROM reports WHERE user_id = ? AND report_date = ?', (user_id, today)).fetchone()
    if existing:
        conn.execute(
            'UPDATE reports SET touches=?, appointments=?, registrations=? WHERE user_id=? AND report_date=?',
            (touches, appointments, registrations, user_id, today)
        )
    else:
        conn.execute(
            'INSERT INTO reports (user_id, report_date, touches, appointments, registrations) VALUES (?,?,?,?,?)',
            (user_id, today, touches, appointments, registrations)
        )
    conn.commit()
    conn.close()


def get_submitted_ids_today():
    today = date.today().isoformat()
    conn = sqlite3.connect('bot.db')
    rows = conn.execute('SELECT user_id FROM reports WHERE report_date = ?', (today,)).fetchall()
    conn.close()
    return {r[0] for r in rows}


def get_today_reports():
    today = date.today().isoformat()
    conn = sqlite3.connect('bot.db')
    rows = conn.execute('''
        SELECT u.full_name, r.touches, r.appointments, r.registrations
        FROM reports r JOIN users u ON r.user_id = u.user_id
        WHERE r.report_date = ? AND u.role = 'agent'
        ORDER BY u.full_name
    ''', (today,)).fetchall()
    conn.close()
    return rows


def get_week_reports():
    week_ago = (date.today() - timedelta(days=6)).isoformat()
    today = date.today().isoformat()
    conn = sqlite3.connect('bot.db')
    rows = conn.execute('''
        SELECT u.full_name, r.touches, r.appointments, r.registrations
        FROM reports r JOIN users u ON r.user_id = u.user_id
        WHERE r.report_date >= ? AND r.report_date <= ? AND u.role = 'agent'
        ORDER BY u.full_name
    ''', (week_ago, today)).fetchall()
    conn.close()
    return rows


# ─── Текст сводки ─────────────────────────────────────────────────────────────

def build_summary():
    agents = get_users_by_role('agent')
    reports = {r[0]: r for r in get_today_reports()}
    submitted_ids = get_submitted_ids_today()

    text = f"Сводка за {date.today().strftime('%d.%m.%Y')}\n\n"
    total_t = total_a = total_r = 0
    not_submitted = []

    for agent_id, full_name in agents:
        if agent_id in submitted_ids and full_name in reports:
            r = reports[full_name]
            text += f"[{full_name}]\n"
            text += f"  Касания: {r[1]}\n"
            text += f"  Назначения: {r[2]}\n"
            text += f"  Регистрации: {r[3]}\n\n"
            total_t += r[1]
            total_a += r[2]
            total_r += r[3]
        else:
            not_submitted.append(full_name)

    text += f"ИТОГО ПО КОМАНДЕ:\n"
    text += f"  Касания: {total_t}\n"
    text += f"  Назначения: {total_a}\n"
    text += f"  Регистрации: {total_r}\n\n"

    if not_submitted:
        text += "Не сдали отчёт:\n"
        for name in not_submitted:
            text += f"  - {name}\n"
    else:
        text += "Все агенты сдали отчёт!"

    return text


# ─── Проверки доступа ──────────────────────────────────────────────────────────

def is_admin(user_id):
    return user_id == ADMIN_ID


def is_authorized(user_id):
    return is_admin(user_id) or get_user(user_id) is not None


# ─── Команды ──────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if is_admin(user_id):
        await update.message.reply_text(
            "Добро пожаловать, администратор!\n\n"
            "Команды:\n"
            "/addagent [ID] [Имя] — добавить агента\n"
            "/removeagent [ID] — удалить агента\n"
            "/addobserver [ID] [Имя] — добавить наблюдателя\n"
            "/removeobserver [ID] — удалить наблюдателя\n"
            "/agents — список команды\n"
            "/summary — сводка за сегодня\n"
            "/week — статистика за 7 дней"
        )
    elif is_authorized(user_id):
        user = get_user(user_id)
        if user[2] == 'agent':
            await update.message.reply_text(
                f"Привет, {update.effective_user.first_name}!\n"
                "Используй /report чтобы сдать отчёт за сегодня."
            )
        else:
            await update.message.reply_text(
                f"Привет, {update.effective_user.first_name}!\n"
                "Ты будешь получать ежедневную сводку в 01:00."
            )
    else:
        await update.message.reply_text("Вы не находитесь в команде, у вас нет доступа.")


async def addagent_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа.")
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /addagent [ID] [Имя]\n"
            "Пример: /addagent 123456789 Вероника Иванова\n\n"
            "ID агента можно узнать через @userinfobot"
        )
        return
    try:
        agent_id = int(context.args[0])
        full_name = ' '.join(context.args[1:])
        add_user(agent_id, full_name, 'agent')
        await update.message.reply_text(f"Агент {full_name} добавлен!")
    except ValueError:
        await update.message.reply_text("Ошибка: ID должен быть числом.")


async def removeagent_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /removeagent [ID]")
        return
    try:
        agent_id = int(context.args[0])
        user = get_user(agent_id)
        if user:
            remove_user(agent_id)
            await update.message.reply_text(f"Агент {user[1]} удалён!")
        else:
            await update.message.reply_text("Агент не найден.")
    except ValueError:
        await update.message.reply_text("Ошибка: ID должен быть числом.")


async def addobserver_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /addobserver [ID] [Имя]")
        return
    try:
        obs_id = int(context.args[0])
        full_name = ' '.join(context.args[1:])
        add_user(obs_id, full_name, 'observer')
        await update.message.reply_text(f"Наблюдатель {full_name} добавлен!")
    except ValueError:
        await update.message.reply_text("Ошибка: ID должен быть числом.")


async def removeobserver_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /removeobserver [ID]")
        return
    try:
        obs_id = int(context.args[0])
        user = get_user(obs_id)
        if user:
            remove_user(obs_id)
            await update.message.reply_text(f"Наблюдатель {user[1]} удалён!")
        else:
            await update.message.reply_text("Наблюдатель не найден.")
    except ValueError:
        await update.message.reply_text("Ошибка: ID должен быть числом.")


async def agents_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа.")
        return
    agents = get_users_by_role('agent')
    observers = get_users_by_role('observer')

    text = "Список команды:\n\nАГЕНТЫ:\n"
    if agents:
        for uid, name in agents:
            text += f"  {name} (ID: {uid})\n"
    else:
        text += "  пусто\n"

    text += "\nНАБЛЮДАТЕЛИ:\n"
    if observers:
        for uid, name in observers:
            text += f"  {name} (ID: {uid})\n"
    else:
        text += "  пусто\n"

    await update.message.reply_text(text)


async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)
    if not is_admin(user_id) and (not user or user[2] not in ('observer',)):
        await update.message.reply_text("У вас нет доступа.")
        return
    await update.message.reply_text(build_summary())


async def week_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)
    if not is_admin(user_id) and (not user or user[2] not in ('observer',)):
        await update.message.reply_text("У вас нет доступа.")
        return

    reports = get_week_reports()
    if not reports:
        await update.message.reply_text("Нет данных за последние 7 дней.")
        return

    agents_data = {}
    for full_name, touches, appointments, registrations in reports:
        if full_name not in agents_data:
            agents_data[full_name] = {'t': 0, 'a': 0, 'r': 0}
        agents_data[full_name]['t'] += touches
        agents_data[full_name]['a'] += appointments
        agents_data[full_name]['r'] += registrations

    week_ago = (date.today() - timedelta(days=6)).strftime('%d.%m')
    today_str = date.today().strftime('%d.%m')

    text = f"Статистика за 7 дней ({week_ago} - {today_str})\n\n"
    total_t = total_a = total_r = 0

    for name, d in agents_data.items():
        text += f"[{name}]\n"
        text += f"  Касания: {d['t']}\n"
        text += f"  Назначения: {d['a']}\n"
        text += f"  Регистрации: {d['r']}\n\n"
        total_t += d['t']
        total_a += d['a']
        total_r += d['r']

    text += f"ИТОГО ЗА НЕДЕЛЮ:\n"
    text += f"  Касания: {total_t}\n"
    text += f"  Назначения: {total_a}\n"
    text += f"  Регистрации: {total_r}\n"

    await update.message.reply_text(text)


# ─── Отчёт (диалог) ───────────────────────────────────────────────────────────

async def report_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_authorized(user_id):
        await update.message.reply_text("Вы не находитесь в команде, у вас нет доступа.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user or user[2] != 'agent':
        await update.message.reply_text("Эта команда только для агентов.")
        return ConversationHandler.END
    if has_submitted_today(user_id):
        await update.message.reply_text("Ты уже сдал(а) отчёт сегодня!")
        return ConversationHandler.END
    await update.message.reply_text("Начинаем отчёт!\n\nСколько касаний ты сделал(а) сегодня?")
    return TOUCHES


async def get_touches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['touches'] = int(update.message.text.strip())
        await update.message.reply_text("Сколько назначений ты сделал(а) сегодня?")
        return APPOINTMENTS
    except ValueError:
        await update.message.reply_text("Пожалуйста, введи число.")
        return TOUCHES


async def get_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['appointments'] = int(update.message.text.strip())
        await update.message.reply_text("Сколько регистраций ты сделал(а) сегодня?")
        return REGISTRATIONS
    except ValueError:
        await update.message.reply_text("Пожалуйста, введи число.")
        return APPOINTMENTS


async def get_registrations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        registrations = int(update.message.text.strip())
        user_id = update.effective_user.id
        touches = context.user_data['touches']
        appointments = context.user_data['appointments']
        save_report(user_id, touches, appointments, registrations)
        await update.message.reply_text(
            f"Отчёт принят!\n\n"
            f"Касания: {touches}\n"
            f"Назначения: {appointments}\n"
            f"Регистрации: {registrations}"
        )
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("Пожалуйста, введи число.")
        return REGISTRATIONS


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отчёт отменён.")
    return ConversationHandler.END


async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("Вы не находитесь в команде, у вас нет доступа.")


# ─── Планировщик ──────────────────────────────────────────────────────────────

async def send_reminder(app, text):
    agents = get_users_by_role('agent')
    submitted = get_submitted_ids_today()
    for agent_id, _ in agents:
        if agent_id not in submitted:
            try:
                await app.bot.send_message(chat_id=agent_id, text=text)
            except Exception as e:
                logging.error(f"Reminder error for {agent_id}: {e}")


async def reminder_22(app):
    await send_reminder(app, "Напоминание! До конца дня 2 часа. Сдай отчёт командой /report")


async def reminder_23(app):
    await send_reminder(app, "Последнее напоминание! До конца дня 1 час. Сдай отчёт командой /report")


async def daily_summary(app):
    text = build_summary()
    recipients = [ADMIN_ID] + [uid for uid, _ in get_users_by_role('observer')]
    for uid in recipients:
        try:
            await app.bot.send_message(chat_id=uid, text=text)
        except Exception as e:
            logging.error(f"Summary error for {uid}: {e}")
    update_google_sheet()


# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    report_handler = ConversationHandler(
        entry_points=[CommandHandler('report', report_start)],
        states={
            TOUCHES: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_touches)],
            APPOINTMENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_appointments)],
            REGISTRATIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_registrations)],
        },
        fallbacks=[CommandHandler('cancel', cancel), CommandHandler('report', report_start)]
    )

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('addagent', addagent_cmd))
    app.add_handler(CommandHandler('removeagent', removeagent_cmd))
    app.add_handler(CommandHandler('addobserver', addobserver_cmd))
    app.add_handler(CommandHandler('removeobserver', removeobserver_cmd))
    app.add_handler(CommandHandler('agents', agents_cmd))
    app.add_handler(CommandHandler('summary', summary_cmd))
    app.add_handler(CommandHandler('week', week_cmd))
    app.add_handler(report_handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown))

    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)
    scheduler.add_job(reminder_22, 'cron', hour=22, minute=0, args=[app])
    scheduler.add_job(reminder_23, 'cron', hour=23, minute=0, args=[app])
    scheduler.add_job(daily_summary, 'cron', hour=1, minute=0, args=[app])
    scheduler.start()

    app.run_polling()


if __name__ == '__main__':
    main()
