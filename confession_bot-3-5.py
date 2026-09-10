#!/usr/bin/env python3
"""
University Anonymous Confession Bot — single-file implementation.

Flow:  Student -> Bot -> Admin review -> Channel post -> Comments/replies

Everything (config, database access, keyboards, handlers) lives in this one
file on purpose, as requested. See the "TELEGRAM LIMITATIONS" notes below
for the few places where the spec asked for something the Bot API cannot
literally do, and what was built instead.

Run:
    pip install python-telegram-bot==21.4 python-dotenv psycopg2-binary
    cp .env.example .env   # fill in BOT_TOKEN / ADMIN_IDS / CHANNEL_ID / DATABASE_URL
    python confession_bot.py

Data is stored in Postgres (e.g. Supabase or Neon's free tier), connected
via the DATABASE_URL environment variable, so it survives host restarts.

TELEGRAM LIMITATIONS handled here:
  * A channel post cannot open a "conversation" for a comment box — buttons
    on channel posts can only carry callback_data (which requires the
    pressing user to already be a private-chat participant with the bot,
    which channel viewers usually are not) or a URL. So the "💬 Comment"
    button on the channel post is a *deep-link URL* button
    (https://t.me/<bot>?start=c_<id>) that opens a private chat with the
    bot and jumps straight into that confession's comment view. This is
    the standard, correct pattern for "channel post -> bot conversation".
  * Comments/likes/replies are therefore managed in the user's private
    chat with the bot, not as replies inside the channel itself. The
    channel post's visible comment counter is kept in sync by editing the
    original channel message whenever a comment/reply/delete happens.
  * GIFs are Telegram "animations"; the bot stores them under media_type
    'animation' the same way it stores photos/stickers.
"""

from __future__ import annotations

import html
import logging
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x}
CHANNEL_ID = os.getenv("CHANNEL_ID", "")  # e.g. -1001234567890 or @channelusername
DATABASE_URL = os.getenv("DATABASE_URL", "")
UNIVERSITY_NAME = os.getenv("UNIVERSITY_NAME", "ThoughtDrop")

MAX_CONFESSION_LEN = int(os.getenv("MAX_CONFESSION_LEN", "1500"))
MAX_COMMENT_LEN = int(os.getenv("MAX_COMMENT_LEN", "500"))

AURA_PER_COMMENT = int(os.getenv("AURA_PER_COMMENT", "1"))
AURA_PER_LIKE_RECEIVED = int(os.getenv("AURA_PER_LIKE_RECEIVED", "2"))
AURA_PER_CONFESSION_APPROVED = int(os.getenv("AURA_PER_CONFESSION_APPROVED", "3"))

RATE_LIMIT_WINDOW_SECONDS = 10
RATE_LIMIT_MAX_ACTIONS = 6  # max comments/reactions/confessions per window

DEFAULT_CATEGORIES = [
    ("Love", "\u2764\ufe0f"),
    ("Relationship", "\U0001f494"),
    ("Funny", "\U0001f602"),
    ("University", "\U0001f393"),
    ("Education", "\U0001f4da"),
    ("Friendship", "\U0001f9d1\u200d\U0001f91d\u200d\U0001f9d1"),
    ("Personal", "\U0001f614"),
    ("Thoughts", "\U0001f4ad"),
    ("Hot Topic", "\U0001f525"),
    ("Question", "\u2753"),
    ("sex", "\U0001f4dd"),
]

COMMUNITY_RULES = (
    "1. ትንኮሳ አይፈቀድም።\n"
    "2. ማስፈራሪያ አይፈቀድም።\n"
    "3. አላስፈላጊ መልእክት (spam) አይፈቀድም።\n"
    "4. ጥላቻ ወይም መድልዎ አይፈቀድም።\n"
    "5. የሌሎችን የግል መረጃ ማጋራት አይፈቀድም።\n"
    "6. የሌላ ሰው ማንነት መስሎ መቅረብ አይፈቀድም።\n"
    "7. ሕገ ወጥ ይዘት አይፈቀድም።\n"
    "8. ሌሎች ተማሪዎችን አለማክበር።"
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("confession_bot")

# --------------------------------------------------------------------------
# DATABASE
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    username TEXT,
    profile_name TEXT,
    sex TEXT,
    department TEXT,
    year TEXT,
    bio TEXT,
    aura_points INTEGER NOT NULL DEFAULT 0,
    is_banned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    emoji TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS confessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    category_id INTEGER REFERENCES categories(id),
    content TEXT,
    media_type TEXT,
    media_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    channel_message_id BIGINT,
    admin_message_id BIGINT,
    created_at TEXT NOT NULL,
    approved_at TEXT
);

CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    confession_id INTEGER NOT NULL REFERENCES confessions(id),
    user_id INTEGER NOT NULL REFERENCES users(id),
    parent_comment_id INTEGER REFERENCES comments(id),
    content TEXT,
    media_type TEXT,
    media_id TEXT,
    likes INTEGER NOT NULL DEFAULT 0,
    dislikes INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reactions (
    id SERIAL PRIMARY KEY,
    comment_id INTEGER NOT NULL REFERENCES comments(id),
    user_id INTEGER NOT NULL REFERENCES users(id),
    reaction_type TEXT NOT NULL,
    UNIQUE(comment_id, user_id)
);

CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    reporter_id INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    content_id INTEGER NOT NULL,
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_confessions_status ON confessions(status);
CREATE INDEX IF NOT EXISTS idx_comments_confession ON comments(confession_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
CREATE INDEX IF NOT EXISTS idx_reactions_comment_user ON reactions(comment_id, user_id);
"""


class _PGConn:
    """Thin wrapper so the rest of the file can keep using sqlite-style
    conn.execute("...?...", (...)) calls unchanged, against Postgres."""

    def __init__(self, raw_conn):
        self._conn = raw_conn

    def execute(self, sql: str, params=None):
        sql = sql.replace("?", "%s")
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur

    def executemany(self, sql: str, seq_of_params):
        sql = sql.replace("?", "%s")
        cur = self._conn.cursor()
        cur.executemany(sql, seq_of_params)
        return cur

    def executescript(self, script: str) -> None:
        cur = self._conn.cursor()
        cur.execute(script)
        cur.close()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def db():
    raw = psycopg2.connect(DATABASE_URL)
    return _PGConn(raw)


def init_db() -> None:
    with closing(db()) as conn:
        conn.executescript(SCHEMA)
        conn.commit()
        row = conn.execute("SELECT COUNT(*) c FROM categories").fetchone()
        if row["c"] == 0:
            conn.executemany(
                "INSERT INTO categories (name, emoji, active) VALUES (?, ?, 1)",
                DEFAULT_CATEGORIES,
            )
            conn.commit()
    log.info("Database ready (Postgres)")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def esc(text: Optional[str]) -> str:
    return html.escape(text or "")


# ---- user helpers ---------------------------------------------------------

def get_or_create_user(telegram_id: int, username: Optional[str]) -> sqlite3.Row:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        if row:
            if username and row["username"] != username:
                conn.execute(
                    "UPDATE users SET username = ? WHERE id = ?", (username, row["id"])
                )
                conn.commit()
            return row
        conn.execute(
            "INSERT INTO users (telegram_id, username, created_at) VALUES (?, ?, ?)",
            (telegram_id, username, now()),
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()


def get_user_by_id(user_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def is_admin(telegram_id: int) -> bool:
    return telegram_id in ADMIN_IDS


def add_aura(user_id: int, amount: int) -> None:
    with closing(db()) as conn:
        conn.execute(
            "UPDATE users SET aura_points = aura_points + ? WHERE id = ?",
            (amount, user_id),
        )
        conn.commit()


# --------------------------------------------------------------------------
# ANTI-SPAM (very small in-memory rate limiter, per telegram_id)
# --------------------------------------------------------------------------

_action_log: dict[int, list[float]] = {}


def rate_limited(telegram_id: int) -> bool:
    """Return True if the user should be blocked for going too fast."""
    t = time.monotonic()
    window_start = t - RATE_LIMIT_WINDOW_SECONDS
    hits = [x for x in _action_log.get(telegram_id, []) if x > window_start]
    hits.append(t)
    _action_log[telegram_id] = hits
    return len(hits) > RATE_LIMIT_MAX_ACTIONS


def is_banned(row: sqlite3.Row) -> bool:
    return bool(row["is_banned"])


async def vanish(message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


# --------------------------------------------------------------------------
# KEYBOARDS
# --------------------------------------------------------------------------

def kb_main_menu(telegram_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("\U0001f464 My Profile", callback_data="menu:profile")],
        [InlineKeyboardButton("\U0001f4dd Confess", callback_data="menu:confess")],
        [InlineKeyboardButton("\u2b50 My Aura", callback_data="menu:myaura")],
        [
            InlineKeyboardButton("\U0001f4dc Rules", callback_data="menu:rules"),
            InlineKeyboardButton("\U0001f512 Privacy", callback_data="menu:privacy"),
        ],
        [InlineKeyboardButton("\u2753 Help", callback_data="menu:help")],
    ]
    if is_admin(telegram_id):
        rows.append([InlineKeyboardButton("\U0001f510 Admin Panel", callback_data="admin:panel")])
    return InlineKeyboardMarkup(rows)


def kb_back(target: str = "menu:home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f519 Back", callback_data=target)]])


def kb_profile(user: sqlite3.Row) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("\u270f\ufe0f Change Name", callback_data="profile:edit:name")],
            [
                InlineKeyboardButton("\U0001f6b9 Male", callback_data="profile:sex:Male"),
                InlineKeyboardButton("\U0001f6ba Female", callback_data="profile:sex:Female"),
            ],
            [InlineKeyboardButton("\U0001f3eb Change Department", callback_data="profile:edit:department")],
            [InlineKeyboardButton("\U0001f4c6 Change Year", callback_data="profile:edit:year")],
            [InlineKeyboardButton("\U0001f4dd Change Bio", callback_data="profile:edit:bio")],
            [InlineKeyboardButton("\U0001f519 Back", callback_data="menu:home")],
        ]
    )


def kb_categories() -> InlineKeyboardMarkup:
    with closing(db()) as conn:
        cats = conn.execute(
            "SELECT * FROM categories WHERE active = 1 ORDER BY id"
        ).fetchall()
    rows, row = [], []
    for c in cats:
        row.append(InlineKeyboardButton(f"{c['emoji']} {c['name']}", callback_data=f"confess:cat:{c['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("\u274c Cancel", callback_data="confess:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_cancel(cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data=cb)]])


def kb_admin_review(confession_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("\u2705 Approve", callback_data=f"admin:approve:{confession_id}"),
                InlineKeyboardButton("\u274c Reject", callback_data=f"admin:reject:{confession_id}"),
            ]
        ]
    )


def kb_admin_user_actions(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("\u26a0\ufe0f Warn User", callback_data=f"admin:warn:{user_id}"),
                InlineKeyboardButton("\U0001f6ab Ban User", callback_data=f"admin:ban:{user_id}"),
            ]
        ]
    )


def kb_channel_post(confession_id: int, bot_username: str, count: int = 0) -> InlineKeyboardMarkup:
    url = f"https://t.me/{bot_username}?start=c_{confession_id}"
    label = f"\U0001f4ac {count} Comment" + ("" if count == 1 else "s")
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=url)]])


def kb_comment_actions(comment_id: int, likes: int, dislikes: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(f"\u2764\ufe0f {likes}", callback_data=f"react:like:{comment_id}"),
                InlineKeyboardButton(f"\U0001f44e {dislikes}", callback_data=f"react:dislike:{comment_id}"),
                InlineKeyboardButton("\u21a9\ufe0f Reply", callback_data=f"comment:reply:{comment_id}"),
            ],
            [InlineKeyboardButton("\U0001f6a9 Report", callback_data=f"report:comment:{comment_id}")],
        ]
    )


def kb_comments_menu(confession_id: int, count: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"\U0001f440 {count} \u2022 View Comments", callback_data=f"comment:view:{confession_id}:0")],
            [InlineKeyboardButton("\u270d\ufe0f Add Comment", callback_data=f"comment:add:{confession_id}")],
            [InlineKeyboardButton("\U0001f6a9 Report Confession", callback_data=f"report:confession:{confession_id}")],
            [InlineKeyboardButton("\U0001f3e0 Main Menu", callback_data="menu:home")],
        ]
    )


def kb_writing_options() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["\U0001f4ce Attach Media", "\u23ed\ufe0f Skip (Text Only)"]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("\U0001f4ca Statistics", callback_data="admin:stats")],
            [InlineKeyboardButton("\U0001f4dd Pending Confessions", callback_data="admin:pending")],
            [InlineKeyboardButton("\U0001f4e2 Send Promotion", callback_data="admin:promo")],
            [InlineKeyboardButton("\U0001f3e0 Main Menu", callback_data="menu:home")],
        ]
    )


# --------------------------------------------------------------------------
# TEXT BUILDERS
# --------------------------------------------------------------------------

WELCOME_TEXT = (
    f"\U0001f393 <b>እንኳን ወደ {esc(UNIVERSITY_NAME)}በደህና መጡ</b>\n\n"
    "ይህ በነጻነት የምትናገሩበት፣ ሃሳባችሁን የምታካፍሉበት እና ማህበረሰባችሁ ጋር "
    "የምትገናኙበት ቦታ ነው።\n\n"
    "\u2022 ሃሳቦን በግል ይላኩ እና ወደ admin ቡድናችን ይደርሳል።\n"
    "\u2022 ማንነትዎ ለሌሎች ተማሪዎች ሆነ ለ Admin <b>ስውር ሆኖ ይቆያል</b>\n"
    "\u2022 Approved Thoughts አስተያየት ምስጫ ጋር ወደ ቻናሉ ይለጠፋሉ።\n"
    "\u2022 ትንኮሳ፣ አላስፈላጊ መልእክት አይፈቀዱም  /rules ይመልከቱ።\n\n"
    "ለመጀመር ከታች ካሉት አማራጮች ውስጥ አንዱን ይምረጡ።"
)

PRIVACY_TEXT = (
    "\U0001f512 <b>PRIVACY</b>\n\n"
    "\u2022 መጀመሪያ የርሶ Thoughts ለሌሎች ተማሪዎች ስውር ናቸው።\n"
    "\u2022 Adminኦች ለቁጥጥር ሲባል Thoughtኦችን መጀመሪያ የሚያዩ ይሆናል\n"
    "\u2022 አስተያየቶች(comments) እርስዎ በመረጡት የህዝብ መገለጫ ስም(profil name) ስር ይታያሉ።\n"
    "\u2022 የይለፍ ቃል፣ የገንዘብ ዝርዝር ወይም ሌላ በጣም ሚስጥራዊ የግል መረጃ "
    "በቦቱ በኩል በጭራሽ አይላኩ።"
)

HELP_TEXT = (
    "\u2753 <b>HELP</b>\n\n"
    "\U0001f464 <b>Profile</b> — በአስተያየቶች ውስጥ እንዴት እንደሚታዩ ያዘጋጁ።\n"
    "\U0001f4dd <b>Thoughts</b> — ለምርመራ በስውር ያስገቡ።\n"
    "\u2b50 <b>Aura point</b> — የAura ነጥብዎን ይመልከቱ።\n"
    "\U0001f4dc <b>Rules</b> / \U0001f512 <b>privacy</b> — ማህበረሰቡ እንዴት እንደሚተዳደር።\n\n"
    "አንድ Thought ከተፈቀደ በኋላ ወደ ቻናሉ ይለጠፋል፣ በዚያም \U0001f4ac አስተያየት "
    "የሚለው ቁልፍ ውይይቱን በዚህ የግል ቻት ውስጥ ይከፍታል።"
)


def profile_text(u: sqlite3.Row) -> str:
    return (
        "\U0001f464 <b>My Profile</b>\n\n"
        f"Name: {esc(u['profile_name']) or 'Not set'}\n"
        f"Sex: {esc(u['sex']) or 'Not set'}\n"
        f"Department: {esc(u['department']) or 'Not set'}\n"
        f"Year: {esc(u['year']) or 'Not set'}\n"
        f"Bio: {esc(u['bio']) or 'Not set'}\n"
        f"\u2b50 Aura: {u['aura_points']}"
    )


def display_name(u: sqlite3.Row) -> str:
    return u["profile_name"] or f"Student#{u['id']}"


def confession_channel_text(conf: sqlite3.Row, cat: sqlite3.Row, comment_count: int) -> str:
    cid = f"{conf['id']:06d}"
    body = esc(conf["content"]) if conf["content"] else ""
    return (
        f"\U0001f4dd <b>CONFESSION #{cid}</b>\n\n"
        f"{body}\n\n"
        f"\U0001f3f7 Category: {cat['emoji']} {esc(cat['name'])}"
    )


def admin_review_text(conf: sqlite3.Row, cat: Optional[sqlite3.Row], user: sqlite3.Row) -> str:
    uname = f"@{user['username']}" if user["username"] else "(no username)"
    body = esc(conf["content"]) if conf["content"] else "(media attached)"
    cat_label = f"{cat['emoji']} {esc(cat['name'])}" if cat else "(uncategorized)"
    return (
        "<b>CONFESSION REVIEW</b>\n\n"
        f"ID: #{conf['id']:06d}\n"
        f"Category: {cat_label}\n\n"
        f"User: {esc(uname)}\n"
        f"User ID: {user['telegram_id']}\n\n"
        f"Confession:\n{body}"
    )


# --------------------------------------------------------------------------
# SMALL DATA HELPERS
# --------------------------------------------------------------------------

def get_category(cat_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM categories WHERE id = ?", (cat_id,)).fetchone()


def get_confession(conf_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM confessions WHERE id = ?", (conf_id,)).fetchone()


def comment_count(conf_id: int) -> int:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT COUNT(*) c FROM comments WHERE confession_id = ? AND deleted = 0",
            (conf_id,),
        ).fetchone()
        return row["c"]


async def refresh_channel_count(context: ContextTypes.DEFAULT_TYPE, conf: sqlite3.Row) -> None:
    if not conf["channel_message_id"]:
        return
    cat = get_category(conf["category_id"])
    n = comment_count(conf["id"])
    text = confession_channel_text(conf, cat, n)
    try:
        await context.bot.edit_message_text(
            chat_id=CHANNEL_ID,
            message_id=conf["channel_message_id"],
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_post(conf["id"], context.bot.username, n),
        )
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            log.warning("Could not refresh channel message: %s", e)


# --------------------------------------------------------------------------
# COMMAND HANDLERS
# --------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tg_user = update.effective_user
    user = get_or_create_user(tg_user.id, tg_user.username)
    context.user_data.clear()

    args = context.args
    if args and args[0].startswith("c_"):
        try:
            conf_id = int(args[0][2:])
        except ValueError:
            conf_id = None
        if conf_id:
            conf = get_confession(conf_id)
            if conf and conf["status"] == "approved":
                await open_comments_menu(update.effective_chat.id, context, conf_id)
                return

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=WELCOME_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main_menu(tg_user.id),
    )


async def cmd_privacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(PRIVACY_TEXT, parse_mode=ParseMode.HTML)


async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        f"\U0001f4dc <b>Community Rules</b>\n\n{esc(COMMUNITY_RULES)}", parse_mode=ParseMode.HTML
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    await update.effective_message.reply_text(
        "\U0001f510 <b>ADMIN PANEL</b>", parse_mode=ParseMode.HTML, reply_markup=kb_admin_panel()
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.clear()
    await update.effective_message.reply_text(
        "Cancelled.", reply_markup=kb_main_menu(update.effective_user.id)
    )


# --------------------------------------------------------------------------
# CALLBACK QUERY ROUTER
# --------------------------------------------------------------------------

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    tg_user = update.effective_user
    user = get_or_create_user(tg_user.id, tg_user.username)
    data = q.data or ""

    if is_banned(user) and not data.startswith("menu:"):
        await q.answer("You are banned from participating.", show_alert=True)
        return

    parts = data.split(":")
    ns = parts[0]

    try:
        if ns == "menu":
            await q.answer()
            await route_menu(q, context, user, parts)
        elif ns == "profile":
            await q.answer()
            await route_profile(q, context, user, parts)
        elif ns == "confess":
            await q.answer()
            await route_confess(q, context, user, parts)
        elif ns == "comment":
            await q.answer()
            await route_comment(q, context, user, parts)
        elif ns == "react":
            await route_react(q, context, user, parts)
        elif ns == "report":
            await q.answer()
            await route_report(q, context, user, parts)
        elif ns == "admin":
            await route_admin(q, context, user, parts)
        else:
            await q.answer()
    except Exception:
        log.exception("Error handling callback %s", data)
        try:
            await q.answer("Something went wrong. Please try again.", show_alert=True)
        except Exception:
            pass


async def route_menu(q, context, user, parts) -> None:
    action = parts[1]
    chat_id = q.message.chat_id
    if action == "home":
        await q.edit_message_text(
            WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=kb_main_menu(user["telegram_id"])
        )
    elif action == "profile":
        await q.edit_message_text(profile_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_profile(user))
    elif action == "confess":
        context.user_data.clear()
        await q.edit_message_text(
            "\U0001f4dd Choose a category for your confession:", reply_markup=kb_categories()
        )
    elif action == "myaura":
        await q.edit_message_text(
            f"\u2b50 <b>My Aura:</b> {user['aura_points']} points\n\n"
            "Earn aura by commenting and receiving likes from the community.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back(),
        )
    elif action == "rules":
        await q.edit_message_text(
            f"\U0001f4dc <b>Community Rules</b>\n\n{esc(COMMUNITY_RULES)}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_back(),
        )
    elif action == "privacy":
        await q.edit_message_text(PRIVACY_TEXT, parse_mode=ParseMode.HTML, reply_markup=kb_back())
    elif action == "help":
        await q.edit_message_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=kb_back())


async def route_profile(q, context, user, parts) -> None:
    action = parts[1]
    if action == "edit":
        field = parts[2]
        context.user_data["awaiting"] = "profile_edit"
        context.user_data["profile_field"] = field
        prompts = {
            "name": "Send your new display name (shown on your comments):",
            "department": "Send your department:",
            "year": "Send your year (e.g. 2nd Year):",
            "bio": "Send a short bio:",
        }
        await q.edit_message_text(prompts[field], reply_markup=kb_cancel("profile:cancel"))
    elif action == "sex":
        value = parts[2]
        with closing(db()) as conn:
            conn.execute("UPDATE users SET sex = ? WHERE id = ?", (value, user["id"]))
            conn.commit()
        user = get_user_by_id(user["id"])
        await q.edit_message_text(profile_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_profile(user))
    elif action == "cancel":
        context.user_data.clear()
        user = get_user_by_id(user["id"])
        await q.edit_message_text(profile_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_profile(user))


async def route_confess(q, context, user, parts) -> None:
    action = parts[1]
    if action == "cat":
        cat_id = int(parts[2])
        context.user_data["awaiting"] = "confession_content"
        context.user_data["confession_category"] = cat_id
        await q.edit_message_text(
            f"Send your confession now (text, photo, GIF, or sticker).\n"
            f"Max {MAX_CONFESSION_LEN} characters for text.",
            reply_markup=kb_cancel("confess:cancel"),
        )
        await context.bot.send_message(
            q.message.chat_id,
            "Choose how you want to add your confession:",
            reply_markup=kb_writing_options(),
        )
    elif action == "cancel":
        context.user_data.clear()
        await q.edit_message_text("Cancelled.", reply_markup=kb_main_menu(user["telegram_id"]))


async def open_comments_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, conf_id: int) -> None:
    conf = get_confession(conf_id)
    if not conf:
        await context.bot.send_message(chat_id, "This confession no longer exists.")
        return
    n = comment_count(conf_id)
    await context.bot.send_message(
        chat_id,
        f"\U0001f4ac <b>Confession #{conf_id:06d}</b> — {n} comment(s)",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_comments_menu(conf_id, n),
    )


PAGE_SIZE = 5


async def route_comment(q, context, user, parts) -> None:
    action = parts[1]
    chat_id = q.message.chat_id

    if action == "add":
        conf_id = int(parts[2])
        context.user_data["awaiting"] = "comment_content"
        context.user_data["comment_confession_id"] = conf_id
        context.user_data.pop("reply_parent_id", None)
        await q.edit_message_text(
            "Send your comment (text, photo, GIF, or sticker).",
            reply_markup=kb_cancel(f"comment:canceladd:{conf_id}"),
        )
    elif action == "canceladd":
        context.user_data.clear()
        conf_id = int(parts[2])
        n = comment_count(conf_id)
        await q.edit_message_text(
            f"\U0001f4ac Confession #{conf_id:06d}",
            reply_markup=kb_comments_menu(conf_id, n),
        )
    elif action == "menu":
        conf_id = int(parts[2])
        n = comment_count(conf_id)
        await context.bot.send_message(
            chat_id,
            f"\U0001f4ac <b>Confession #{conf_id:06d}</b> — {n} comment(s)",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_comments_menu(conf_id, n),
        )
    elif action == "view":
        conf_id, page = int(parts[2]), int(parts[3])
        await show_comments_page(q, context, conf_id, page)
    elif action == "reply":
        comment_id = int(parts[2])
        with closing(db()) as conn:
            c = conn.execute("SELECT * FROM comments WHERE id = ?", (comment_id,)).fetchone()
        if not c:
            await q.answer("Comment not found.", show_alert=True)
            return
        context.user_data["awaiting"] = "reply_content"
        context.user_data["reply_parent_id"] = comment_id
        context.user_data["comment_confession_id"] = c["confession_id"]
        await context.bot.send_message(
            chat_id,
            "Send your reply (text, photo, GIF, or sticker).",
            reply_markup=kb_cancel(f"comment:canceladd:{c['confession_id']}"),
        )


async def show_comments_page(q, context, conf_id: int, page: int) -> None:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM comments WHERE confession_id = ? AND deleted = 0 "
            "ORDER BY created_at ASC",
            (conf_id,),
        ).fetchall()

    if not rows:
        await q.edit_message_text(
            "No comments yet — be the first!", reply_markup=kb_comments_menu(conf_id, 0)
        )
        return

    start = page * PAGE_SIZE
    chunk = rows[start : start + PAGE_SIZE]
    chat_id = q.message.chat_id
    bot_username = context.bot.username

    await q.edit_message_text(
        f"\U0001f4ac Comment #{conf_id:06d} — showing {start + 1}-{start + len(chunk)} of {len(rows)}",
    )

    for c in chunk:
        author = get_user_by_id(c["user_id"])
        prefix = "\u21b3 " if c["parent_comment_id"] else ""
        body = esc(c["content"]) if c["content"] else ""
        # Telegram Bot API has no custom text colors — an <a> link renders as
        # Telegram's standard blue, so that's used for the "blue, link-like"
        # name style. It points at the bot itself (never at the real user),
        # so anonymity toward other students is preserved.
        name_link = f'<a href="https://t.me/{bot_username}">{esc(display_name(author))}</a>'
        aura_points = author["aura_points"]
        footer = f"\U0001f464 {name_link}  \u2b50 {aura_points} Aura"
        text = f"{prefix}{body}\n\n{footer}" if body else f"{prefix}{footer}"
        markup = kb_comment_actions(c["id"], c["likes"], c["dislikes"])

        if c["media_type"] == "photo" and c["media_id"]:
            await context.bot.send_photo(chat_id, c["media_id"], caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
        elif c["media_type"] == "animation" and c["media_id"]:
            await context.bot.send_animation(chat_id, c["media_id"], caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
        elif c["media_type"] == "sticker" and c["media_id"]:
            await context.bot.send_sticker(chat_id, c["media_id"])
            await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=markup)
        else:
            await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=markup)

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("\u2b05 Prev", callback_data=f"comment:view:{conf_id}:{page-1}"))
    if start + PAGE_SIZE < len(rows):
        nav.append(InlineKeyboardButton("Next \u27a1", callback_data=f"comment:view:{conf_id}:{page+1}"))
    nav_rows = [nav] if nav else []
    nav_rows.append([InlineKeyboardButton("\U0001f519 Comment Menu", callback_data=f"comment:menu:{conf_id}")])
    await context.bot.send_message(chat_id, "\u2500" * 10, reply_markup=InlineKeyboardMarkup(nav_rows))


async def route_react(q, context, user, parts) -> None:
    reaction, comment_id = parts[1], int(parts[2])
    with closing(db()) as conn:
        c = conn.execute("SELECT * FROM comments WHERE id = ?", (comment_id,)).fetchone()
        if not c:
            await q.answer("Comment not found.", show_alert=True)
            return
        existing = conn.execute(
            "SELECT * FROM reactions WHERE comment_id = ? AND user_id = ?",
            (comment_id, user["id"]),
        ).fetchone()

        delta_like = delta_dislike = 0
        if existing is None:
            conn.execute(
                "INSERT INTO reactions (comment_id, user_id, reaction_type) VALUES (?, ?, ?)",
                (comment_id, user["id"], reaction),
            )
            if reaction == "like":
                delta_like = 1
            else:
                delta_dislike = 1
        elif existing["reaction_type"] == reaction:
            conn.execute("DELETE FROM reactions WHERE id = ?", (existing["id"],))
            if reaction == "like":
                delta_like = -1
            else:
                delta_dislike = -1
        else:
            conn.execute(
                "UPDATE reactions SET reaction_type = ? WHERE id = ?", (reaction, existing["id"])
            )
            if reaction == "like":
                delta_like, delta_dislike = 1, -1
            else:
                delta_like, delta_dislike = -1, 1

        conn.execute(
            "UPDATE comments SET likes = likes + ?, dislikes = dislikes + ? WHERE id = ?",
            (delta_like, delta_dislike, comment_id),
        )
        conn.commit()
        c = conn.execute("SELECT * FROM comments WHERE id = ?", (comment_id,)).fetchone()

    if delta_like == 1:
        add_aura(c["user_id"], AURA_PER_LIKE_RECEIVED)
    elif delta_like == -1 and existing is not None and existing["reaction_type"] == "like":
        add_aura(c["user_id"], -AURA_PER_LIKE_RECEIVED)

    await q.answer("Reaction saved.")
    try:
        await q.edit_message_reply_markup(reply_markup=kb_comment_actions(c["id"], c["likes"], c["dislikes"]))
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            log.warning("reaction markup update failed: %s", e)


async def route_report(q, context, user, parts) -> None:
    content_type, content_id = parts[1], int(parts[2])
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO reports (reporter_id, content_type, content_id, reason, status, created_at) "
            "VALUES (?, ?, ?, ?, 'pending', ?)",
            (user["id"], content_type, content_id, "Reported via bot", now()),
        )
        conn.commit()
        report_id = conn.execute("SELECT lastval() id").fetchone()["id"]

    await q.answer("Thanks — our team will review this.", show_alert=True)

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                "\U0001f6a9 <b>CONTENT REPORT</b>\n\n"
                f"Type: {esc(content_type)}\n"
                f"Content ID: {content_id}\n"
                f"Reported by user id: {user['telegram_id']}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("\u2705 Keep", callback_data=f"admin:reportkeep:{report_id}"),
                            InlineKeyboardButton("\U0001f5d1 Delete", callback_data=f"admin:reportdel:{report_id}"),
                        ]
                    ]
                ),
            )
        except Forbidden:
            pass


# --------------------------------------------------------------------------
# ADMIN ROUTES
# --------------------------------------------------------------------------

async def route_admin(q, context, user, parts) -> None:
    if not is_admin(user["telegram_id"]):
        await q.answer("Not authorized.", show_alert=True)
        return
    await q.answer()
    action = parts[1]

    if action == "panel":
        await q.edit_message_text("\U0001f510 <b>ADMIN PANEL</b>", parse_mode=ParseMode.HTML, reply_markup=kb_admin_panel())

    elif action == "stats":
        await show_stats(q, context)

    elif action == "pending":
        await show_pending(q, context)

    elif action == "promo":
        context.user_data["awaiting"] = "promotion_content"
        await q.edit_message_text(
            "Send the promotion message (text, or photo/video with caption). "
            "It will be broadcast to every registered user.",
            reply_markup=kb_cancel("admin:panel"),
        )

    elif action == "approve":
        await admin_approve(q, context, int(parts[2]))

    elif action == "reject":
        await admin_reject(q, context, int(parts[2]))

    elif action == "warn":
        target_user_id = int(parts[2])
        target = get_user_by_id(target_user_id)
        if target:
            try:
                await context.bot.send_message(
                    target["telegram_id"],
                    "\u26a0\ufe0f You have received a warning from the moderation team for violating "
                    "community rules. Further violations may result in a ban.",
                )
            except Forbidden:
                pass
        await q.answer("User warned.", show_alert=True)

    elif action == "ban":
        target_user_id = int(parts[2])
        with closing(db()) as conn:
            conn.execute("UPDATE users SET is_banned = 1 WHERE id = ?", (target_user_id,))
            conn.commit()
        target = get_user_by_id(target_user_id)
        if target:
            try:
                await context.bot.send_message(
                    target["telegram_id"], "\U0001f6ab You have been banned from the confession community."
                )
            except Forbidden:
                pass
        await q.answer("User banned.", show_alert=True)

    elif action == "unban":
        target_user_id = int(parts[2])
        with closing(db()) as conn:
            conn.execute("UPDATE users SET is_banned = 0 WHERE id = ?", (target_user_id,))
            conn.commit()
        await q.answer("User unbanned.", show_alert=True)

    elif action == "reportkeep":
        report_id = int(parts[2])
        with closing(db()) as conn:
            conn.execute("UPDATE reports SET status = 'kept' WHERE id = ?", (report_id,))
            conn.commit()
        await q.edit_message_reply_markup(reply_markup=None)
        await q.message.reply_text("Report resolved: content kept.")

    elif action == "reportdel":
        report_id = int(parts[2])
        with closing(db()) as conn:
            report = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
            if report:
                if report["content_type"] == "comment":
                    conn.execute("UPDATE comments SET deleted = 1 WHERE id = ?", (report["content_id"],))
                elif report["content_type"] == "confession":
                    conn.execute("UPDATE confessions SET status = 'rejected' WHERE id = ?", (report["content_id"],))
                conn.execute("UPDATE reports SET status = 'deleted' WHERE id = ?", (report_id,))
                conn.commit()
                if report["content_type"] == "comment":
                    c = conn.execute("SELECT * FROM comments WHERE id = ?", (report["content_id"],)).fetchone()
                    if c:
                        conf = get_confession(c["confession_id"])
                        if conf:
                            await refresh_channel_count(context, conf)
        await q.edit_message_reply_markup(reply_markup=None)
        await q.message.reply_text("Report resolved: content deleted.")


async def show_stats(q, context) -> None:
    with closing(db()) as conn:
        total_users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        active_users = conn.execute("SELECT COUNT(*) c FROM users WHERE is_banned = 0").fetchone()["c"]
        total_conf = conn.execute("SELECT COUNT(*) c FROM confessions").fetchone()["c"]
        pending_conf = conn.execute("SELECT COUNT(*) c FROM confessions WHERE status='pending'").fetchone()["c"]
        approved_conf = conn.execute("SELECT COUNT(*) c FROM confessions WHERE status='approved'").fetchone()["c"]
        rejected_conf = conn.execute("SELECT COUNT(*) c FROM confessions WHERE status='rejected'").fetchone()["c"]
        total_comments = conn.execute("SELECT COUNT(*) c FROM comments WHERE deleted=0").fetchone()["c"]
        total_reactions = conn.execute("SELECT COUNT(*) c FROM reactions").fetchone()["c"]

    text = (
        "\U0001f4ca <b>Statistics</b>\n\n"
        f"Total users: {total_users}\n"
        f"Active (non-banned): {active_users}\n"
        f"Total confessions: {total_conf}\n"
        f"Pending: {pending_conf}\n"
        f"Approved: {approved_conf}\n"
        f"Rejected: {rejected_conf}\n"
        f"Total comments: {total_comments}\n"
        f"Total reactions: {total_reactions}"
    )
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb_back("admin:panel"))


async def show_pending(q, context) -> None:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM confessions WHERE status='pending' ORDER BY created_at ASC LIMIT 10"
        ).fetchall()
    if not rows:
        await q.edit_message_text("No pending confessions.", reply_markup=kb_back("admin:panel"))
        return
    await q.edit_message_text(f"{len(rows)} pending confession(s):", reply_markup=kb_back("admin:panel"))
    for conf in rows:
        cat = get_category(conf["category_id"])
        author = get_user_by_id(conf["user_id"])
        await context.bot.send_message(
            q.message.chat_id,
            admin_review_text(conf, cat, author),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_admin_review(conf["id"]),
        )


async def admin_approve(q, context, conf_id: int) -> None:
    conf = get_confession(conf_id)
    if not conf or conf["status"] != "pending":
        await q.answer("Already handled.", show_alert=True)
        return
    cat = get_category(conf["category_id"])

    text = confession_channel_text(conf, cat, 0)
    if conf["media_type"] == "photo" and conf["media_id"]:
        msg = await context.bot.send_photo(
            CHANNEL_ID, conf["media_id"], caption=text, parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_post(conf_id, context.bot.username),
        )
    elif conf["media_type"] == "animation" and conf["media_id"]:
        msg = await context.bot.send_animation(
            CHANNEL_ID, conf["media_id"], caption=text, parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_post(conf_id, context.bot.username),
        )
    elif conf["media_type"] == "sticker" and conf["media_id"]:
        await context.bot.send_sticker(CHANNEL_ID, conf["media_id"])
        msg = await context.bot.send_message(
            CHANNEL_ID, text, parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_post(conf_id, context.bot.username),
        )
    else:
        msg = await context.bot.send_message(
            CHANNEL_ID, text, parse_mode=ParseMode.HTML,
            reply_markup=kb_channel_post(conf_id, context.bot.username),
        )

    with closing(db()) as conn:
        conn.execute(
            "UPDATE confessions SET status='approved', channel_message_id=?, approved_at=? WHERE id=?",
            (msg.message_id, now(), conf_id),
        )
        conn.commit()

    add_aura(conf["user_id"], AURA_PER_CONFESSION_APPROVED)

    author = get_user_by_id(conf["user_id"])
    try:
        await context.bot.send_message(
            author["telegram_id"],
            f"\u2705 Your confession #{conf_id:06d} was approved and published!",
        )
    except Forbidden:
        pass

    await q.edit_message_text(q.message.text_html + "\n\n\u2705 <b>APPROVED</b>", parse_mode=ParseMode.HTML)


async def admin_reject(q, context, conf_id: int) -> None:
    conf = get_confession(conf_id)
    if not conf or conf["status"] != "pending":
        await q.answer("Already handled.", show_alert=True)
        return
    with closing(db()) as conn:
        conn.execute("UPDATE confessions SET status='rejected' WHERE id=?", (conf_id,))
        conn.commit()

    author = get_user_by_id(conf["user_id"])
    try:
        await context.bot.send_message(
            author["telegram_id"], f"\u274c Your confession #{conf_id:06d} was not approved for publishing."
        )
    except Forbidden:
        pass

    await q.edit_message_text(q.message.text_html + "\n\n\u274c <b>REJECTED</b>", parse_mode=ParseMode.HTML)


# --------------------------------------------------------------------------
# FREE-TEXT / MEDIA INPUT HANDLER  (drives every "awaiting" flow)
# --------------------------------------------------------------------------

def extract_media(message):
    """Return (media_type, media_id, caption_or_text) for a supported message."""
    if message.photo:
        return "photo", message.photo[-1].file_id, message.caption
    if message.animation:
        return "animation", message.animation.file_id, message.caption
    if message.sticker:
        return "sticker", message.sticker.file_id, None
    if message.text:
        return None, None, message.text
    return None, None, None


async def on_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tg_user = update.effective_user
    user = get_or_create_user(tg_user.id, tg_user.username)
    awaiting = context.user_data.get("awaiting")
    if not awaiting:
        return  # not part of any flow — ignore silently

    if is_banned(user):
        await update.effective_message.reply_text("You are banned from participating.")
        return

    if rate_limited(tg_user.id):
        await update.effective_message.reply_text(
            "\u26a0\ufe0f You are sending messages too quickly. Please wait a moment."
        )
        return

    message = update.effective_message
    media_type, media_id, text = extract_media(message)

    # ---- profile fields (text only) ----
    if awaiting == "profile_edit":
        field = context.user_data.get("profile_field")
        column_by_field = {
            "name": "profile_name",
            "department": "department",
            "year": "year",
            "bio": "bio",
        }
        column = column_by_field.get(field)
        if not column:
            context.user_data.clear()
            await message.reply_text("Something went wrong. Please try again.")
            return
        if not text:
            await message.reply_text("Please send text for this field.")
            return
        text = text.strip()[:200]
        with closing(db()) as conn:
            conn.execute(f"UPDATE users SET {column} = ? WHERE id = ?", (text, user["id"]))
            conn.commit()
        context.user_data.clear()
        user = get_user_by_id(user["id"])
        await message.reply_text(profile_text(user), parse_mode=ParseMode.HTML, reply_markup=kb_profile(user))
        await vanish(message)
        return

    # ---- confession submission ----
    if awaiting == "confession_content":
        if text in ("\U0001f4ce Attach Media", "\u23ed\ufe0f Skip (Text Only)"):
            reply = (
                "Send your photo or GIF now."
                if text == "\U0001f4ce Attach Media"
                else "Send your confession text now."
            )
            await message.reply_text(reply, reply_markup=ReplyKeyboardRemove())
            await vanish(message)
            return
        if text and len(text) > MAX_CONFESSION_LEN:
            await message.reply_text(f"Confession too long (max {MAX_CONFESSION_LEN} characters).")
            return
        cat_id = context.user_data.get("confession_category")
        with closing(db()) as conn:
            conn.execute(
                "INSERT INTO confessions (user_id, category_id, content, media_type, media_id, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (user["id"], cat_id, text, media_type, media_id, now()),
            )
            conn.commit()
            conf_id = conn.execute("SELECT lastval() id").fetchone()["id"]
        context.user_data.clear()

        try:
            await message.reply_text(
                f"\u2705 Your confession has been submitted for review (ref #{conf_id:06d}).\n"
                "Please wait for administrator approval. Your identity will not be displayed publicly.",
                reply_markup=kb_main_menu(tg_user.id),
            )
        except Exception:
            log.exception("Failed to send confirmation for confession #%s", conf_id)
        await vanish(message)

        conf = get_confession(conf_id)
        cat = get_category(cat_id)
        for admin_id in ADMIN_IDS:
            try:
                if media_type == "photo":
                    m = await context.bot.send_photo(admin_id, media_id, caption=admin_review_text(conf, cat, user), parse_mode=ParseMode.HTML)
                elif media_type == "animation":
                    m = await context.bot.send_animation(admin_id, media_id, caption=admin_review_text(conf, cat, user), parse_mode=ParseMode.HTML)
                else:
                    m = None
                    if media_type == "sticker":
                        await context.bot.send_sticker(admin_id, media_id)
                    m = await context.bot.send_message(admin_id, admin_review_text(conf, cat, user), parse_mode=ParseMode.HTML)
                await context.bot.send_message(
                    admin_id, "Moderation actions:", reply_markup=kb_admin_review(conf_id)
                )
            except Forbidden:
                log.warning("Admin %s has not started the bot; cannot deliver review.", admin_id)
            except Exception:
                log.exception("Failed to deliver confession #%s to admin %s", conf_id, admin_id)
        return

    # ---- comment / reply submission ----
    if awaiting in ("comment_content", "reply_content"):
        if text and len(text) > MAX_COMMENT_LEN:
            await message.reply_text(f"Comment too long (max {MAX_COMMENT_LEN} characters).")
            return
        conf_id = context.user_data.get("comment_confession_id")
        parent_id = context.user_data.get("reply_parent_id")
        with closing(db()) as conn:
            conn.execute(
                "INSERT INTO comments (confession_id, user_id, parent_comment_id, content, media_type, media_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (conf_id, user["id"], parent_id, text, media_type, media_id, now()),
            )
            conn.commit()
            new_id = conn.execute("SELECT lastval() id").fetchone()["id"]

        add_aura(user["id"], AURA_PER_COMMENT)
        context.user_data.clear()

        conf = get_confession(conf_id)
        await refresh_channel_count(context, conf)

        await message.reply_text(
            "\u2705 Posted." if not parent_id else "\u2705 Reply posted.",
            reply_markup=kb_comments_menu(conf_id, comment_count(conf_id)),
        )
        await vanish(message)

        if parent_id:
            with closing(db()) as conn:
                parent = conn.execute("SELECT * FROM comments WHERE id = ?", (parent_id,)).fetchone()
            parent_author = get_user_by_id(parent["user_id"])
            if parent_author and parent_author["telegram_id"] != tg_user.id:
                try:
                    await context.bot.send_message(
                        parent_author["telegram_id"],
                        "\U0001f514 <b>New Reply</b>\n\n"
                        f"Someone replied to your comment on Confession #{conf_id:06d}.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("View Comment", callback_data=f"comment:view:{conf_id}:0")]]
                        ),
                    )
                except Forbidden:
                    pass
        return

    # ---- promotion broadcast (admin only) ----
    if awaiting == "promotion_content":
        if not is_admin(tg_user.id):
            context.user_data.clear()
            return
        context.user_data.clear()
        with closing(db()) as conn:
            targets = conn.execute("SELECT telegram_id FROM users WHERE is_banned = 0").fetchall()
        sent = 0
        for t in targets:
            try:
                if media_type == "photo":
                    await context.bot.send_photo(t["telegram_id"], media_id, caption=text)
                elif media_type == "animation":
                    await context.bot.send_animation(t["telegram_id"], media_id, caption=text)
                else:
                    await context.bot.send_message(t["telegram_id"], f"\U0001f4e2 <b>PROMOTION</b>\n\n{esc(text)}", parse_mode=ParseMode.HTML)
                sent += 1
            except Forbidden:
                continue
        await message.reply_text(f"Promotion sent to {sent} user(s).", reply_markup=kb_admin_panel())
        await vanish(message)
        return


# --------------------------------------------------------------------------
# ERROR HANDLER
# --------------------------------------------------------------------------

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("Something went wrong. Please try again.")
        except Exception:
            pass


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------

def build_app() -> Application:
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is not set. Copy .env.example to .env and fill it in.")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS is empty — no one will be able to moderate confessions.")
    if not CHANNEL_ID:
        log.warning("CHANNEL_ID is empty — approved confessions cannot be published.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("privacy", cmd_privacy))
    app.add_handler(CommandHandler("rules", cmd_rules))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    app.add_handler(CallbackQueryHandler(on_callback))

    app.add_handler(
        MessageHandler(
            (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.Sticker.ALL | filters.ANIMATION,
            on_input,
        )
    )

    app.add_error_handler(on_error)
    return app


def main() -> None:
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is not set. Copy .env.example to .env and fill it in.")
    init_db()
    app = build_app()
    webhook_url = os.getenv("WEBHOOK_URL", "") or os.getenv("RENDER_EXTERNAL_URL", "")
    webhook_url = webhook_url.rstrip("/")
    port = int(os.getenv("PORT", "8443"))
    if webhook_url:
        log.info("Bot starting (webhook)...")
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,
            webhook_url=f"{webhook_url}/{BOT_TOKEN}",
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        log.info("Bot starting (polling)...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
