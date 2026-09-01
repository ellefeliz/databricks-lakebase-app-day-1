"""
Databricks App: Support Ticketing System
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ticketing-app")

app = Flask(__name__)
_w = WorkspaceClient()

TICKETS_TABLE = os.environ.get("TICKETS_TABLE_NAME", "tickets")
MESSAGES_TABLE = os.environ.get("MESSAGES_TABLE_NAME", "ticket_messages")

# The set of statuses a ticket may have. Keep these in sync with the
# <select> options in templates/index.html.
_VALID_STATUSES = ("open", "in_progress", "resolved", "closed")


def ensure_tables():
    """Create the tickets and messages tables in Lakebase if they don't exist yet.

    The schema mirrors the pre-provisioned 'ticketing database': integer
    primary keys (ticket_id / message_id) defined as IDENTITY ... GENERATED
    ALWAYS, so Postgres auto-assigns new IDs and we read them back via
    RETURNING (see create_ticket / add_message).
    """
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKETS_TABLE} (
            ticket_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            title VARCHAR,
            status VARCHAR,
            created_by VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
            message_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ticket_id INTEGER NOT NULL REFERENCES {TICKETS_TABLE}(ticket_id) ON DELETE CASCADE,
            message_text VARCHAR,
            author VARCHAR,
            created_at TIMESTAMP
        )
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so tickets and messages can be attributed.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


def _run_write_returning(sql: str, params: tuple | None = None) -> list[dict]:
    """Run an INSERT/UPDATE ... RETURNING against Lakebase, commit, and return
    the resulting rows as list[dict].

    Unlike lakebase.run_query (read-only, no commit), this commits the write so
    the RETURNING row is persisted before the connection closes (otherwise the
    inserted row would be rolled back and downstream FK references would fail).
    """
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            conn.commit()
            return rows


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Ticketing system UI."""
    return render_template("index.html")


@app.route("/tickets", methods=["GET"])
def list_tickets():
    """View all support tickets, newest first."""
    ensure_tables()
    rows = lakebase.run_query(
        f"""
        SELECT ticket_id AS id, title, status, created_by, created_at
        FROM {TICKETS_TABLE}
        ORDER BY created_at DESC
        """
    )
    return jsonify(rows)


@app.route("/tickets/<int:ticket_id>", methods=["GET"])
def get_ticket(ticket_id: int):
    """Select a ticket and view its messages."""
    ensure_tables()
    ticket = lakebase.run_query(
        f"""
        SELECT ticket_id AS id, title, status, created_by, created_at
        FROM {TICKETS_TABLE}
        WHERE ticket_id = %s
        """,
        (ticket_id,),
    )
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    messages = lakebase.run_query(
        f"""
        SELECT message_id AS id, author, message_text AS body, created_at
        FROM {MESSAGES_TABLE}
        WHERE ticket_id = %s
        ORDER BY created_at ASC
        """,
        (ticket_id,),
    )
    return jsonify({"ticket": ticket[0], "messages": messages})


@app.route("/tickets", methods=["POST"])
def create_ticket():
    """Create a new support ticket.

    The optional description is stored as the opening message of the thread
    (the tickets table has no description column), so it shows up in the
    conversation view alongside replies.
    """
    ensure_tables()

    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()

    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()

    if not title:
        return jsonify({"error": "Title is required"}), 400

    email = _current_user_email()

    # ticket_id is IDENTITY GENERATED ALWAYS, so omit it and read it back.
    created = _run_write_returning(
        f"""
        INSERT INTO {TICKETS_TABLE} (title, status, created_by, created_at)
        VALUES (%s, 'open', %s, now())
        RETURNING ticket_id AS id, title, status, created_by, created_at
        """,
        (title, email),
    )
    ticket = created[0]
    new_id = ticket["id"]

    if description:
        lakebase.run_write(
            f"""
            INSERT INTO {MESSAGES_TABLE} (ticket_id, message_text, author, created_at)
            VALUES (%s, %s, %s, now())
            """,
            (new_id, description, email),
        )

    return jsonify(ticket), 201


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_message(ticket_id: int):
    """Add a message to an existing ticket."""
    ensure_tables()

    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()

    body = (data.get("body") or "").strip()
    if not body:
        return jsonify({"error": "Message body is required"}), 400

    # Verify the ticket exists.
    ticket = lakebase.run_query(
        f"SELECT ticket_id FROM {TICKETS_TABLE} WHERE ticket_id = %s", (ticket_id,)
    )
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    email = _current_user_email()

    rows = _run_write_returning(
        f"""
        INSERT INTO {MESSAGES_TABLE} (ticket_id, message_text, author, created_at)
        VALUES (%s, %s, %s, now())
        RETURNING message_id AS id, author, message_text AS body, created_at
        """,
        (ticket_id, email, body),
    )
    return jsonify(rows[0]), 201


@app.route("/tickets/<int:ticket_id>/status", methods=["POST"])
def update_status(ticket_id: int):
    """Update a ticket's status."""
    ensure_tables()

    if request.is_json:
        data = request.get_json(silent=True) or {}
    else:
        data = request.form.to_dict()

    status = (data.get("status") or "").strip().lower()

    if status not in _VALID_STATUSES:
        return jsonify(
            {"error": f"Invalid status: {status!r}. Must be one of {', '.join(_VALID_STATUSES)}"}
        ), 400

    # Verify the ticket exists.
    ticket = lakebase.run_query(
        f"SELECT ticket_id FROM {TICKETS_TABLE} WHERE ticket_id = %s", (ticket_id,)
    )
    if not ticket:
        return jsonify({"error": "Ticket not found"}), 404

    lakebase.run_write(
        f"UPDATE {TICKETS_TABLE} SET status = %s WHERE ticket_id = %s",
        (status, ticket_id),
    )

    rows = lakebase.run_query(
        f"SELECT ticket_id AS id, status FROM {TICKETS_TABLE} WHERE ticket_id = %s",
        (ticket_id,),
    )
    return jsonify(rows[0])


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")