import hashlib
import hmac
import os
import secrets

from .db import now, uid


def derive(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310000)


def provision(db, tenant, login, password, role):
    salt = os.urandom(16)
    db.execute("INSERT INTO users VALUES(?,?,?,?,?,?)",
               (uid(), tenant, login, salt, derive(password, salt), role))


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


def authenticate(db, tenant, login, password):
    row = db.execute("SELECT * FROM users WHERE tenant=? AND login=?", (tenant, login)).fetchone()
    check = derive(password, row["salt"] if row else bytes(16))
    if not row or not hmac.compare_digest(check, row["password"]):
        return None
    return dict(row)


def issue(db, user_id):
    token = secrets.token_urlsafe(32)
    expires = now() + 12 * 3600
    db.execute("DELETE FROM sessions WHERE expires<=?", (now(),))
    db.execute("INSERT INTO sessions VALUES(?,?,?)", (digest(token), user_id, expires))
    return token, expires
