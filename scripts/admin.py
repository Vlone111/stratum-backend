"""Provision one workspace user on the instance that owns the database."""
import argparse
import getpass
from stratum.db import connection, initialize
from stratum.security import provision


def main():
    parser = argparse.ArgumentParser(description="Создать аккаунт рабочего пространства")
    parser.add_argument("workspace")
    parser.add_argument("login")
    parser.add_argument("--role", choices=("editor", "viewer"), default="viewer")
    args = parser.parse_args()
    if not all(0 < len(s.strip()) <= 100 and s == s.strip() for s in (args.workspace, args.login)):
        parser.error("workspace и login должны содержать от 1 до 100 символов без пробелов по краям")
    password = getpass.getpass("Пароль (12–256 символов): ")
    if not 12 <= len(password) <= 256:
        parser.error("требуется от 12 до 256 символов")
    initialize()
    with connection(True) as db:
        if db.execute("SELECT 1 FROM users WHERE tenant=? AND login=?", (args.workspace, args.login)).fetchone():
            parser.error("логин уже занят в этом пространстве")
        db.execute("INSERT OR IGNORE INTO tenants VALUES(?,?)", (args.workspace, args.workspace))
        provision(db, args.workspace, args.login, password, args.role)


if __name__ == "__main__":
    main()
