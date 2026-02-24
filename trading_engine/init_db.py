#!/usr/bin/env python3
import sys
import os
import hashlib
import secrets

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("init_db")


def reset_database():
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.orm import sessionmaker

    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trading_data.db")
    db_url = f"sqlite:///{db_path}"
    local_engine = create_engine(db_url, connect_args={"check_same_thread": False})

    from trading_engine.models import Base, AdminUser

    logger.info("Dropping all tables...")
    Base.metadata.drop_all(local_engine)
    logger.info("All tables dropped.")

    logger.info("Recreating all tables...")
    Base.metadata.create_all(local_engine)
    logger.info("All tables created.")

    Session = sessionmaker(bind=local_engine)
    with Session() as session:
        count = session.query(AdminUser).count()
        if count == 0:
            salt = secrets.token_hex(16)
            h = hashlib.pbkdf2_hmac("sha256", "pass123".encode(), salt.encode(), 100000).hex()
            pw_hash = f"{salt}:{h}"
            session.add(AdminUser(username="admin", password_hash=pw_hash))
            session.commit()
            logger.info("Default admin user seeded (admin/pass123)")

    insp = inspect(local_engine)
    tables = insp.get_table_names()
    logger.info(f"Tables created ({len(tables)}): {tables}")

    for table_name in tables:
        indexes = insp.get_indexes(table_name)
        if indexes:
            idx_names = [idx["name"] for idx in indexes]
            logger.info(f"  {table_name} indexes: {idx_names}")

    local_engine.dispose()
    logger.info("Database reset complete.")


if __name__ == "__main__":
    reset_database()
