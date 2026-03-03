import base64
import os
import logging

from cryptography.fernet import Fernet

logger = logging.getLogger("trading_engine.encryption")

_KEY_ENV = "WP_ENCRYPTION_KEY"
_fernet = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is not None:
        return _fernet

    key = os.environ.get(_KEY_ENV)
    if not key:
        key = Fernet.generate_key().decode()
        os.environ[_KEY_ENV] = key
        logger.warning(
            f"[ENCRYPTION] No {_KEY_ENV} found — generated ephemeral key. "
            "Set this secret for persistent encryption across restarts."
        )

    if isinstance(key, str):
        key = key.encode()

    _fernet = Fernet(key)
    return _fernet


def encrypt(plaintext: str) -> str:
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    f = _get_fernet()
    return f.decrypt(ciphertext.encode()).decode()
