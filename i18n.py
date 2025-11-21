import json
import os
from functools import lru_cache
from typing import Dict

DEFAULT_LANGUAGE = "ua"
LOCALES_DIR = os.path.join(os.path.dirname(__file__), "locales")


@lru_cache(maxsize=None)
def _load_language(lang: str) -> Dict[str, str]:
    filename = os.path.join(LOCALES_DIR, f"{lang}.json")
    if not os.path.exists(filename):
        if lang != DEFAULT_LANGUAGE:
            return _load_language(DEFAULT_LANGUAGE)
        return {}
    with open(filename, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}


class Translator:
    def __init__(self, lang: str | None = None):
        self.lang = lang or DEFAULT_LANGUAGE
        self.data = _load_language(self.lang)
        self.default_data = _load_language(DEFAULT_LANGUAGE)

    def t(self, key: str, **kwargs) -> str:
        template = self.data.get(key) or self.default_data.get(key) or key
        try:
            return template.format(**kwargs)
        except Exception:
            return template


def get_translator_for_user(user_row: Dict[str, str] | None) -> Translator:
    lang = None
    if user_row:
        lang = user_row.get("language") or DEFAULT_LANGUAGE
    return Translator(lang)
