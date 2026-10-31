"""Text folding for search."""

import unicodedata

_NON_DECOMPOSING = str.maketrans({"ł": "l", "đ": "d", "ø": "o"})


def fold(text: str) -> str:
    """Return text without case and diacritics."""
    decomposed = unicodedata.normalize("NFKD", text.casefold().translate(_NON_DECOMPOSING))
    return "".join(char for char in decomposed if not unicodedata.combining(char))
