"""Porter Stemmer + 同根词分组 (sub-task ③a / ④).

We intentionally inline a Porter Stemmer implementation rather than depend on `nltk`
(keeps the Docker image slim and stable). Algorithm is the classic 1980 Porter paper.
"""
import re

_STOP_PREPS = {"for", "with", "of", "the", "a", "an", "to", "and", "or", "in", "on", "at", "by"}
_VOWELS = set("aeiou")


def _is_consonant(word, i):
    if word[i] in _VOWELS:
        return False
    if word[i] == "y":
        return i == 0 or word[i - 1] in _VOWELS
    return True


def _measure(stem):
    """m = number of (VC) sequences in stem."""
    m = 0
    state = "C"
    for i in range(len(stem)):
        c = "C" if _is_consonant(stem, i) else "V"
        if state == "C" and c == "V":
            state = "V"
        elif state == "V" and c == "C":
            m += 1
            state = "C"
    return m


def _has_vowel(stem):
    return any(not _is_consonant(stem, i) for i in range(len(stem)))


def _ends_dbl_consonant(word):
    if len(word) < 2:
        return False
    return word[-1] == word[-2] and _is_consonant(word, len(word) - 1)


def _ends_cvc(word):
    if len(word) < 3:
        return False
    return (_is_consonant(word, len(word) - 3) and not _is_consonant(word, len(word) - 2)
            and _is_consonant(word, len(word) - 1) and word[-1] not in "wxy")


def _replace_suffix(word, suffix, replacement, m_min=0):
    if word.endswith(suffix):
        stem = word[: len(word) - len(suffix)]
        if _measure(stem) > m_min:
            return stem + replacement
    return word


def porter_stem(word: str) -> str:
    """Apply Porter Stemmer steps to a single word."""
    if not word or len(word) <= 2:
        return word
    w = word.lower()

    # Step 1a
    if w.endswith("sses"):
        w = w[:-2]
    elif w.endswith("ies"):
        w = w[:-2]
    elif w.endswith("ss"):
        pass
    elif w.endswith("s"):
        w = w[:-1]

    # Step 1b
    if w.endswith("eed"):
        stem = w[:-3]
        if _measure(stem) > 0:
            w = stem + "ee"
    else:
        flag = False
        for suf in ("ed", "ing"):
            if w.endswith(suf):
                stem = w[: -len(suf)]
                if _has_vowel(stem):
                    w = stem
                    flag = True
                    break
        if flag:
            if w.endswith("at") or w.endswith("bl") or w.endswith("iz"):
                w += "e"
            elif _ends_dbl_consonant(w) and w[-1] not in "lsz":
                w = w[:-1]
            elif _measure(w) == 1 and _ends_cvc(w):
                w += "e"

    # Step 1c
    if w.endswith("y") and _has_vowel(w[:-1]):
        w = w[:-1] + "i"

    # Step 2 (simplified — most common suffixes)
    pairs2 = [
        ("ational", "ate"), ("tional", "tion"), ("enci", "ence"), ("anci", "ance"),
        ("izer", "ize"), ("abli", "able"), ("alli", "al"), ("entli", "ent"),
        ("eli", "e"), ("ousli", "ous"), ("ization", "ize"), ("ation", "ate"),
        ("ator", "ate"), ("alism", "al"), ("iveness", "ive"), ("fulness", "ful"),
        ("ousness", "ous"), ("aliti", "al"), ("iviti", "ive"), ("biliti", "ble"),
    ]
    for suf, rep in pairs2:
        new = _replace_suffix(w, suf, rep)
        if new != w:
            w = new
            break

    # Step 3
    pairs3 = [("icate", "ic"), ("ative", ""), ("alize", "al"),
              ("iciti", "ic"), ("ical", "ic"), ("ful", ""), ("ness", "")]
    for suf, rep in pairs3:
        new = _replace_suffix(w, suf, rep)
        if new != w:
            w = new
            break

    # Step 4 — m > 1 suffix removal
    suf4 = ["al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement",
            "ment", "ent", "ou", "ism", "ate", "iti", "ous", "ive", "ize"]
    for suf in suf4:
        if w.endswith(suf):
            stem = w[: -len(suf)]
            if _measure(stem) > 1:
                w = stem
                break
    if w.endswith("ion"):
        stem = w[:-3]
        if _measure(stem) > 1 and stem and stem[-1] in "st":
            w = stem

    # Step 5a
    if w.endswith("e"):
        stem = w[:-1]
        m = _measure(stem)
        if m > 1 or (m == 1 and not _ends_cvc(stem)):
            w = stem

    # Step 5b
    if _measure(w) > 1 and _ends_dbl_consonant(w) and w[-1] == "l":
        w = w[:-1]

    return w


_TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")


def compute_stem(query: str) -> str:
    """Stem a multi-word query: strip prepositions, lowercase, stem each token in ORIGINAL ORDER.

    Word order is preserved (per 陈翔宇 2026-05-13 feedback): "单词调换位置组成的长尾词不应同根".
    Examples:
      "controles nintendo switch 2" → "control nintendo switch 2"
      "controller nintendo switch 2" → "control nintendo switch 2"  ← same stem ✓
      "nintendo switch 2 controles"  → "nintendo switch 2 control"  ← different ✓ (different word order)
    """
    if not query:
        return ""
    tokens = _TOKEN_RE.findall(query.lower())
    kept = [t for t in tokens if t not in _STOP_PREPS]
    stems = [porter_stem(t) for t in kept]
    return " ".join(stems)


def is_singular_plural_only_diff(queries):
    """Return True iff the only difference within the group is singular/plural (queries.size>=2)."""
    if len(queries) < 2:
        return False
    norm = set()
    for q in queries:
        tokens = _TOKEN_RE.findall(q.lower())
        # strip trailing s/es/ies from last token to test
        if tokens:
            last = tokens[-1]
            if last.endswith("ies"):
                last = last[:-3] + "y"
            elif last.endswith("es") and len(last) > 3:
                last = last[:-2]
            elif last.endswith("s") and len(last) > 2 and not last.endswith("ss"):
                last = last[:-1]
            tokens[-1] = last
        norm.add(" ".join(tokens))
    return len(norm) == 1


def is_preposition_only_diff(queries):
    """Return True iff the only difference is presence/absence of prepositions."""
    if len(queries) < 2:
        return False
    norm = set()
    for q in queries:
        tokens = _TOKEN_RE.findall(q.lower())
        kept = [t for t in tokens if t not in _STOP_PREPS]
        norm.add(" ".join(kept))
    return len(norm) == 1
