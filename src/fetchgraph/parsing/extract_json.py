import ast
import codecs
import json
import re
from textwrap import dedent
from typing import Any, List, Optional, Tuple, cast

# ------------------------------------------------------------
# Fences / блоки
# ------------------------------------------------------------

_JSON_FENCE_PATTERN = re.compile(r"```json\s*(?P<json>.*?)```", re.DOTALL)
_FENCE_ANY_RE = re.compile(r"```(?:\w+)?\s*(.*?)\s*```", re.DOTALL)
_STRING_RE = re.compile(r'("(?:[^"\\]|\\.)*")')
_SIMPLE_NUMBER_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


def _extract_fenced_json(text: str) -> Optional[str]:
    m = _JSON_FENCE_PATTERN.search(text)
    return m.group('json').strip() if m else None


def _extract_any_fence(text: str) -> Optional[str]:
    m = _FENCE_ANY_RE.search(text)
    return m.group(1).strip() if m else None


def _strip_outer_noise(text: str) -> str:
    """
    Оставляем содержимое от первой {/[ до последней }/] включительно.
    """
    start = re.search(r'[{[]', text)
    if not start:
        return text
    depth = 0
    end_idx: Optional[int] = None
    for i in range(start.start(), len(text)):
        ch = text[i]
        if ch in '{[':
            depth += 1
        elif ch in '}]':
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx is None:
        end_idx = len(text)

    core = text[start.start():end_idx]
    prefix = text[:start.start()]
    key_match = re.search(r'"(?:[^"\\]|\\.)*"\s*:\s*$', prefix)
    if key_match:
        key_segment = prefix[key_match.start():start.start()]
        if not re.search(r'[{[]', prefix[:key_match.start()]):
            combined = key_segment + core
            return '{' + combined.strip() + '}'
    else:
        empty_key = re.search(r'"\s*:\s*$', prefix)
        if empty_key and not re.search(r'[{[]', prefix[:empty_key.start()]):
            leading = prefix[empty_key.start():start.start()]
            key_segment = '""' + leading[1:]
            combined = key_segment + core
            return '{' + combined.strip() + '}'
    return core


def extract_bracketed_json(text: str) -> Optional[str]:
    """
    Находит ПЕРВЫЙ сбалансированный {...} или [...] с учётом строк/эскейпов.
    """
    pairs = [('{' , '}'), ('[', ']')]
    candidates: List[Tuple[int, int]] = []

    for open_char, close_char in pairs:
        stack: List[int] = []
        in_str = False
        esc = False
        for idx, ch in enumerate(text):
            if ch == '"' and not esc:
                if not in_str:
                    j = idx + 1
                    while j < len(text) and text[j].isspace():
                        j += 1
                    if j < len(text) and text[j] == ':':
                        k = idx - 1
                        while k >= 0 and text[k].isspace():
                            k -= 1
                        prev = text[k] if k >= 0 else ''
                        if prev in {'', '{', '[', ','}:
                            continue
                in_str = not in_str
            if in_str and ch == '\\' and not esc:
                esc = True
                continue
            if not in_str:
                if ch == open_char:
                    stack.append(idx)
                elif ch == close_char and stack:
                    start_idx = stack.pop()
                    candidates.append((start_idx, idx + 1))
            if esc:
                esc = False

    if not candidates:
        return None
    candidates.sort(key=lambda span: span[0])
    start, end = candidates[0]
    return text[start:end]


# ------------------------------------------------------------
# think-теги: удаляем вне fences
# ------------------------------------------------------------

_THINK_OPEN_CLOSE_RE = re.compile(r'<think\b[^>]*>.*?</think\s*>',
                                  re.IGNORECASE | re.DOTALL)
_THINK_CLOSE_ONLY_RE = re.compile(r'</think\s*>', re.IGNORECASE)
_FENCE_RE = re.compile(r'(^|\n)```(?:\w+)?\n.*?\n```', re.DOTALL)


def _find_ranges(regex: re.Pattern, text: str) -> List[Tuple[int, int]]:
    return [(m.start(), m.end()) for m in regex.finditer(text)]


def _masked_ranges(text: str, ranges: List[Tuple[int, int]]) -> List[bool]:
    mask = [False] * len(text)
    for a, b in ranges:
        for i in range(max(0, a), min(len(text), b)):
            mask[i] = True
    return mask


def remove_think_sections(text: str) -> str:
    def _remove_all(pattern: re.Pattern, s: str) -> str:
        fence_ranges = _find_ranges(_FENCE_RE, s)
        masked = _masked_ranges(s, fence_ranges)
        out: list[str] = []
        last = 0
        for m in pattern.finditer(s):
            a, b = m.start(), m.end()
            if any(masked[a:b]):
                continue
            out.append(s[last:a])
            last = b
        out.append(s[last:])
        return ''.join(out)

    text = _remove_all(_THINK_OPEN_CLOSE_RE, text)
    text = _remove_all(_THINK_CLOSE_ONLY_RE, text)
    return text


# ------------------------------------------------------------
# «Мягкие» правки (вне строк)
# ------------------------------------------------------------

_UNQUOTED_KEY_RE = re.compile(r'(?P<prefix>\{|,|\[|\}|\]|^)(?P<key>\s*[A-Za-z_][A-Za-z0-9_]*)\s*(?=\s*:)')
_SINGLE_QUOTE_RE = re.compile(r"'([^'\\]*(?:\\.[^'\\]*)*)'")
_TRAILING_COMMA_RE = re.compile(r',\s*(?=[}\]])')
_LINE_COMMENT_RE = re.compile(r'(?<!:)//.*?$|/\*.*?\*/', re.DOTALL | re.MULTILINE)
_INSERT_COMMA_RE = re.compile(r'"\s*\n(\s*)"')
_BARE_WORD_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def strip_comments(text: str) -> str:
    parts = re.split(_STRING_RE, text)
    for i in range(0, len(parts), 2):
        parts[i] = _LINE_COMMENT_RE.sub('', parts[i])
    return ''.join(parts)


def remove_trailing_commas(text: str) -> str:
    return _TRAILING_COMMA_RE.sub('', text)


def normalize_single_to_double_quotes_outside_strings(text: str) -> str:
    parts = re.split(r'(".*?(?<!\\)")', text, flags=re.DOTALL)
    for i in range(0, len(parts), 2):
        def repl(m: re.Match) -> str:
            inner = m.group(1).replace('"', r'\"')
            return f"\"{inner}\""

        parts[i] = _SINGLE_QUOTE_RE.sub(repl, parts[i])
    return ''.join(parts)


def insert_missing_commas(text: str) -> str:
    return _INSERT_COMMA_RE.sub(r'",\n\1"', text)


def quote_unquoted_keys(text: str) -> str:
    def repl(m: re.Match) -> str:
        prefix = m.group('prefix')
        key = m.group('key').strip()
        return f'{prefix}"{key}"'

    parts = re.split(r'("(?:[^"\\]|\\.)*")', text)
    for i in range(0, len(parts), 2):
        parts[i] = _UNQUOTED_KEY_RE.sub(repl, parts[i])
    return ''.join(parts)


def _ensure_comma_before_value(out: list[str]) -> None:
    """Вставляет запятую перед новым значением массива, если её не хватает."""
    ws: list[str] = []
    while out and out[-1].isspace():
        ws.append(out.pop())
    ws.reverse()
    if out and out[-1] not in '{[,':
        if out[-1] != ',':
            out.append(',')
    out.extend(ws)


def quote_bare_identifiers_in_values(text: str) -> str:
    """Оборачивает голые идентификаторы в массивных/объектных значениях в кавычки.

    Попутно дозаполняет пропущенные запятые между значениями массивов. Это нужно,
    чтобы конструкция вроде `"value"\n  end` превращалась в `"value",\n  "end"`.
    """

    out: list[str] = []
    stack: list[dict[str, str]] = []
    in_str = False
    esc = False
    i = 0
    n = len(text)

    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
                    if stack:
                        ctx = stack[-1]
                        if ctx['type'] == 'object':
                            if ctx['expect'] == 'key':
                                ctx['expect'] = 'post_key'
                            elif ctx['expect'] == 'value':
                                ctx['expect'] = 'post_value'
                        elif ctx['type'] == 'array' and ctx['expect'] == 'value':
                            ctx['expect'] = 'post_value'
            i += 1
            continue

        if ch in '{[':
            stack.append({'type': 'object' if ch == '{' else 'array',
                          'expect': 'key' if ch == '{' else 'value'})
            out.append(ch)
            i += 1
            continue

        if ch in '}]':
            if stack:
                stack.pop()
            out.append(ch)
            i += 1
            if stack:
                parent = stack[-1]
                if parent['type'] == 'array' and parent['expect'] == 'value':
                    parent['expect'] = 'post_value'
                elif parent['type'] == 'object' and parent['expect'] == 'value':
                    parent['expect'] = 'post_value'
            continue

        if ch == ',':
            out.append(ch)
            i += 1
            if stack:
                ctx = stack[-1]
                if ctx['type'] == 'array':
                    ctx['expect'] = 'value'
                else:
                    ctx['expect'] = 'key'
            continue

        if ch == ':':
            out.append(ch)
            i += 1
            if stack and stack[-1]['type'] == 'object':
                stack[-1]['expect'] = 'value'
            continue

        if ch.isspace():
            out.append(ch)
            i += 1
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue

        j = i
        while j < n and (text[j].isalnum() or text[j] in {'_', '-'}):
            j += 1
        token = text[i:j]
        if not token:
            out.append(ch)
            i += 1
            continue

        handled = False
        if stack:
            ctx = stack[-1]
            if ctx['type'] == 'array':
                if ctx['expect'] == 'post_value':
                    _ensure_comma_before_value(out)
                    ctx['expect'] = 'value'
                if ctx['expect'] == 'value':
                    lowered = token.lower()
                    if lowered in {'true', 'false', 'null'}:
                        out.append(lowered)
                    elif _SIMPLE_NUMBER_RE.match(token):
                        out.append(token)
                    elif _BARE_WORD_RE.match(token):
                        out.append(f'"{token}"')
                    else:
                        out.append(token)
                    ctx['expect'] = 'post_value'
                    handled = True
            elif ctx['type'] == 'object' and ctx['expect'] == 'value':
                lowered = token.lower()
                if lowered in {'true', 'false', 'null'}:
                    out.append(lowered)
                elif _SIMPLE_NUMBER_RE.match(token):
                    out.append(token)
                elif _BARE_WORD_RE.match(token):
                    out.append(f'"{token}"')
                else:
                    out.append(token)
                ctx['expect'] = 'post_value'
                handled = True

        if not handled:
            out.append(token)
        i = j

    return ''.join(out)


def remove_unmatched_brackets(text: str) -> str:
    depth = {'{': 0, '[': 0}
    out = []
    in_str = False
    esc = False
    for ch in text:
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            continue

        if ch == '{':
            depth['{'] += 1
            out.append(ch)
        elif ch == '}' and depth['{'] > 0:
            depth['{'] -= 1
            out.append(ch)
        elif ch == '[':
            depth['['] += 1
            out.append(ch)
        elif ch == ']' and depth['['] > 0:
            depth['['] -= 1
            out.append(ch)
        else:
            if ch not in '{}[]':
                out.append(ch)
    return ''.join(out)


def _unescape_ws_outside_strings(text: str) -> str:
    """
    \n/\r/\t → реальные whitespace ТОЛЬКО вне строк.
    """
    out = []
    in_str = False
    esc = False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            i += 1
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue

        if ch == '\\' and i + 1 < n:
            nxt = text[i + 1]
            if nxt == 'n':
                out.append('\n')
                i += 2
                continue
            if nxt == 't':
                out.append('\t')
                i += 2
                continue
            if nxt == 'r':
                out.append('\r')
                i += 2
                continue

        out.append(ch)
        i += 1
    return ''.join(out)


# ------------------------------------------------------------
# Управляющие символы ВНУТРИ строк
# ------------------------------------------------------------


def escape_control_chars_inside_strings(s: str) -> str:
    """
    Внутри строк экранируем управляющие (<0x20) → \n/\r/\t/\\u00XX.
    Если строка оканчивается на '\\n' + пробелы/табуляции — хвост пробелов срезаем
    (сам '\\n' сохраняем).
    """
    out: List[str] = []
    in_s = False
    esc = False
    buf: Optional[List[str]] = None

    i, n = 0, len(s)
    while i < n:
        ch = s[i]

        if in_s:
            assert buf is not None
            if esc:
                buf.append(ch)
                esc = False
                i += 1
                continue
            if ch == '\\':
                buf.append('\\')
                esc = True
                i += 1
                continue
            if ch == '"':
                inner = ''.join(buf)
                res, esc2 = [], False
                for c in inner:
                    if esc2:
                        res.append(c)
                        esc2 = False
                        continue
                    if c == '\\':
                        res.append('\\')
                        esc2 = True
                        continue
                    oc = ord(c)
                    if oc < 0x20:
                        if c == '\n':
                            res.append('\\n')
                        elif c == '\r':
                            res.append('\\r')
                        elif c == '\t':
                            res.append('\\t')
                        else:
                            res.append('\\u%04x' % oc)
                    else:
                        res.append(c)
                inner_esc = ''.join(res)
                pos = inner_esc.rfind('\\n')
                if pos != -1 and pos + 2 < len(inner_esc):
                    after = inner_esc[pos + 2:]
                    if re.fullmatch(r'(?:[ \\t]|\\t|\\ )*', after):
                        inner_esc = inner_esc[:pos + 2]

                out.append(inner_esc)
                out.append('"')
                in_s = False
                buf = None
                i += 1
                continue

            buf.append(ch)
            i += 1
            continue

        out.append(ch)
        if ch == '"':
            in_s = True
            buf = cast(List[str], [])
        i += 1

    return ''.join(out)


def _escape_control_chars_inside_strings(s: str) -> str:
    return escape_control_chars_inside_strings(s)


# ------------------------------------------------------------
# Эллипсисы и утилиты
# ------------------------------------------------------------

ELLIPSIS_MARKER = "ELLIPSIS"


def _replace_ellipsis_outside_strings(text: str, marker: str = ELLIPSIS_MARKER) -> str:
    out = []
    i, n = 0, len(text)
    in_str, esc = False, False
    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            i += 1
            continue
        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch == '.' and i + 2 < n and text[i:i + 3] == '...':
            out.append(f'"{marker}"')
            i += 3
            continue
        out.append(ch)
        i += 1
    return ''.join(out)


def _prune_ellipsis_markers(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_prune_ellipsis_markers(x) for x in obj if not (isinstance(x, str) and x == ELLIPSIS_MARKER)]
    if isinstance(obj, dict):
        return {k: _prune_ellipsis_markers(v)
                for k, v in obj.items()
                if not (isinstance(v, str) and v == ELLIPSIS_MARKER)}
    return obj


_ESC_RE = re.compile(r'\\[nrt"\\u]')


def _looks_like_json_doc(s: str) -> bool:
    t = s.lstrip()
    return t.startswith('{') or t.startswith('[')


def _escape_density(s: str) -> float:
    m = _ESC_RE.findall(s)
    return (len(m) / max(len(s), 1))


def _is_quoted_literal(t: str) -> bool:
    t = t.strip()
    return len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'"))


# ------------------------------------------------------------
# Вспомогательные детекторы/фиксы
# ------------------------------------------------------------


def _has_esc_outside_strings(s: str) -> bool:
    i, n = 0, len(s)
    in_str = False
    esc = False
    while i < n:
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            i += 1
            continue

        if ch == '"':
            in_str = True
            i += 1
            continue

        if ch == '\\' and i + 1 < n:
            nxt = s[i + 1]
            if nxt in ['"', 'n', 't', 'r', '{', '}', '[', ']'] or (
                nxt == 'u' and i + 5 < n and re.match(r'[0-9a-fA-F]{4}', s[i + 2:i + 6])
            ):
                return True
        i += 1
    return False


def _fix_stray_backslashes_outside_strings(s: str) -> str:
    """
    Удаляет лишние backslash перед структурными токенами вне строк и
    нормализует пробелы перед двоеточием (кейс \": {...}).
    """
    out, in_str, esc, i, n = [], False, False, 0, len(s)

    while i < n:
        ch = s[i]

        if in_str:
            if ch == '\\' and i + 1 < n and s[i + 1] == '"':
                j = i + 2
                while j < n and s[j].isspace():
                    j += 1
                if j < n and s[j] == ':':
                    i += 1
                    continue

            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            i += 1
            continue

        if ch == '\\' and i + 1 < n and s[i + 1] in '{}[]":':
            i += 1
            continue

        if ch == '"':
            j = i + 1
            while j < n and s[j].isspace():
                j += 1
            if j < n and s[j] == ':':
                out.append(ch)
                i += 1
                continue

            in_str = True
            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    result = ''.join(out)

    parts = re.split(r'("(?:[^"\\]|\\.)*")', result, flags=re.DOTALL)
    for k in range(0, len(parts), 2):
        parts[k] = re.sub(r'"\s*:\s*', '": ', parts[k])
    return ''.join(parts)


# ------------------------------------------------------------
# Пилинг
# ------------------------------------------------------------


def _peel_once(text: str) -> tuple[str, bool]:
    changed = False
    t = text

    t2 = _fix_stray_backslashes_outside_strings(t)
    if t2 != t:
        t = t2
        changed = True

    block = extract_bracketed_json(t)
    if block and block != t:
        start = t.find(block)
        m = re.search(r'"\s*:\s*$', t[:start])
        if m:
            t = t[m.start():start] + block
        else:
            t = block

    changed = True
    t2 = _unescape_ws_outside_strings(t)
    if t2 != t:
        t = t2
        changed = True

    return t, changed


def remove_stray_quoted_strings(text: str) -> str:
    """
    Удаляет строковые литералы, которые не вписываются в ожидаемую структуру
    объекта: например, второй литерал в {"s":"line1"\n"line2"}. Для этого
    отслеживаем стек контекстов (объекты/массивы) и проверяем, появляется ли
    строка на месте ключа без последующего двоеточия — в таком случае удаляем
    её вместе с вставленной ранее запятой.
    """
    out: list[str] = []
    stack: list[dict[str, str]] = []
    in_str = False
    esc = False
    string_start = 0
    pending_key_ctx: Optional[dict[str, str]] = None
    i, n = 0, len(text)

    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
                    if pending_key_ctx is not None:
                        j = i + 1
                        while j < n and text[j].isspace():
                            j += 1
                        if j < n and text[j] == ':':
                            pending_key_ctx['expect'] = 'value'
                        else:
                            del out[string_start:]
                            k = len(out) - 1
                            while k >= 0 and out[k].isspace():
                                k -= 1
                            if k >= 0 and out[k] == ',':
                                out.pop(k)
                            pending_key_ctx['expect'] = 'key'
                        pending_key_ctx = None
                    else:
                        if stack:
                            ctx = stack[-1]
                            if ctx['type'] == 'object' and ctx.get('expect') == 'value':
                                ctx['expect'] = 'post_value'
                            elif ctx['type'] == 'array':
                                ctx['expect'] = 'post_value'
            i += 1
            continue

        if ch == '"':
            context = stack[-1] if stack else None
            string_start = len(out)
            out.append(ch)
            in_str = True
            esc = False
            pending_key_ctx = None
            if context and context['type'] == 'object':
                if context.get('expect') in {'key', 'post_value'}:
                    pending_key_ctx = context
                    context['expect'] = 'post_key'
            i += 1
            continue

        out.append(ch)
        if ch == '{':
            stack.append({'type': 'object', 'expect': 'key'})
        elif ch == '[':
            stack.append({'type': 'array', 'expect': 'value'})
        elif ch == '}':
            if stack:
                stack.pop()
            if stack:
                parent = stack[-1]
                if parent['type'] == 'object':
                    parent['expect'] = 'post_value'
        elif ch == ']':
            if stack:
                stack.pop()
            if stack:
                parent = stack[-1]
                if parent['type'] == 'object':
                    parent['expect'] = 'post_value'
        elif ch == ':':
            if stack and stack[-1]['type'] == 'object':
                stack[-1]['expect'] = 'value'
        elif ch == ',':
            if stack:
                ctx = stack[-1]
                if ctx['type'] == 'object':
                    ctx['expect'] = 'key'
                elif ctx['type'] == 'array':
                    ctx['expect'] = 'value'
        i += 1

    return ''.join(out)


# ---------------------------------------------------------------------------
# Unwrap embedded JSON strings
# ---------------------------------------------------------------------------

def _unwrap_embedded_json_strings(text: str) -> str:
    out = []
    i, n = 0, len(text)
    in_str = False
    esc = False
    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            else:
                if ch == '\\':
                    esc = True
                elif ch == '"':
                    in_str = False
            i += 1
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue

        if ch == ':':
            out.append(ch)
            i += 1
            while i < n and text[i].isspace():
                out.append(text[i])
                i += 1
            if i < n and text[i] == '"':
                i += 1
                spaces_after = []
                while i < n and text[i].isspace():
                    spaces_after.append(text[i])
                    i += 1
                if i < n and text[i] in '{[':
                    start = i
                    nested = extract_bracketed_json(text[start:])
                    if nested:
                        end = start + len(nested)
                        j = end
                        while j < n and text[j].isspace():
                            j += 1
                        if j < n and text[j] == '"':
                            out.extend(spaces_after)
                            out.extend(text[start:end])
                            i = j + 1
                            continue
                out.append('"')
                out.extend(spaces_after)
                in_str = True
                esc = False
                continue
            continue

        out.append(ch)
        i += 1

    return ''.join(out)


def _peel_until_json(text: str, max_rounds: int = 6) -> tuple[dict | list | None, str]:
    t = text
    for _ in range(max_rounds):
        try:
            return json.loads(t), t
        except json.JSONDecodeError:
            pass

        t_new, changed = _peel_once(t)

        try:
            return json.loads(t_new), t_new
        except json.JSONDecodeError:
            pass

        if not changed:
            break
        t = t_new

    try:
        return json.loads(t), t
    except json.JSONDecodeError:
        return None, t


# ------------------------------------------------------------
# Итеративное снятие unicode_escape с попытками парсинга
# ------------------------------------------------------------


def _iter_unicode_unescape_and_try_parse(s: str, max_rounds: int = 3) -> tuple[dict | list | None, str]:
    """
    На каждом шаге (до max_rounds):
      - если похоже на JSON и есть «грязные» esc вне строк — снимаем один слой unicode_escape,
      - экранируем управляющие ВНУТРИ строк,
      - заменяем ... вне строк маркером,
      - пытаемся json.loads.
    """
    t = s
    for _ in range(max_rounds):
        try:
            return json.loads(t), t
        except json.JSONDecodeError:
            pass

        if not _looks_like_json_doc(t) and not _has_esc_outside_strings(t):
            break

        try:
            t_dec = codecs.decode(t, 'unicode_escape')
        except Exception:
            break

        t_dec = _escape_control_chars_inside_strings(t_dec)
        t_dec = _replace_ellipsis_outside_strings(t_dec)

        try:
            obj = json.loads(t_dec)
            obj = _prune_ellipsis_markers(obj)
            return obj, t_dec
        except json.JSONDecodeError:
            t_fix = _fix_stray_backslashes_outside_strings(t_dec)
            if t_fix != t_dec:
                try:
                    obj = json.loads(t_fix)
                    obj = _prune_ellipsis_markers(obj)
                    return obj, t_fix
                except json.JSONDecodeError:
                    pass
            t = t_dec

    try:
        return json.loads(t), t
    except json.JSONDecodeError:
        return None, t


_ESC_NEED_RE = re.compile(r'(?:\\){2,}(?:["ntr\\]|u[0-9a-fA-F]{4})')


def _needs_more_unicode_decode(s: str) -> bool:
    in_str = False
    esc = False
    for ch in s:
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == '\\':
                return True
    return bool(_ESC_NEED_RE.search(s))


def unquote_json_seed(raw: str, max_rounds: int = 4) -> str:
    """
    Возвращает строку с ПЕРВЫМ валидным JSON-блоком ({…} или […]) из `raw`,
    «раскавычивая» и снимая максимум один-два unicode_escape-слоя при необходимости.
    Идемпотентна: повторный вызов на её результате не меняет строку.

    Примеры допустимого мусора вокруг:
      '\": { ... } ```"}'
      !!! NOISE !!! "[ {\\\"k\\\":1} ]" tail
    """
    t = dedent(raw).strip()

    two_layer_prefix = re.match(r"^'\s*\"\s*:\s*", t)
    if two_layer_prefix:
        t = t[two_layer_prefix.end():]
        if t.endswith("'"):
            t = t[:-1]

    if t.startswith('"') and re.match(r'^"\s*:\s*[{\[]', t):
        t = '{"' + t + '}'

    stripped_for_guard = t.lstrip('\ufeff')
    if not (stripped_for_guard.startswith('{') or stripped_for_guard.startswith('[')):
        fenced = _extract_any_fence(t)
        if fenced:
            t = fenced

    fallback_block: Optional[str] = None

    for _ in range(max_rounds):
        block = extract_bracketed_json(t)
        if not block or not _is_plausible_json_block(block):
            alt_block, start_idx, alt_fallback = _seek_plausible_json_block(t)
            if fallback_block is None and alt_fallback is not None:
                fallback_block = alt_fallback
            if alt_block:
                prefix_match = re.search(r'"\s*:\s*$', t[:start_idx])
                if prefix_match:
                    block = t[prefix_match.start(): start_idx + len(alt_block)]
                else:
                    block = alt_block
        if block:
            if block.startswith('"') and re.match(r'^"\s*:\s*[{[]', block):
                block = '{"' + block + '}'
            if fallback_block is None:
                fallback_block = block.strip()
            if _needs_more_unicode_decode(block):
                try:
                    dec = codecs.decode(block, 'unicode_escape')
                except Exception:
                    dec = block
                return dec.strip()
            return block.strip()

        if _is_quoted_literal(t):
            try:
                t2 = ast.literal_eval(t)
                if isinstance(t2, str):
                    t = t2
                    continue
            except Exception:
                t = t[1:-1]
                continue

        if _needs_more_unicode_decode(t):
            try:
                t = codecs.decode(t, 'unicode_escape')
            except Exception:
                pass
            continue

        break

    final_block = extract_bracketed_json(t)
    if final_block:
        return final_block.strip()
    if fallback_block is not None:
        return fallback_block
    return t.strip()


def _is_plausible_json_block(block: str) -> bool:
    block = block.strip()
    if not block:
        return False
    head = block[0]
    tail = block[-1]
    if head == '{' and tail == '}':
        inner = block[1:-1].strip()
        if not inner:
            return True
        if ':' in inner:
            return True
        if inner.startswith('{') or inner.startswith('['):
            return True
        return False
    if head == '[' and tail == ']':
        inner = block[1:-1].strip()
        if not inner:
            return True
        if any(ch in inner for ch in ':,"{}[]'):
            return True
        if _SIMPLE_NUMBER_RE.match(inner):
            return True
        if inner in {'true', 'false', 'null'}:
            return True
        return False
    return False


def _seek_plausible_json_block(text: str) -> tuple[Optional[str], int, Optional[str]]:
    fallback = None
    n = len(text)
    idx = 0
    while idx < n:
        ch = text[idx]
        if ch not in '{[':
            idx += 1
            continue
        candidate_text = text[idx:]
        block = extract_bracketed_json(candidate_text)
        if not block:
            idx += 1
            continue
        if fallback is None:
            fallback = block.strip()
        if _is_plausible_json_block(block):
            return block, idx, fallback
        idx += 1
    return None, 0, fallback


def remove_escaped_quotes_in_keys(text: str) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        return f'"{key}"'

    parts = re.split(_STRING_RE, text)
    for i in range(0, len(parts), 2):
        seg = parts[i]
        seg = re.sub(r'\\"([^"\\]+)\\"(?=\s*:)', repl, seg)
        parts[i] = seg
    return ''.join(parts)


# ------------------------------------------------------------
# Основная функция
# ------------------------------------------------------------


def extract_json(raw: str) -> str:
    """
    Достаёт JSON из текста/фенсов, аккуратно снимает «слои», чинит только
    внестроковые артефакты и возвращает pretty JSON (ensure_ascii=False).
    """
    content = dedent(raw).strip()
    content = remove_think_sections(content)

    seed = _extract_fenced_json(content) or content
    seed = unquote_json_seed(seed) or seed

    obj, candidate = _peel_until_json(seed)
    if obj is not None:
        return json.dumps(obj, indent=2, ensure_ascii=False)

    obj2, _txt2 = _iter_unicode_unescape_and_try_parse(candidate, max_rounds=3)
    if obj2 is not None:
        return json.dumps(obj2, indent=2, ensure_ascii=False)

    cand2 = _replace_ellipsis_outside_strings(candidate)
    try:
        obj = json.loads(cand2)
        obj = _prune_ellipsis_markers(obj)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        pass

    core = extract_bracketed_json(candidate) or candidate

    core_try = _replace_ellipsis_outside_strings(core)
    try:
        lit = ast.literal_eval(core_try)
        if isinstance(lit, str):
            obj3, _ = _iter_unicode_unescape_and_try_parse(lit, max_rounds=3)
            if obj3 is not None:
                return json.dumps(obj3, indent=2, ensure_ascii=False)
    except Exception:
        pass

    stage = candidate
    stage = strip_comments(stage)
    stage = normalize_single_to_double_quotes_outside_strings(stage)
    stage = insert_missing_commas(stage)
    stage = quote_unquoted_keys(stage)
    stage = remove_trailing_commas(stage)
    stage = remove_unmatched_brackets(stage)
    stage = _fix_stray_backslashes_outside_strings(stage)
    stage = _unwrap_embedded_json_strings(stage)
    stage = quote_bare_identifiers_in_values(stage)
    stage = remove_escaped_quotes_in_keys(stage)
    stage = remove_stray_quoted_strings(stage)
    stage = _strip_outer_noise(stage)
    stage = _escape_control_chars_inside_strings(stage)

    obj = json.loads(stage)

    def _unwrap_json_in_strings(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _unwrap_json_in_strings(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_unwrap_json_in_strings(v) for v in value]
        if isinstance(value, str):
            s = value.strip()
            if (s.startswith('{') and s.endswith('}')) or (s.startswith('[') and s.endswith(']')):
                try:
                    inner = json.loads(s)
                    return _unwrap_json_in_strings(inner)
                except Exception:
                    pass
        return value

    obj = _unwrap_json_in_strings(obj)
    return json.dumps(obj, indent=2, ensure_ascii=False)


__all__ = ["extract_json"]
