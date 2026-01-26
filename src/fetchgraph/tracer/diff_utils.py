from __future__ import annotations


def first_diff_path(left: object, right: object, *, prefix: str = "") -> str | None:
    if type(left) is not type(right):
        return prefix or "<root>"
    if isinstance(left, dict) and isinstance(right, dict):
        left_keys = set(left.keys())
        right_keys = set(right.keys())
        for key in sorted(left_keys | right_keys):
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                return next_prefix
            diff = first_diff_path(left[key], right[key], prefix=next_prefix)
            if diff is not None:
                return diff
        return None
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return f"{prefix}.length" if prefix else "length"
        for idx, (l_item, r_item) in enumerate(zip(left, right), start=1):
            next_prefix = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            diff = first_diff_path(l_item, r_item, prefix=next_prefix)
            if diff is not None:
                return diff
        return None
    if left != right:
        return prefix or "<root>"
    return None
