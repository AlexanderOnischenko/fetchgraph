"""Type aliases for JSON-like structures used in provider selectors.

These aliases describe data that must remain JSON-serializable and are
intended for use with the ``selectors`` argument in provider protocols.
"""

from typing import Dict, List, Union

JSONPrimitive = Union[str, int, float, bool, None]
JSONValue = Union[JSONPrimitive, Dict[str, "JSONValue"], List["JSONValue"]]
JSONDict = Dict[str, JSONValue]
SelectorsDict = JSONDict
