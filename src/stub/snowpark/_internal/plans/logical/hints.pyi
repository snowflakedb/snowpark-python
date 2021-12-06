from typing import Any

class JoinHint:
    left_hint: Any
    right_hint: Any
    def __init__(self, left_hint, right_hint) -> None: ...
    @classmethod
    def none(cls): ...
