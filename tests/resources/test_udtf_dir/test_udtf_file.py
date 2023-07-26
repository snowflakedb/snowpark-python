import decimal
from typing import Iterable, List, Tuple


class MyUDTFWithTypeHints:
    def process(
        self,
        int_: int,
        float_: float,
        bool_: bool,
        decimal_: decimal.Decimal,
        str_: str,
        bytes_: bytes,
        byte_array_: bytearray,
    ) -> List[Tuple[int, float, bool, decimal.Decimal, str, bytes, bytearray]]:
        return [
            (
                int_,
                float_,
                bool_,
                decimal_,
                str_,
                bytes_,
                byte_array_,
            )
        ]


class MyUDTFWithoutTypeHints:
    def process(
        self,
        int_,
        float_,
        bool_,
        decimal_,
        str_,
        bytes_,
        byte_array_,
    ):
        return [
            (
                int_,
                float_,
                bool_,
                decimal_,
                str_,
                bytes_,
                byte_array_,
            )
        ]


class GeneratorUDTF:
    def process(self, n):
        for i in range(n):
            yield (i,)


class ProcessReturnsNone:
    def process(self, a: int, b: int, c: int) -> None:
        pass

    def end_partition(self) -> Iterable[Tuple[int]]:
        yield (1,)
