import datetime
import decimal
from typing import Dict, Iterable, List, Tuple


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


class MyUDTFWithOptionalArgs:
    def process(
        self,
        int_req: int,
        str_req: str,
        int_opt: int = 1,
        str_opt: str = "",
        bool_: bool = False,
        decimal_: decimal.Decimal = decimal.Decimal("3.14"),  # noqa: B008
        bytes_: bytes = b"one",
        time_: datetime.time = datetime.time(16, 10, second=2),  # noqa: B008
        dict_: Dict[int, str] = {1: "a"},  # noqa: B006
    ) -> List[
        Tuple[
            int,
            str,
            int,
            str,
            bool,
            decimal.Decimal,
            bytes,
            datetime.time,
            Dict[int, str],
        ]
    ]:
        return [
            (int_req, str_req, int_opt, str_opt, bool_, decimal_, bytes_, time_, dict_)
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
