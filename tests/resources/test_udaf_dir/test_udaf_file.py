class MyUDAFWithTypeHints:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self) -> int:
        return self._sum

    def accumulate(self, input_value: int) -> None:
        self._sum += input_value

    def merge(self, other_sum: int) -> None:
        self._sum += other_sum

    def finish(self) -> int:
        return self._sum


class MyUDAFWithoutTypeHints:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self):
        return self._sum

    def accumulate(self, input_value):
        self._sum += input_value

    def merge(self, other_sum):
        self._sum += other_sum

    def finish(self):
        return self._sum
