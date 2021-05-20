from .LogicalPlan import LeafNode


class Range(LeafNode):
    def __init__(self, start, end, step, num_slices=1):
        super().__init__()
        self.start = start
        self.end = end
        self.step = step
        self.num_slices = num_slices

