class SimClock:
    def __init__(self, now: int = 0) -> None:
        self.now = int(now)

    def advance_to(self, seconds: int) -> None:
        if seconds < self.now:
            raise ValueError("cannot move simulated clock backwards")
        self.now = seconds
