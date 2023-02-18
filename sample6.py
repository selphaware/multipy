from multipy import MultiPy
from logdec import logit
import pandas as pd
import numpy as np


NUM_THREADS = 1  # None indicates maximum number of threads are employed
LOG = "log1.txt"


class RawJobs(object):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_feature(self, col: str, out: str, factor: int):
        self.df[out] = self.df[col] * factor


class DataJobs(MultiPy):
    def __init__(self, df: pd.DataFrame):
        self.rw = RawJobs(df)
        super().__init__(self, num_threads=NUM_THREADS, log_filepath=LOG)

    @logit
    def job1(self) -> None:
        for x, y, z in [
            ("a", "b", 2),
            ("b", "c", 3),
            ("c", "d", 4),
        ]:
            self.rw.create_feature(x, y, z)

    @logit
    def job2(self) -> None:
        for x, y, z in [
            ("a", "e", 20),
            ("a", "f", 30),
        ]:
            self.rw.create_feature(x, y, z)

    @logit
    def job3(self) -> None:
        for x, y, z in [
            ("a", "g", 200),
            ("a", "h", 300),
        ]:
            self.rw.create_feature(x, y, z)


def read_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": np.arange(0, 100000001, 1)
        }
    )


if __name__ == "__main__":
    data = read_data()
    mp = DataJobs(data)
    res = mp._execute()
    print(res)
    print(mp.rw.df)
