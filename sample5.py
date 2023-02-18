from multipy import MultiPy
from logdec import logit
import pandas as pd
import numpy as np


NUM_THREADS = .25  # None indicates maximum number of threads are employed
LOG = "log1.txt"


class DataJobs(MultiPy):
    def __init__(self, df: pd.DataFrame):
        self.df = df
        super().__init__(self, num_threads=NUM_THREADS, log_filepath=LOG)

    @logit
    def job1(self) -> None:
        self.df["b"] = self.df["a"] * 2
        self.df["d"] = self.df["b"] - 1
        self.df["f"] = np.sin(self.df["a"])

    @logit
    def job2(self) -> None:
        self.df["c"] = self.df["a"] ** 2

    @logit
    def job3(self) -> None:
        self.df["e"] = self.df["a"] - 1

    @logit
    def job4(self) -> None:
        self.df["g"] = np.cos(self.df["a"]) - 1

    @logit
    def job5(self) -> None:
        self.df["h"] = np.tan(self.df["a"]) - 1

    @logit
    def job6(self) -> None:
        self.df["i"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 2)))
        self.df["j"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 3)))
        self.df["k"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 4)))
        self.df["l"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 5)))

    @logit
    def job7(self) -> None:
        self.df["ii"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 6)))
        self.df["jj"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 7)))
        self.df["kk"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 8)))
        self.df["ll"] = np.tan(np.arcsinh(np.sin(self.df["a"] ** 9)))


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
    print(mp.df)
