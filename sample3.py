from logdec import logit
from multipy import MultiPyExecutor
import pandas as pd
import numpy as np


def read_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": np.arange(0, 100000001, 1)
        }
    )


@logit
def job1(df: pd.DataFrame) -> None:
    df["b"] = df["a"] * 2
    df["d"] = df["b"] - 1
    df["f"] = np.sin(df["a"])


@logit
def job2(df: pd.DataFrame) -> None:
    df["c"] = df["a"] ** 2


@logit
def job3(df: pd.DataFrame) -> None:
    df["e"] = df["a"] - 1


@logit
def job4(df: pd.DataFrame) -> None:
    df["g"] = np.cos(df["a"]) - 1


@logit
def job5(df: pd.DataFrame) -> None:
    df["h"] = np.tan(df["a"]) - 1


def main():
    nt = None
    mp = MultiPyExecutor(num_threads=nt, log_pathfile="log1.txt")
    df = read_data()
    jobs = [
        (x, [df])
        for x in [job1, job2, job3, job4, job5]
    ]

    _ = mp.execute(jobs)

    print(df.tail(20))


if __name__ == "__main__":
    main()
