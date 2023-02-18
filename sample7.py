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


def main():
    nt = 1
    mp = MultiPyExecutor(num_threads=nt, log_pathfile="log1.txt")
    dat_df = read_data()

    j1 = 'df["s1"] = df["a"] * 42'
    j2 = 'df["s2"] = xx'
    j3 = 'df["s3"] = df["a"] * 820'

    k1 = (logit(lambda df: exec(j1)), [dat_df])
    k2 = (logit(lambda df, xx: exec(j2)), [dat_df, 5666])
    k3 = (logit(lambda df: exec(j3)), [dat_df])

    jobs = [k1, k2, k3]

    _ = mp.execute(jobs)

    print(dat_df.head(20))


if __name__ == "__main__":
    main()
