from logdec import logit
from multipy import MultiPyExecutor
import pandas as pd
import numpy as np
from functools import partial


def read_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": np.arange(0, 100000001, 1)
        }
    )


def main():
    nt = 3
    mp = MultiPyExecutor(num_threads=nt, log_pathfile="log1.txt")
    dat_df = read_data()

    main_ks = [
        lambda df, in_x: exec(in_j) for in_j in
        [
            'df["s1"] = df["a"] * in_x',
            'df["s2"] = in_x',
            'df["s3"] = df["a"] * .5'
        ]
    ]

    import pdb
    pdb.set_trace()

    jobs = [
        (logit(partial(kp, dat_df, 56)), [])
        for kp in main_ks
    ]

    pdb.set_trace()

    _ = mp.execute(jobs)

    print(dat_df.head(20))


if __name__ == "__main__":
    main()
