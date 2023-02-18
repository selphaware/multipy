import pandas as pd
import numpy as np
from logdec import logit
from multipy import MultiPyExecutor
from colorama import init
from colorama import Fore, Back, Style
from time import sleep


@logit
def make_df(r, c, s, n):
    ncolor: str = list(Fore.__dict__.values())[s % n % len(Fore.__dict__)] + \
                  list(Back.__dict__.values())[(s + 1) % n % len(Back.__dict__)]

    print(ncolor + f"Started -> {(r, c, s)}" + Style.RESET_ALL)
    cols = [chr(65 + x) for x in range(c)]
    dd = {
        col: np.arange(0, r, s)
        for col in cols
    }
    df = pd.DataFrame(dd)
    sleep(c / 2)
    print(ncolor + f"Completed -> {(r, c, s)}, {df.shape}" + Style.RESET_ALL)
    return df


def main():
    nthreads = 12
    mp = MultiPyExecutor(num_threads=nthreads)
    num_jobs = 50
    funcs = [make_df for _ in range(num_jobs)]
    in_args = [
        [int(1E6) * x, 2 * x, x, nthreads]
        for x in range(num_jobs)
    ]
    results = mp.execute(funcs, in_args)
    # results = mp.execute(list(zip(funcs, in_args)))
    fbool = True

    if fbool:
        input("press any key to continue..")
        for proc, proc_res in results.items():
            print("-" * 30)
            print(proc)
            print(proc_res)
            print("-" * 30)


if __name__ == "__main__":
    init()
    main()
