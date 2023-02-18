from logdec import logit
from multipy import MultiPyExecutor
from time import sleep


@logit
def add(a, b, c):
    sleep(c)
    return a + b


@logit
def mul(a, b, c):
    sleep(c * 12.5)
    return a * b


def main():
    mp = MultiPyExecutor(num_threads=16)
    num_jobs = 16
    funcs = [add if x % 2 == 0 else mul for x in range(num_jobs)]
    in_args = [
        [x, x ** 2, 1 if x % 2 == 1 else 0.1]
        for x in range(num_jobs)
    ]
    print(in_args)
    results = mp.execute(funcs, in_args)
    for proc, proc_res in results.items():
        print(f"{proc[0]}, {proc[1]} -> {proc_res}")


if __name__ == "__main__":
    main()
