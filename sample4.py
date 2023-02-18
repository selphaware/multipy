from multipy import MultiPy
from logdec import logit
from time import sleep


class MyMPy(MultiPy):
    def __init__(self):
        super().__init__(self, num_threads=3,
                         log_filepath="log1.txt")

    @logit
    def sleep1(self):
        print("Sleeping for 1 second, ")
        sleep(1)

    @logit
    def sleep2(self):
        print("Sleeping for 2 second, ")
        sleep(2)

    @logit
    def sleep3(self):
        print("Sleeping for 3 second, ")
        sleep(3)


# IDEA: ss

if __name__ == "__main__":
    mp = MyMPy()
    res = mp._execute()
    print(res)
