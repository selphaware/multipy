import logging
from typing import List, Callable, Any, Dict, Tuple, Optional, Union
from time import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import Future
from multipledispatch import dispatch
from inspect import ismethod, isfunction
from avail_cpu import available_cpu_count
from math import ceil
from functools import partial


ListCallable = List[Union[Callable, partial]]
Args = List[List[Any]]
ListCallableArgs = List[Tuple[Union[Callable, partial], List[Any]]]
Results = Dict[Tuple[int, str], Any]
OpIntFloat = Optional[Union[int, float]]


class MultiPyExecutor(object):
    def __init__(
            self,
            num_threads: OpIntFloat = None,
            log_pathfile: str = "log_mpy.txt"
            ):
        """

        :param num_threads:
        :param log_pathfile:
        """
        # calc number of threads to use
        self.max_threads = int(available_cpu_count())

        if num_threads is None:
            self.number_threads = self.max_threads - 1

        elif isinstance(num_threads, int):
            if not (1 <= num_threads <= self.max_threads):
                raise ValueError("Number of threads must be >= 1")
            self.number_threads = num_threads

        elif isinstance(num_threads, float):
            if not (0 < num_threads <= 1):
                raise ValueError("CPU percentage value must be between "
                                 "0 and 1")
            self.number_threads = ceil(num_threads * self.max_threads)

        else:
            raise ValueError("num_threads variable must be int or float")

        # set logging
        log_pathfile = log_pathfile

        logging.basicConfig(
            filename=log_pathfile, filemode='a',
            level=logging.INFO, format='%(asctime)s - %(name)s - '
                                       '%(levelname)s - %(message)s'
        )

        self.logger = logging.getLogger(__name__)

    @dispatch(list, list)
    def execute(
            self,
            procs: ListCallable,
            in_args: Args
            ) -> Results:
        return self.__execute(procs, in_args)

    @dispatch(list)
    def execute(
            self,
            procs_args: ListCallableArgs
            ) -> Results:
        procs = [x[0] for x in procs_args]
        in_args = [x[1] for x in procs_args]
        return self.__execute(procs, in_args)

    def __execute(
            self,
            procs: ListCallable,
            in_args: Args
            ) -> Results:
        """

        :param procs:
        :param in_args:
        :return:
        """
        ts = time()

        logging.info("-" * 50)
        logging.info("Starting concurrent thread pool executor "
                     f"with {self.number_threads} threads executing "
                     f"{len(procs)} jobs")

        future_results: Dict[Tuple[int, str], Any] = dict()

        with ThreadPoolExecutor(max_workers=self.number_threads) as executor:
            for proc_num, proc in enumerate(procs):
                future: Future = executor.submit(proc, *in_args[proc_num])
                if hasattr(proc, "__name__"):
                    fname = proc.__name__
                elif hasattr(proc.func, "__name__"):
                    fname = proc.func.__name__
                else:
                    fname = "<NA>"
                future_results[(proc_num, fname)] = future

        logging.info(f"Complete. Elapsed time: {time() - ts} seconds")

        final_results: Dict[Tuple[int, str], Future] = dict()
        for fut_tup in future_results.items():
            key: Tuple[int, str] = fut_tup[0]
            val_fut: Future = fut_tup[1]
            final_results[key] = val_fut.result()

        return final_results


class MultiPy(object):
    def __init__(self,
                 child_obj: object,
                 num_threads: OpIntFloat = None,
                 log_filepath: str = "log_mpy.txt"
                 ):
        """

        :param child_obj:
        :param num_threads:
        :param log_filepath:
        """
        self._multipy_executor = MultiPyExecutor(
            num_threads=num_threads, log_pathfile=log_filepath
        )

        attrs_names = [x for x in dir(child_obj) if not "_" == x[0]]
        attrs = [getattr(child_obj, x) for x in attrs_names]
        self.jobs = [x for x in attrs if ismethod(x) or isfunction(x)]

    def _execute(self) -> Any:
        """

        :return:
        """
        jobs_args = [(x, []) for x in self.jobs]
        return self._multipy_executor.execute(jobs_args)
