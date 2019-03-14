import math
import time
import random

from hecuba import config, StorageDict
from gen_random_data import MyData, gen_random_data
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on


@task(returns=float)
def my_task(d):
    '''
    Perform n^2 random accesses to the data
    '''
    start_task = time.time()

    keys = d.keys()
    n = len(keys)

    for i in range(0, n):
        for j in range(0, n):
            access = d[random.choice(keys)].val0

    end_task = time.time()
    return end_task - start_task


if __name__ == "__main__":
    config.partition_strategy = "DYNAMIC"
    my_data = MyData("my_ksp.my_data")
    start = time.time()
    d = dict()
    for partition in my_data.split():
        d[partition.number_of_partitions] = my_task(my_data)

    d = compss_wait_on(d)
    end = time.time()
    with open("results.txt", "a") as results:
        results.write("Quadratic task results:\n")
        results.write("    Total time:  %s seconds\n" % (end-start))
        for partitions, time in d.iteritems():
            results.write("    %s partitions:  %s seconds.\n" % (partitions, round(time, 4)))
        results.write("\n")


