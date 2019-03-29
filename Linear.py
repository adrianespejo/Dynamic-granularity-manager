import math
import time
import random

from hecuba import config, StorageDict
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from gen_random_data import MyData


@task(returns = float)
def my_task(d):
    '''
    Perform n random accesses to the data
    '''
    print("Starting my_task")
    start_task = time.time()

    keys = d.keys()
    n = len(keys)

    for i in xrange(0, n):
        access = d[random.choice(keys)].val0
    time.sleep(1)

    end_task = time.time()
    return end_task - start_task


if __name__ == "__main__":
    config.partition_strategy = "DYNAMIC"
    my_data = MyData("my_ksp.my_data")
    start = time.time()
    res = []
    for i, partition in enumerate(my_data.split()):
        print("Created task number: %s" % i)
        res.append(my_task(partition))

    res = compss_wait_on(res)

    end = time.time()
    print("Writing results")
    with open("results.txt", "a") as results:
        results.write("Linear task results:\n")
        results.write("    Total time:  %s seconds\n" % (round(end-start,4)))
        #for partitions, time in res:
        #    results.write("    %s partitions:  %s seconds.\n" % (partitions, round(time, 4)))
        results.write("\n")
    print("Finished")


