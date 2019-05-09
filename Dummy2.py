import math
import sys
import time
import random
import os

from hecuba import config, StorageDict
from gen_random_data import MyData, Result, Result2, gen_random_data


from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on


@task(returns=float)
def my_task(A, result, a, b):
    # @TypeSpec dict<<key0:int, key1:int>, val0:str, val1:float, val2:int, val3:str, val4:str, val5:float, val6:str>
    start_task = time.time()

    for key, values in A.iteritems():
        dummy = random.randint(0, 50)
        for _ in range(0, dummy):
            try:
                result[key] += a*values.val1 + b*values.val5
            except KeyError:
                result[key] = a*values.val1 + b*values.val5

    end_task = time.time()
    return end_task - start_task


if __name__ == "__main__":
    config.session.execute("DROP TABLE IF EXISTS my_ksp.my_data")
    config.session.execute("DROP TABLE IF EXISTS my_ksp.my_result2")
    try:
        config.partition_strategy = sys.argv[2]
        if config.partition_strategy == "SIMPLE":
            config.number_of_partitions = int(sys.argv[3])
        else:
           os.environ["NODES_NUMBER"] = sys.argv[3] 
    except IndexError:
        config.partition_strategy = "SIMPLE"

    a, b = random.uniform(0, 10), random.uniform(10, 20)
    N = int(sys.argv[1])
    gen_random_data(N)
    my_data = MyData("my_ksp.my_data")
    result = Result2("my_ksp.my_result2")
    start = time.time()
    res = []
    for i, partition in enumerate(my_data.split()):
        # print("Created task number: %s" % i)
        res.append(my_task(partition, result, a, b))

    res = compss_wait_on(res)

    end = time.time()

    print("\n\nDummy task results with matrix of {0}*{0}:\n".format(N))
    print("Used %s number of partitions" % config.number_of_partitions)
    print("Total time: %s seconds\n\n" % (round(end - start, 4)))

    print("Writing results\n\n")
    with open("/home/bsc31/bsc31310/results_dummy.txt", "a") as results:
        results.write("Dummy task results with matrix of {0}*{0}:\n".format(N))
        results.write("    Total time:  %s seconds\n" % (round(end - start, 4)))
        results.write("\n")
    print("Finished")
