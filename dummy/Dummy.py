import os
import random
import string
import sys
import time

from hecuba import config, StorageDict
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task


class MyData(StorageDict):
    '''
    @TypeSpec dict<<i:int, j:int>, val0:str, val1:float, val2:int, val3:str, val4:str, val5:float, val6:str>
    '''


class Result(StorageDict):
    '''
    @TypeSpec dict<<i:int, j:int>, result:float>
    '''


def gen_random_data(N=10000):
    config.session.execute("DROP TABLE IF EXISTS my_ksp.my_data")
    my_data = MyData("my_ksp.my_data")
    print("Generating random matrix: %s N" % N)
    for i in range(0, N):
        for j in range(0, N):
            row = gen_random_row()
            my_data[i, j] = row


def gen_random_row():
    row = []
    row.append(gen_random_string(size=10))
    row.append(random.uniform(0, 10))
    row.append(random.randint(0, 10))
    row.append(gen_random_string(size=15))
    row.append(gen_random_string(size=10))
    row.append(random.uniform(0, 10))
    row.append(gen_random_string(size=15))
    return row


def gen_random_string(size=10, chars=string.ascii_uppercase + string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


@task(returns=float)
def my_task(A, result, a, b):
    start_task = time.time()

    for key, values in A.iteritems():
        dummy = random.randint(0, 50)
        for _ in range(0, dummy):
            try:
                result[key] += a * values.val1 + b * values.val5
            except KeyError:
                result[key] = a * values.val1 + b * values.val5

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
    result = Result("my_ksp.my_result2")
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
    print("Finished")
