import random
import string
import sys
from hecuba import config, StorageDict


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


if __name__ == "__main__":
    try:
        rows = int(sys.argv[1])
    except IndexError:
        rows = 40000

    gen_random_data(rows)
