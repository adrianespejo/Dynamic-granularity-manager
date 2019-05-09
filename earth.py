from classes import GridPoints, GlobalStats
from hecuba import config
from pycompss.api.task import task
from pycompss.api.api import compss_barrier, compss_wait_on
from pycompss.api.parameter import *
import os
import sys

def compute_stats(m, v):
    m.sum += v
    if v < m.min:
        m.min = v
    if v > m.max:
        m.max = v
    m.count = m.count + 1

def get_keys(partition):
    keyDict = {}
    for r in partition.iterkeys():
        if r.time not in keyDict:
            keyDict[r.time] = {}
        if r.ilev not in keyDict[r.time]:
            keyDict[r.time][r.ilev] = {}
        if r.lat not in keyDict[r.time][r.ilev]:
            keyDict[r.time][r.ilev][r.lat] = []
        keyDict[r.time][r.ilev][r.lat].append(r.lon)
    return keyDict

@task(d=CONCURRENT)
def interpolate(partition, d, NP, dist):
    import time
    keyDict = get_keys(partition)
    localStats = GlobalStats()
    num_t = 4
    for t in keyDict:
        index = t / (num_t * 6)
        for ilev in keyDict[t]:
            for lat in keyDict[t][ilev]:
                new_lon = 0.0
                p1i = p2i = 0
                for _ in range(NP):
                    while keyDict[t][ilev][lat][p2i] < new_lon and keyDict[t][ilev][lat][p2i] < 360.0:
                        p2i += 1
                    p1i = p2i - 1
                    d1 = new_lon - keyDict[t][ilev][lat][p1i]
                    d2 = keyDict[t][ilev][lat][p2i] - new_lon
                    stats = localStats[index, lat, new_lon]
                    range_n = len(partition[lat, keyDict[t][ilev][lat][p1i], t, ilev])
                    for i in range(range_n):
                        p1 = partition[lat, keyDict[t][ilev][lat][p1i], t, ilev][i]
                        p2 = partition[lat, keyDict[t][ilev][lat][p2i], t, ilev][i]
                        p = (d1 * p2 + d2 * p1) / (d1 + d2)
                        compute_stats(stats[i], p)
                    localStats[index, lat, new_lon] = stats
                    new_lon += dist
    d.update(localStats)
    del localStats

if __name__ == "__main__":
    try:
        config.partition_strategy = sys.argv[1]
        if config.partition_strategy == "SIMPLE":
            config.number_of_partitions = int(sys.argv[2])
        else:
            os.environ["NODES_NUMBER"] = sys.argv[2]
    except:
        config.partition_strategy = "SIMPLE"

    # Number of points in the normal grid
    NP = 10#2560
    dist = 360.0 / NP
    # Using same input data for each execution
    sdict = GridPoints('test.gtgd')
    
    import numpy as np
    name = 'test' + str(np.random.randint(10000))
    dayStats = GlobalStats('day' + name)

    import time
    time1 = time.time()
    for partition in sdict.split():
        interpolate(partition, dayStats, NP, dist)
    compss_barrier()
    print("\n\nUsing strategy %s with %s partitions" % (config.partition_strategy, config.number_of_partitions))
    print "Execution time of the earth app: " + str(time.time() - time1) + " seconds.\n\n"
