import os
import time
import unittest
from random import randint

from hecuba import config, StorageDict, StorageObj


class MyDict(StorageDict):
    '''
    @TypeSpec dict<<key0:int>, val0:str>
    '''


class PartitionerTest(unittest.TestCase):

    def computeItems(self, SDict):
        counter = 0
        for item in SDict.iterkeys():
            counter = counter + 1
        return counter

    def test_simple(self):
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        nsplits = 0
        config.partition_strategy = "SIMPLE"
        for partition in d.split():
            nsplits += 1
            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)

    def test_table_size(self):
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        nsplits = 0
        config.partition_strategy = "TABLE_SIZE"
        config.optimal_partition_size = 64  # in kb
        for partition in d.split():
            nsplits += 1
            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)

    def test_dynamic_same_time_per_token(self):
        os.environ["NODES_NUMBER"] = "5"
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        config.session.execute("DROP TABLE IF EXISTS hecuba.partitioning")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        config.partition_strategy = "DYNAMIC"
        # granularity = [32, 64, 128, 256]
        times = [(0, 80), (0, 40), (0, 20), (0, 10)]
        for nsplits, partition in enumerate(d.split()):
            if nsplits <= 3:
                # this will be done by the compss api
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [times[nsplits][0], times[nsplits][1], partition._storage_id])

            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)
        self.assertEqual(config.number_of_partitions, 32)

    def test_dynamic_different_time_per_token(self):
        os.environ["NODES_NUMBER"] = "5"
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        config.session.execute("DROP TABLE IF EXISTS hecuba.partitioning")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        config.partition_strategy = "DYNAMIC"
        # granularity = [32, 64, 128, 256]
        times = [(0, 80), (0, 38), (0, 15), (0, 3)]
        for nsplits, partition in enumerate(d.split()):
            if nsplits <= 3:
                # this will be done by the compss api
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [times[nsplits][0], times[nsplits][1], partition._storage_id])

            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)
        self.assertEqual(config.number_of_partitions, 256)

    def test_dynamic_best_without_finishing(self):
        """
        Test if the best granularity is set without finishing all the initial granularities tasks.
        This happens when all the unfinished tasks are worse than the best granularity with at least one finished task
        """
        os.environ["NODES_NUMBER"] = "5"
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        config.session.execute("DROP TABLE IF EXISTS hecuba.partitioning")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        config.partition_strategy = "DYNAMIC"
        # granularity = [32, 64, 128, 256]
        times = [(0, 80), (0, 60), (0, 50), (0, 40)]
        for nsplits, partition in enumerate(d.split()):
            # pretending that task with gran=32 is taking a lot of time
            if nsplits == 0:
                # this will be done by the compss api
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [0, partition._storage_id])
            elif nsplits <= 3:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [times[nsplits][0], times[nsplits][1], partition._storage_id])

            if nsplits >= 4:
                self.assertEqual(config.number_of_partitions, 64)

            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)
        self.assertEqual(config.number_of_partitions, 64)

    def test_dynamic_best_idle_nodes(self):
        """
        Test if the best granularity is set without finishing all the initial granularities tasks.
        This happens when all the unfinished tasks are worse than the best granularity with at least one finished task
        """
        os.environ["NODES_NUMBER"] = "5"
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        config.session.execute("DROP TABLE IF EXISTS hecuba.partitioning")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        config.partition_strategy = "DYNAMIC"
        # granularity = [32, 64, 128, 256]
        times = [(0, 80), (0, 60), (0, 50), (0, 40)]
        for nsplits, partition in enumerate(d.split()):
            # pretending that task with gran=32 is taking a lot of time
            if nsplits == 0:
                id_partition0 = partition._storage_id
                # this will be done by the compss api
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [time.time(), partition._storage_id])
            elif nsplits <= 3:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [times[nsplits][0], times[nsplits][1], partition._storage_id])
            elif nsplits == 15:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [time.time()+150, id_partition0])
            else:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                start = randint(0, 200)
                config.session.execute(query, [start, start + 60, partition._storage_id])

            if nsplits >= 4:
                self.assertEqual(config.number_of_partitions, 64)

            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)
        self.assertEqual(config.number_of_partitions, 64)

    def test_dynamic_idle_nodes_new_best(self):
        os.environ["NODES_NUMBER"] = "5"
        config.session.execute("DROP TABLE IF EXISTS my_app.mydict")
        config.session.execute("DROP TABLE IF EXISTS hecuba.partitioning")
        d = MyDict("my_app.mydict")
        nitems = 10000
        for i in range(0, nitems):
            d[i] = "RandomText" + str(i)

        time.sleep(2)
        # assert all the data has been written
        self.assertEqual(len(d.keys()), nitems)

        acc = 0
        config.partition_strategy = "DYNAMIC"
        # granularity = [32, 64, 128, 256]
        times = [(0, 80), (0, 60), (0, 50), (0, 40)]
        for nsplits, partition in enumerate(d.split()):
            if nsplits == 0:
                id_partition0 = partition._storage_id
                # this will be done by the compss api
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?
                                                  WHERE storage_id = ?""")
                # time.time() to avoid choosing gran=64 when task with gran=32 taking a lot of time
                # dynamic partitioning mode will use time.time() to check how much is taking
                config.session.execute(query, [time.time(), partition._storage_id])
            elif nsplits <= 3:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [times[nsplits][0], times[nsplits][1], partition._storage_id])
            elif nsplits == 10:
                last_time = config.session.execute("""SELECT start_time FROM hecuba.partitioning
                                                      WHERE storage_id = %s""" % id_partition0)[0][0]
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET end_time = ?
                                                  WHERE storage_id = ?""")
                config.session.execute(query, [last_time + 80, id_partition0])
            else:
                query = config.session.prepare("""UPDATE hecuba.partitioning
                                                  SET start_time = ?, end_time = ?
                                                  WHERE storage_id = ?""")
                start = randint(0, 200)
                config.session.execute(query, [start, start + 60, partition._storage_id])

            if 10 >= nsplits >= 4:
                self.assertEqual(config.number_of_partitions, 64)
            elif nsplits > 10:
                self.assertEqual(config.number_of_partitions, 32)
            acc += self.computeItems(partition)

        self.assertEqual(nitems, acc)
        self.assertEqual(config.number_of_partitions, 32)


if __name__ == "__main__":
    unittest.main()
