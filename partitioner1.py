import os
import subprocess
import time
import uuid
from collections import defaultdict
from math import ceil

from hecuba import config, log


def partitioner_split(father):
    if config.partition_strategy == "TABLE_SIZE":
        return TableSizePartitioner(father).split()
    elif config.partition_strategy == "DYNAMIC":
        return DynamicPartitioner(father).split()
    else:
        # config.partition_strategy == "SIMPLE" or a wrong strategy
        return SimplePartitioner(father).split()


class SimplePartitioner(object):

    def __init__(self, father):
        self._father = father

    @staticmethod
    def _tokens_partitions(tokens, min_number_of_tokens, number_of_partitions):
        """
        Method that calculates the new token partitions for a given object
        Args:
            tokens: current number of tokens of the object
            min_number_of_tokens: defined minimum number of tokens
        Returns:
            a partition everytime it's called
        """
        if len(tokens) < min_number_of_tokens:
            # In this case we have few token and thus we split them
            tkns_per_partition = min_number_of_tokens / number_of_partitions
            step_size = ((2 ** 64) - 1) / min_number_of_tokens
            partition = []
            for fraction, to in tokens:
                while fraction < to - step_size:
                    partition.append((fraction, fraction + step_size))
                    fraction += step_size
                    if len(partition) >= tkns_per_partition:
                        yield partition
                        partition = []
                # Adding the last token
                partition.append((fraction, to))
            if len(partition) > 0:
                yield partition
        else:

            # This is the case we have more tokens than partitions,.
            splits = max(len(tokens) / number_of_partitions, 1)

            for i in xrange(0, len(tokens), splits):
                yield tokens[i:i + splits]

    def split(self):
        '''
        config.partition_strategy == "SIMPLE"
        Data will be partitioned in config.number_of_partitions different chunks
        :return: an iterator over partitions
        '''
        st = time.time()
        tokens = self._father._build_args.tokens

        for token_split in self._tokens_partitions(tokens, config.min_number_of_tokens, config.number_of_partitions):
            storage_id = uuid.uuid4()
            log.debug('assigning to %s %d  tokens', str(storage_id), len(token_split))
            new_args = self._father._build_args._replace(tokens=token_split, storage_id=storage_id)
            yield self._father.__class__.build_remotely(new_args._asdict())
        log.debug('completed split of %s in %f', self._father.__class__.__name__, time.time() - st)


class TableSizePartitioner(object):

    def __init__(self, father):
        self._father = father

    def split(self):
        '''
        config.partition_strategy == "TABLE_SIZE"
        Data will be partitioned in table_size // config.optimal_partition_size different chunks
        :return: an iterator over partitions
        '''
        table_size = self._get_table_size()
        if table_size == 0:
            config.partition_strategy = "SIMPLE"
            print("Could not get table size, proceeding to use simple strategy partitioning.")
        else:
            config.number_of_partitions = ceil(float(table_size) / float(config.optimal_partition_size))
        return SimplePartitioner(self._father).split()

    def _get_table_size(self):
        '''
        :return: the size of the table in kilobytes
        '''
        # system.size_estimates can take time to be refreshed
        output_flush = output_refresh = 1
        for node in config.contact_names + ["localhost"]:
            try:
                # first we need to flush the Cassandra cache, 0 if finished correctly
                output_flush = subprocess.call(
                    "nodetool -h {} flush -- {} {}".format(node, self._father._ksp, self._father._table),
                    shell=True)
                # then we refresh the system.size_estimates table, 0 if finished correctly
                output_refresh = subprocess.call("nodetool -h {} refreshsizeestimates".format(node), shell=True)
            except Exception as ex:
                print("Could not flush data in node {}.".format(node))
                print(ex)
            if output_flush == 0 and output_refresh == 0:
                break
        else:
            print("Could not flush data in any node.")
            return 0

        prepared_get_size = config.session.prepare(
            "SELECT mean_partition_size, partitions_count FROM system.size_estimates WHERE keyspace_name='{}' and table_name='{}'".format(
                self._father._ksp, self._father._table))

        res = None
        # some attempts to wait until the cache is flushed and the system.size_estimates table is refreshed
        attempts = 0
        while attempts < 5:
            res = config.session.execute(prepared_get_size)
            if res:
                break
            attempts += 1
            time.sleep(1)

        if not res:
            print("Could not get table size.")
            return 0

        # aggregate info of the table
        total_size_bytes = 0
        for partition_size, partitions_count in res:
            total_size_bytes += partition_size * partitions_count

        total_size_kb = total_size_bytes / 1000
        return total_size_kb


class DynamicPartitioner(object):
    def __init__(self, father):
        self._father = father
        self._setup_dynamic_structures()

    def split(self):
        '''
        config.partition_strategy == "DYNAMIC"
        Data will be partitioned in config.number_of_partitions different chunks
        :return: an iterator over partitions
        '''
        st = time.time()
        tokens = self._father._build_args.tokens

        for token_split in self._tokens_partitions(tokens, config.min_number_of_tokens):
            storage_id = uuid.uuid4()
            log.debug('assigning to %s %d  tokens', str(storage_id), len(token_split))
            new_args = self._father._build_args._replace(tokens=token_split, storage_id=storage_id)
            partitioned_object = self._father.__class__.build_remotely(new_args._asdict())
            # so we know in a task the number of partitions of this splitted object
            # partitioned_object.number_of_partitions = config.number_of_partitions
            table_name = "%s.%s" % (self._father._ksp, self._father._table)
            config.session.execute(self._prepared_store_id,
                                   [table_name, partitioned_object._storage_id, config.number_of_partitions])
            yield partitioned_object
        log.debug('completed split of %s in %f', self._father.__class__.__name__, time.time() - st)

    def _setup_dynamic_structures(self):
        try:
            config.session.execute("""CREATE TABLE IF NOT EXISTS hecuba.partitioning(
                                        table_name text,
                                        storage_id uuid,
                                        number_of_partitions int,
                                        start_time double,
                                        end_time double,
                                        PRIMARY KEY (storage_id))""")
            config.session.execute("TRUNCATE TABLE hecuba.partitioning")
            print("Created table hecuba.partitioning")
        except Exception as ex:
            print("Could not create table hecuba.partitioning.")
            raise ex

        self._prepared_store_id = config.session.prepare("""INSERT INTO hecuba.partitioning
                                                            (table_name, storage_id, number_of_partitions)
                                                            VALUES (?, ?, ?)""")
        self._partitions_time = defaultdict(list)
        self._best_granularity = None
        # compute self._basic_partitions depending on the number of nodes
        try:
            nodes_number = len(os.environ["PYCOMPSS_NODES"].split(","))
        except KeyError:
            nodes_number = int(os.environ["NODES_NUMBER"])  # master and worker
        self._initial_send = nodes_number - 1  # -1 because one node will be the master
        partitions = [32, 64, 128, 256, 512, 768, 1024, 2048, 48, 96, 192, 384, 1536, 3072, 4096, 5120]
        self._basic_partitions = partitions[:self._initial_send]
        print("Basic partitions: %s" % self._basic_partitions)

    def _tokens_partitions(self, tokens, min_number_of_tokens):
        """
        Method that calculates the new token partitions for a given object
        Args:
            tokens: current number of tokens of the object
            min_number_of_tokens: defined minimum number of tokens
        Returns:
            a partition everytime it's called
        """
        config.number_of_partitions = self._choose_number_of_partitions()
        tkns_per_partition = min_number_of_tokens / config.number_of_partitions

        if len(tokens) < min_number_of_tokens:
            # In this case we have few token and thus we split them
            step_size = ((2 ** 64) - 1) / min_number_of_tokens
            partition = []
            for fraction, to in tokens:
                while fraction < to - step_size:
                    partition.append((fraction, fraction + step_size))
                    fraction += step_size
                    if len(partition) >= tkns_per_partition:
                        yield partition
                        partition = []
                        config.number_of_partitions = self._choose_number_of_partitions()
                        tkns_per_partition = min_number_of_tokens / config.number_of_partitions
                # Adding the last token
                partition.append((fraction, to))
            if len(partition) > 0:
                yield partition
        else:
            i = 0
            while i < len(tokens):
                splits = max(len(tokens) / config.number_of_partitions, 1)
                yield tokens[i:i + splits]
                i += splits
                config.number_of_partitions = self._choose_number_of_partitions()

    def _choose_number_of_partitions(self):
        if self._best_granularity is None:
            self._update_partitions_time()
            # basic granularities will be tested at the start of the application
            if self._initial_send > 0:
                config.number_of_partitions = self._basic_partitions[len(self._basic_partitions) - self._initial_send]
                self._partitions_time[config.number_of_partitions] = []
                self._initial_send -= 1
            else:
                while [] in self._partitions_time.values():
                    # Maybe too much time waiting here, look for an asynchronous way so that the main can continue
                    time.sleep(1)
                    self._update_partitions_time()

                best_granularity = self._best_time_per_token()
                config.number_of_partitions = best_granularity

        return config.number_of_partitions

    def _best_time_per_token(self):
        """
        The time is not a good measure, because the smaller tasks will be the shortest.
        We should do a time / tokens proportion
        """
        times_per_token = dict()
        for number_of_partitions, partition_times in self._partitions_time.items():
            if len(self._father._build_args.tokens) < config.min_number_of_tokens:
                tkns_per_partition = config.min_number_of_tokens / number_of_partitions
            else:
                tkns_per_partition = max(len(self._father._build_args.tokens) / config.number_of_partitions, 1)

            partition_time = sum(partition_times) / float(len(partition_times))
            try:
                times_per_token[number_of_partitions] = partition_time / tkns_per_partition
            except ZeroDivisionError:
                pass
            # print("Number of partitions: %s, time: %s, tkns per partitions: %s, time per token: %s" % (number_of_partitions, partition_time, tkns_per_partition, times_per_token[number_of_partitions]))

        sorted_times = sorted(times_per_token.items(), key=lambda item: item[1])
        # print(sorted_times)
        # print("Tiempo de la mejor granularidad %s particiones: %s s" % (sorted_times[0][0], sum(self._partitions_time[sorted_times[0][0]])/float(len(self._partitions_time[sorted_times[0][0]]))))

        best = None
        for parts, _ in sorted_times:
            # task time must be higher than 2 seconds
            if sum(self._partitions_time[parts]) / float(len(self._partitions_time[parts])) >= 2.0:
                best = parts
                break

        if best is None:
            # if not any task have a time of more than 2 seconds, best granularity is the minimum number of partitions
            best = min([parts for parts, _ in sorted_times])

        return best

    def _update_partitions_time(self):
        partitions_times = config.session.execute(
            """SELECT number_of_partitions, start_time, end_time
               FROM hecuba.partitioning WHERE table_name = '{}.{}' ALLOW FILTERING""".format(self._father._ksp,
                                                                                             self._father._table))
        for partitions, start, end in partitions_times:
            if start is not None and end is not None:
                self._partitions_time[partitions].append(end - start)
