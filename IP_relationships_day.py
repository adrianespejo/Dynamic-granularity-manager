from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
import time

from hecuba import config
from data_model_v2 import *

PUBLIC_SPACE_THRESHOLD = 10

KNOWN_AGGR_BANK_TOOLS_IP = set()

'''
Formats
FK_NUMPERSO: user's id (number)
PK_ANYOMESDIA: connection date YYYYMMDD
IP_TERMINAL: ip (e.g. 10.0.0.1)
FK_COD_OPERACION: type of operation (e.g. login, etc etc).
'''


def csv_to_cassandra(records):
    with open("data.csv", "r") as f:
        for line in f:
            person, date, ip, code = line.split("\t")
            records[int(person), date] = [ip, code]


@task()
def chunk_aggr(partition, ip_connections, date_users):
    # A map of maps with format
    #  { 'june': { 'userA': {('127.0.0.1','15th'}]}}

    for ((FK_NUMPERSO, PK_ANYOMESDIA), (IP_TERMINAL, _)) in partition.iteritems():
        if IP_TERMINAL not in KNOWN_AGGR_BANK_TOOLS_IP:

            try:
                date_users[PK_ANYOMESDIA, IP_TERMINAL].add(FK_NUMPERSO)
            except KeyError:
                date_users[PK_ANYOMESDIA, IP_TERMINAL] = {FK_NUMPERSO}

            try:
                ip_connections[IP_TERMINAL].add(FK_NUMPERSO)
            except KeyError:
                ip_connections[IP_TERMINAL] = {FK_NUMPERSO}


@task(returns=set)
def get_blacklist(ip_connections):
    blacklist = set(
        map(lambda a: a[0], filter(lambda a: len(a[1]) > PUBLIC_SPACE_THRESHOLD, ip_connections.iteritems())))
    # print("We found %d public IPs" % (len(blacklist)))
    return blacklist


def merge_blacklist(*blacklist):
    result = set()
    for bl in blacklist:
        result.update(bl)
    return result


@task()
def compute_months(partition, blacklist, relationships):
    for (month, IP), users in partition.iteritems():
        # first check if IP in blacklist
        if IP not in blacklist:
            users = list(users)
            for a in range(len(users)):
                for b in range(a + 1, len(users)):
                    user_a = users[a]
                    user_b = users[b]
                    try:
                        relationships[user_a].add(user_b)
                    except KeyError:
                        relationships[user_a] = {user_b}
                    try:
                        relationships[user_b].add(user_a)
                    except KeyError:
                        relationships[user_b] = {user_a}


def main():
    # ############################# INITIALIZATION ########################### #
    config.session.execute("DROP TABLE IF EXISTS test.example_dataset")
    config.session.execute("DROP TABLE IF EXISTS test.DateUsers")
    config.session.execute("DROP TABLE IF EXISTS test.IPConnections")
    config.session.execute("DROP TABLE IF EXISTS test.Relationships")
    records = IPlogs("test.example_dataset")
    ip_connections = IPConnections("test.IPConnections")
    date_users = DateUsers("test.DateUsers")
    relationships = Relationships("test.Relationships")
    csv_to_cassandra(records)

    start = time.time()
    for partition in records.split():
        chunk_aggr(partition, ip_connections, date_users)
    compss_barrier()
    time1 = time.time()

    # ################## BLACKLIST AND MONTHS CHECK ########################## #
    blacklist = map(get_blacklist, ip_connections.split())
    blacklist = compss_wait_on(blacklist)
    time2 = time.time()
    blacklist = merge_blacklist(*blacklist)

    # ########################## FIND RELATIONSHIPS ########################## #
    time3 = time.time()
    for partition in date_users.split():
        compute_months(partition, blacklist, relationships)
    compss_barrier()

    end = time.time()
    # ######################################################################## #

    n = 0
    for user, rels in relationships.iteritems():
        n += len(rels)
    print("Number of relationships: %s" % (n / 2))

    chunk_aggr_time = time1 - start
    blacklist_comp_time = time2 - time1
    ips_comp_time = end - time3

    print("IP relationships in the same day:\n")
    print("    chunk_aggr time:  %s seconds.\n" % round(chunk_aggr_time, 4))
    print("    get_blacklist time:  %s seconds.\n" % round(blacklist_comp_time, 4))
    print("    compute_IPs time:  %s seconds.\n" % round(ips_comp_time, 4))

    with open("results.txt", "a") as results:
        results.write("IP relationships in overlapping dates:\n")
        results.write("    chunk_aggr time:  %s seconds.\n" % round(chunk_aggr_time, 4))
        results.write("    get_blacklist time:  %s seconds.\n" % round(blacklist_comp_time, 4))
        results.write("    compute_IPs time:  %s seconds.\n\n" % round(ips_comp_time, 4))


if __name__ == '__main__':
    main()
