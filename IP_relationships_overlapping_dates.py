from collections import defaultdict
from datetime import datetime
import time
from pycompss.api.task import task
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from random import randint

from hecuba import config
from data_model import *

PUBLIC_SPACE_THRESHOLD = 10

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
    
    for i in range(2000000):
        date = '20{year:02d}{month:02d}{day:02d}'.format(year=randint(0, 18), month=randint(1, 12), day=randint(1, 28))
        ip = '{}.{}.{}.{}'.format(randint(1, 255), randint(1, 255), randint(1, 255), randint(1, 255))
        records[randint(0, 10000), date] = [ip, 'login']


@task()
def chunk_aggr(partition, ip_users_date):
    # A map of maps with format
    #  { 'june': { 'userA': {('127.0.0.1','15th'}]}}

    for ((FK_NUMPERSO, PK_ANYOMESDIA), (IP_TERMINAL, _)) in partition.iteritems():

            date = PK_ANYOMESDIA

            try:
                ip_users_date[IP_TERMINAL].add((FK_NUMPERSO, date))
            except KeyError:
                ip_users_date[IP_TERMINAL] = {(FK_NUMPERSO, date)}


@task(returns=set)
def get_blacklist(ip_users_date):
    ip_connections = defaultdict(set)
    for IP, users_dates in ip_users_date.iteritems():
        for user, date in users_dates:
            ip_connections[IP].add(user)

    blacklist = set(
        map(lambda a: a[0], filter(lambda a: len(a[1]) > PUBLIC_SPACE_THRESHOLD, ip_connections.iteritems())))
    # print("We found %d public IPs" % (len(blacklist)))
    return blacklist


def merge_blacklist(*blacklist):
    result = set()
    for bl in blacklist:
        result.update(bl)
    return result


def format_date(date):
    date = date.replace("/", "")
    ret = datetime.strptime(date, "%Y%m%d")
    return ret


@task()
def compute_IPs(partition, blacklist, relationships):
    for IP, users_connections in partition.iteritems():
        if IP not in blacklist:
            minmax_dates = defaultdict(list)
            for user, date in users_connections:
                date_f = format_date(date)
                if user not in minmax_dates:
                    minmax_dates[user] = [date_f, date_f]
                elif date_f < minmax_dates[user][0]:
                    minmax_dates[user][0] = date_f
                elif date_f > minmax_dates[user][1]:
                    minmax_dates[user][1] = date_f

            users = minmax_dates.keys()
            for a in range(len(users)):
                for b in range(a + 1, len(users)):
                    user_a = users[a]
                    user_b = users[b]
                    # check if dates overlap
                    if minmax_dates[user_a][0] <= minmax_dates[user_b][1] and minmax_dates[user_a][1] >= minmax_dates[user_b][0]:
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
    config.partition_strategy = "DYNAMIC"
    config.session.execute("DROP TABLE IF EXISTS test.example_dataset")
    config.session.execute("DROP TABLE IF EXISTS test.IPUsersInDate")
    config.session.execute("DROP TABLE IF EXISTS test.Relationships")
    records = IPlogs("test.example_dataset")
    ip_users_date = IPUsersInDate("test.IPUsersInDate")
    relationships = Relationships("test.Relationships")
    csv_to_cassandra(records)

    start = time.time()
    for partition in records.split():
        chunk_aggr(partition, ip_users_date)
    compss_barrier()
    time1 = time.time()

    # ################## BLACKLIST AND MONTHS CHECK ########################## #
    blacklist = map(get_blacklist, ip_users_date.split())
    blacklist = compss_wait_on(blacklist)
    time2 = time.time()
    blacklist = merge_blacklist(*blacklist)

    # ########################## FIND RELATIONSHIPS ########################## #
    time3 = time.time()
    for partition in ip_users_date.split():
        compute_IPs(partition, blacklist, relationships)
    compss_barrier()

    end = time.time()
    # ######################################################################## #
    n = 0
    for user, rels in relationships.iteritems():
        n += len(rels)
        #print(user, rels)
    print("Number of relationships: %s" % (n / 2))

    chunk_aggr_time = time1 - start
    blacklist_comp_time = time2 - time1
    ips_comp_time = end - time3

    print("IP relationships in overlapping dates:\n")
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
