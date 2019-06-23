CREATE KEYSPACE my_app WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE my_app.grid (
    lat double,
    lon double,
    time int,
    ilev int,
    m_cloudfract double,
    m_humidity double,
    m_icewater double,
    m_liquidwate double,
    m_ozone double,
    m_pot_vorticit double,
    m_rain double,
    m_snow double,
    PRIMARY KEY (lat, lon, time, ilev)
) WITH CLUSTERING ORDER BY (lon ASC, time ASC, ilev ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

COPY my_app.grid (lat, lon, time, ilev, m_cloudfract, m_humidity, m_icewater, m_liquidwate, m_ozone, m_pot_vorticit, m_rain, m_snow) FROM '/home/bsc31/bsc31310/earth/fulldb.csv' WITH HEADER = TRUE;
