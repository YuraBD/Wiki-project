
CREATE KEYSPACE IF NOT EXISTS wikimedia 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikimedia;

CREATE TABLE IF NOT EXISTS domain_page_create_stats (
    time_start timestamp,
    time_end timestamp,
    domain text,
    page_count int,
    PRIMARY KEY ((time_start, time_end), domain)
);

CREATE TABLE IF NOT EXISTS bot_page_create_stats (
    time_start timestamp,
    time_end timestamp,
    domain text,
    bot_page_count int,
    PRIMARY KEY ((time_start, time_end), domain)
);

CREATE TABLE IF NOT EXISTS user_page_create_stats (
    time_start timestamp,
    time_end timestamp,
    user_id int,
    user_name text,
    page_titles list<text>,
    page_count int,
    PRIMARY KEY ((time_start, time_end), page_count, user_id)
) WITH CLUSTERING ORDER BY (page_count DESC, user_id ASC);

CREATE TABLE IF NOT EXISTS raw_data_table (
    page_id int,
    page_title text,
    timestamp timestamp,
    domain text,
    user_is_bot boolean,
    user_text text,
    user_id int,
    PRIMARY KEY ((page_id), timestamp)
)
WITH CLUSTERING ORDER BY (timestamp ASC);
