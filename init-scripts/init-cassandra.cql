CREATE KEYSPACE IF NOT EXISTS wikipedia_analytics
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wikipedia_analytics.bot_activity_metrics (
    window_start                TIMESTAMP,
    window_end                  TIMESTAMP,
    bot_count                   BIGINT,
    human_count                 BIGINT,
    bot_activity_percentage     TEXT,
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS wikipedia_analytics.language_activity (
    window_start                TIMESTAMP,
    domain                      TEXT,
    new_page_count              BIGINT,
    unique_authors              BIGINT,
    average_title_length        TEXT,
    PRIMARY KEY (window_start, domain)
);