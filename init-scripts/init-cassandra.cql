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
    trend                       BIGINT,
    PRIMARY KEY (window_start, domain)
);

CREATE TABLE IF NOT EXISTS wikipedia_analytics.page_events (
    user_name       TEXT,
    page_id         TEXT,
    user_is_bot     BOOLEAN,
    dt              TIMESTAMP,
    domain          TEXT,
    page_title      TEXT,
    PRIMARY KEY (user_name, dt)
) WITH CLUSTERING ORDER BY (dt DESC);

CREATE TABLE IF NOT EXISTS wikipedia_analytics.hourly_activity (
    hour             TIMESTAMP,
    domain           TEXT,
    pages_created    BIGINT,
    unique_authors   BIGINT,
    bot_human_ratio  DOUBLE,
    top_authors      LIST<FROZEN<top_author>>,
    PRIMARY KEY (hour, domain)
)

CREATE TYPE IF NOT EXISTS wikipedia_analytics.top_author (
    name    TEXT,
    pages   BIGINT,
    is_bot  BOOLEAN
);

CREATE TABLE IF NOT EXISTS wikipedia_analytics.editor_behavior_patterns (
    user_name        TEXT,
    total_pages      BIGINT,
    avg_gap_seconds  DOUBLE,
    unique_domains   BIGINT,
    activity_hours   SET<INT>,
    specialization   TEXT,
    PRIMARY KEY (user_name)
);

CREATE TABLE IF NOT EXISTS wikipedia_analytics.pages_by_id (
    page_id     TEXT,
    page_title  TEXT,
    domain      TEXT,
    user_name   TEXT,
    user_is_bot BOOLEAN,
    dt          TIMESTAMP,
    PRIMARY KEY (page_id)
)

CREATE TABLE IF NOT EXISTS wikipedia_analytics.pages_by_domain (
    domain      TEXT,
    dt          TIMESTAMP,
    page_id     TEXT,
    page_title  TEXT,
    user_name   TEXT,
    user_is_bot BOOLEAN,
    PRIMARY KEY (domain, dt, page_id)
) WITH CLUSTERING ORDER BY (dt DESC, page_id ASC)
