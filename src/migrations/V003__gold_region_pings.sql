-- Second gold output from the brief: ping volume per region in tumbling windows of event time.
-- The window size is a bundle variable (region_window_minutes), so it is part of the key.

CREATE TABLE IF NOT EXISTS gold_region_pings (
    window_minutes  INT       NOT NULL,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    region          STRING    NOT NULL,
    pings           BIGINT    NOT NULL,
    trucks          BIGINT    NOT NULL,
    updated_at      TIMESTAMP
)
COMMENT 'Pings and distinct trucks per region per tumbling window of event_ts. One row per (window_minutes, window_start, region).';
