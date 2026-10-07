-- Initial layout for one environment's schema.
-- Bronze keeps the payload exactly as received (strings), types are applied in silver.

CREATE TABLE IF NOT EXISTS truck_details (
    truck_id      STRING    NOT NULL,
    make          STRING,
    model         STRING,
    capacity_lbs  INT,
    home_depot    STRING,
    region        STRING,
    driver        STRING,
    updated_at    TIMESTAMP
)
COMMENT 'Reference data per truck: vehicle, home depot, region and assigned driver.';

CREATE TABLE IF NOT EXISTS bronze_pings (
    truck_id       STRING,
    latitude       STRING,
    longitude      STRING,
    event_ts       STRING,
    _rescued_data  STRING,
    _source_file   STRING,
    _ingested_at   TIMESTAMP
)
COMMENT 'GPS pings as landed. Append-only.';

CREATE TABLE IF NOT EXISTS silver_pings (
    truck_id       STRING    NOT NULL,
    event_ts       TIMESTAMP NOT NULL,
    latitude       DOUBLE    NOT NULL,
    longitude      DOUBLE    NOT NULL,
    _source_file   STRING,
    _ingested_at   TIMESTAMP,
    _processed_at  TIMESTAMP
)
COMMENT 'Typed, validated and de-duplicated pings. One row per (truck_id, event_ts).';

CREATE TABLE IF NOT EXISTS silver_pings_quarantine (
    truck_id         STRING,
    latitude         STRING,
    longitude        STRING,
    event_ts         STRING,
    _rescued_data    STRING,
    _source_file     STRING,
    _ingested_at     TIMESTAMP,
    reason           STRING,
    _quarantined_at  TIMESTAMP
)
COMMENT 'Pings rejected by silver validation, kept as received along with the reason.';

CREATE TABLE IF NOT EXISTS gold_truck_position (
    truck_id      STRING    NOT NULL,
    event_ts      TIMESTAMP,
    latitude      DOUBLE,
    longitude     DOUBLE,
    make          STRING,
    model         STRING,
    capacity_lbs  INT,
    home_depot    STRING,
    region        STRING,
    driver        STRING,
    updated_at    TIMESTAMP
)
COMMENT 'Latest known position of every truck, enriched with truck_details.';
