-- ClickHouse GPS Ingestion Schema for Matatu Pulse
-- Optimized for 100k+ vehicles, 20k+ events/sec
-- Designed for high-throughput time-series data with efficient queries

-- ============================================
-- Main GPS Events Table (Replacing TimescaleDB)
-- ============================================

CREATE TABLE IF NOT EXISTS gps_events
(
    -- Event metadata
    event_id UUID DEFAULT generateUUIDv4(),
    trace_id String DEFAULT '',
    
    -- Vehicle and organization identifiers
    vehicle_id UUID,
    organization_id UUID,
    
    -- GPS coordinates and movement data
    latitude Float64,
    longitude Float64,
    altitude Int16,
    speed Int16,           -- km/h
    heading Int16,         -- degrees 0-359
    hdop Float32,          -- Horizontal dilution of precision
    
    -- GPS fix information
    satellites Int8,
    fix_status Enum8('NO_FIX' = 0, '2D' = 2, '3D' = 3),
    
    -- Sensor data
    rain Bool DEFAULT false,
    temperature Nullable(Int8),    -- Celsius
    battery_level Nullable(Int8),  -- Percentage
    fuel_level Nullable(Int8),     -- Percentage
    
    -- Event classification
    event_type Enum8(
        'NORMAL' = 0,
        'GEOFENCE_ENTER' = 1,
        'GEOFENCE_EXIT' = 2,
        'OVERSPEED' = 3,
        'IGNITION_ON' = 4,
        'IGNITION_OFF' = 5,
        'PANIC_BUTTON' = 6,
        'GPS_SIGNAL_LOST' = 7,
        'HARSH_BRAKING' = 8,
        'HARSH_ACCELERATION' = 9
    ) DEFAULT 'NORMAL',
    
    -- Movement filter flags
    movement_filtered Bool DEFAULT false,
    distance_from_last Float32 DEFAULT 0.0,  -- meters
    time_since_last Int32 DEFAULT 0,         -- milliseconds
    
    -- Enriched metadata
    vehicle_plate String DEFAULT '',
    route_id UUID,
    driver_id Nullable(UUID),
    conductor_id Nullable(UUID),
    capacity Int8 DEFAULT 0,
    
    -- Raw message data (for debugging/replay)
    raw_message String DEFAULT '',
    schema_version Int8 DEFAULT 1,
    
    -- Timestamps
    device_timestamp DateTime64(3, 'UTC'),  -- From GPS device
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),  -- When received by MQTT Consumer
    processed_at DateTime64(3, 'UTC') DEFAULT now64(3),  -- When processed by Batch Writer
    recorded_at DateTime64(3, 'UTC') DEFAULT now64(3)   -- When written to ClickHouse
)
ENGINE = ReplicatedReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (organization_id, vehicle_id, recorded_at, event_id)
PRIMARY KEY (organization_id, vehicle_id, recorded_at)
TTL recorded_at + INTERVAL 1 YEAR TO VOLUME 'cold'
SETTINGS
    index_granularity = 8192,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 1073741824;  -- 1GB

-- ============================================
-- Latest Vehicle State Table (Replacing Redis Hash)
-- ============================================

CREATE TABLE IF NOT EXISTS vehicle_latest_state
(
    vehicle_id UUID,
    organization_id UUID,
    
    -- Current position
    latitude Float64,
    longitude Float64,
    altitude Int16,
    speed Int16,
    heading Int16,
    
    -- GPS status
    satellites Int8,
    fix_status Enum8('NO_FIX' = 0, '2D' = 2, '3D' = 3),
    hdop Float32,
    
    -- Vehicle metadata
    vehicle_plate String,
    route_id UUID,
    driver_id Nullable(UUID),
    conductor_id Nullable(UUID),
    capacity Int8,
    status Enum8('ACTIVE' = 0, 'OFFLINE' = 1, 'PARKED' = 2, 'MAINTENANCE' = 3),
    
    -- Sensor readings
    rain Bool DEFAULT false,
    temperature Nullable(Int8),
    battery_level Nullable(Int8),
    fuel_level Nullable(Int8),
    
    -- Timestamps
    last_update DateTime64(3, 'UTC'),
    last_event_type Enum8(
        'NORMAL' = 0,
        'GEOFENCE_ENTER' = 1,
        'GEOFENCE_EXIT' = 2,
        'OVERSPEED' = 3,
        'IGNITION_ON' = 4,
        'IGNITION_OFF' = 5,
        'PANIC_BUTTON' = 6,
        'GPS_SIGNAL_LOST' = 7,
        'HARSH_BRAKING' = 8,
        'HARSH_ACCELERATION' = 9
    ) DEFAULT 'NORMAL',
    
    -- Statistics
    total_distance_today Float64 DEFAULT 0.0,  -- meters
    total_trip_time_today Int32 DEFAULT 0,      -- seconds
    max_speed_today Int16 DEFAULT 0,
    
    -- Index for fast lookups
    INDEX idx_org_vehicle (organization_id, vehicle_id) TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree(last_update)
ORDER BY (organization_id, vehicle_id)
PRIMARY KEY (organization_id, vehicle_id)
TTL last_update + INTERVAL 1 HOUR DELETE  -- Auto-cleanup stale vehicles
SETTINGS
    index_granularity = 8192;

-- ============================================
-- Materialized Views for Aggregated Analytics
-- ============================================

-- 1-minute aggregates (replacing TimescaleDB continuous aggregates)
CREATE MATERIALIZED VIEW gps_events_1min
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (organization_id, vehicle_id, bucket)
POPULATE
AS SELECT
    toStartOfMinute(recorded_at) AS bucket,
    organization_id,
    vehicle_id,
    avgState(latitude) AS avg_lat,
    avgState(longitude) AS avg_lng,
    avgState(speed) AS avg_speed,
    maxState(speed) AS max_speed,
    countState() AS sample_count,
    anyLastState(latitude) AS last_lat,
    anyLastState(longitude) AS last_lng,
    anyLastState(speed) AS last_speed,
    anyLastState(heading) AS last_heading,
    sumState(if(event_type != 'NORMAL', 1, 0)) AS event_count
FROM gps_events
GROUP BY
    organization_id,
    vehicle_id,
    bucket;

-- 5-minute aggregates for trip analysis
CREATE MATERIALIZED VIEW gps_events_5min
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (organization_id, vehicle_id, bucket)
POPULATE
AS SELECT
    toStartOfFiveMinute(recorded_at) AS bucket,
    organization_id,
    vehicle_id,
    avgState(latitude) AS avg_lat,
    avgState(longitude) AS avg_lng,
    avgState(speed) AS avg_speed,
    maxState(speed) AS max_speed,
    minState(speed) AS min_speed,
    countState() AS sample_count,
    sumState(if(speed > 0, 1, 0)) AS moving_samples,
    anyLastState(latitude) AS last_lat,
    anyLastState(longitude) AS last_lng
FROM gps_events
GROUP BY
    organization_id,
    vehicle_id,
    bucket;

-- Hourly aggregates for daily reports
CREATE MATERIALIZED VIEW gps_events_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (organization_id, vehicle_id, bucket)
POPULATE
AS SELECT
    toStartOfHour(recorded_at) AS bucket,
    organization_id,
    vehicle_id,
    avgState(latitude) AS avg_lat,
    avgState(longitude) AS avg_lng,
    avgState(speed) AS avg_speed,
    maxState(speed) AS max_speed,
    countState() AS sample_count,
    sumState(if(speed > 0, 1, 0)) AS moving_samples,
    sumState(if(event_type = 'OVERSPEED', 1, 0)) AS overspeed_events,
    sumState(if(event_type IN ('HARSH_BRAKING', 'HARSH_ACCELERATION'), 1, 0)) AS harsh_events
FROM gps_events
GROUP BY
    organization_id,
    vehicle_id,
    bucket;

-- ============================================
-- Geofence Events Table
-- ============================================

CREATE TABLE IF NOT EXISTS geofence_events
(
    event_id UUID DEFAULT generateUUIDv4(),
    organization_id UUID,
    vehicle_id UUID,
    geofence_id UUID,
    geofence_name String,
    event_type Enum8('ENTER' = 1, 'EXIT' = 2),
    latitude Float64,
    longitude Float64,
    speed Int16,
    recorded_at DateTime64(3, 'UTC') DEFAULT now64(3),
    
    INDEX idx_org_vehicle (organization_id, vehicle_id) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_geofence (geofence_id) TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (organization_id, vehicle_id, recorded_at, geofence_id)
TTL recorded_at + INTERVAL 90 DAYS;

-- ============================================
-- Trip Summary Table
-- ============================================

CREATE TABLE IF NOT EXISTS trip_summaries
(
    trip_id UUID DEFAULT generateUUIDv4(),
    organization_id UUID,
    vehicle_id UUID,
    driver_id Nullable(UUID),
    
    -- Trip timing
    start_time DateTime64(3, 'UTC'),
    end_time DateTime64(3, 'UTC'),
    duration_seconds Int32,
    
    -- Distance and speed
    total_distance_meters Float64,
    avg_speed_kmh Float32,
    max_speed_kmh Int16,
    
    -- Start/end locations
    start_latitude Float64,
    start_longitude Float64,
    end_latitude Float64,
    end_longitude Float64,
    
    -- Event counts
    overspeed_count Int32 DEFAULT 0,
    harsh_braking_count Int32 DEFAULT 0,
    harsh_acceleration_count Int32 DEFAULT 0,
    geofence_entries_count Int32 DEFAULT 0,
    
    -- Fuel consumption (if available)
    fuel_start Nullable(Int8),
    fuel_end Nullable(Int8),
    fuel_consumed Nullable(Float32),
    
    -- Metadata
    route_id UUID,
    trip_status Enum8('COMPLETED' = 0, 'IN_PROGRESS' = 1, 'CANCELLED' = 2),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    
    INDEX idx_org_vehicle_time (organization_id, vehicle_id, start_time) TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(start_time)
ORDER BY (organization_id, vehicle_id, start_time, trip_id)
TTL start_time + INTERVAL 2 YEARS;

-- ============================================
-- Dead Letter Queue (DLQ) Table
-- ============================================

CREATE TABLE IF NOT EXISTS dlq_events
(
    dlq_id UUID DEFAULT generateUUIDv4(),
    original_event String,
    error_message String,
    error_stack String,
    component String,  -- 'mqtt_consumer', 'batch_writer', 'websocket_gateway'
    organization_id UUID,
    vehicle_id UUID,
    attempts Int8 DEFAULT 1,
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    last_retry_at DateTime64(3, 'UTC'),
    status Enum8('PENDING' = 0, 'RETRIED' = 1, 'RESOLVED' = 2, 'DISCARDED' = 3) DEFAULT 'PENDING',
    
    INDEX idx_status_org (status, organization_id) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_failed_at (failed_at) TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(failed_at)
ORDER BY (organization_id, failed_at, dlq_id)
TTL failed_at + INTERVAL 7 DAYS DELETE
SETTINGS
    index_granularity = 8192;

-- ============================================
-- Performance Metrics Table
-- ============================================

CREATE TABLE IF NOT EXISTS system_metrics
(
    metric_id UUID DEFAULT generateUUIDv4(),
    metric_name String,
    component String,
    organization_id UUID,
    value Float64,
    labels Map(String, String),
    recorded_at DateTime64(3, 'UTC') DEFAULT now64(3),
    
    INDEX idx_metric_component (metric_name, component) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_time (recorded_at) TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (metric_name, component, recorded_at, metric_id)
TTL recorded_at + INTERVAL 30 DAYS;

-- ============================================
-- Views for Common Queries
-- ============================================

-- View for active vehicles in last 5 minutes
CREATE VIEW active_vehicles_last_5min AS
SELECT
    organization_id,
    vehicle_id,
    anyLast(vehicle_plate) as plate,
    anyLast(latitude) as last_lat,
    anyLast(longitude) as last_lng,
    anyLast(speed) as last_speed,
    anyLast(heading) as last_heading,
    max(recorded_at) as last_seen
FROM gps_events
WHERE recorded_at >= now() - INTERVAL 5 MINUTE
GROUP BY organization_id, vehicle_id
HAVING max(recorded_at) >= now() - INTERVAL 2 MINUTE;

-- View for organization statistics
CREATE VIEW organization_stats_daily AS
SELECT
    organization_id,
    toDate(recorded_at) as date,
    countDistinct(vehicle_id) as active_vehicles,
    count() as total_events,
    countIf(event_type != 'NORMAL') as alert_events,
    avg(speed) as avg_speed,
    max(speed) as max_speed,
    countIf(speed > 80) as overspeed_count
FROM gps_events
WHERE recorded_at >= now() - INTERVAL 1 DAY
GROUP BY organization_id, date;

-- ============================================
-- User and Access Management
-- ============================================

-- Users table (read from Supabase, replicated to ClickHouse for analytics)
CREATE TABLE IF NOT EXISTS users
(
    user_id UUID,
    organization_id UUID,
    email String,
    role Enum8('ADMIN' = 0, 'MANAGER' = 1, 'VIEWER' = 2),
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    
    INDEX idx_org_user (organization_id, user_id) TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree(updated_at)
ORDER BY (organization_id, user_id);

-- ============================================
-- Settings and Configuration
-- ============================================

-- Settings for ClickHouse optimization
SET allow_experimental_lightweight_delete = 1;
SET allow_experimental_annoy_index = 1;
SET merge_tree_min_rows_for_concurrent_read = 8192;
SET merge_tree_min_bytes_for_concurrent_read = 262144;

-- Create users for different access levels
CREATE USER IF NOT EXISTS batch_writer IDENTIFIED WITH sha256_password BY 'secure_password_here';
CREATE USER IF NOT EXISTS analytics IDENTIFIED WITH sha256_password BY 'secure_password_here';
CREATE USER IF NOT EXISTS api_server IDENTIFIED WITH sha256_password BY 'secure_password_here';

-- Grant permissions
GRANT INSERT ON gps_events TO batch_writer;
GRANT INSERT ON vehicle_latest_state TO batch_writer;
GRANT INSERT ON dlq_events TO batch_writer;

GRANT SELECT ON gps_events TO analytics;
GRANT SELECT ON gps_events_1min TO analytics;
GRANT SELECT ON gps_events_5min TO analytics;
GRANT SELECT ON gps_events_hourly TO analytics;
GRANT SELECT ON trip_summaries TO analytics;
GRANT SELECT ON organization_stats_daily TO analytics;

GRANT SELECT ON gps_events TO api_server;
GRANT SELECT ON vehicle_latest_state TO api_server;
GRANT SELECT ON active_vehicles_last_5min TO api_server;
GRANT SELECT ON organization_stats_daily TO api_server;

-- ============================================
-- Comments and Documentation
-- ============================================

/*
SCHEMA DESIGN NOTES:

1. PARTITIONING STRATEGY:
   - Monthly partitions for gps_events (toYYYYMM)
   - Enables efficient data retention policies
   - Aligns with typical reporting periods

2. ORDER BY KEYS:
   - Primary: (organization_id, vehicle_id, recorded_at)
   - Optimizes queries filtering by org/vehicle with time ranges
   - Supports efficient range queries for trip playback

3. ENGINE SELECTION:
   - ReplicatedReplacingMergeTree: For tables that need deduplication
   - ReplicatedMergeTree: For append-only tables
   - AggregatingMergeTree: For materialized views with aggregates

4. TTL POLICIES:
   - gps_events: 1 year to cold storage, then delete
   - vehicle_latest_state: 1 hour auto-cleanup (Redis replacement)
   - dlq_events: 7 days retention
   - trip_summaries: 2 years retention

5. INDEXING:
   - Bloom filters for high-cardinality columns
   - Minmax indexes for timestamp ranges
   - Granularity optimized for 100k+ vehicles

6. DATA TYPES:
   - UUID for identifiers (matches PostgreSQL)
   - DateTime64(3) for millisecond precision
   - Int16 for speed/heading (sufficient range)
   - Float64 for coordinates (high precision)
   - Enum8 for fixed value sets (efficient storage)

7. SCALING FOR 100K+ VEHICLES:
   - ~20,000 events/sec at 5s intervals
   - ~1.7B events/month
   - ~200GB/month raw storage
   - Compression reduces to ~40GB/month
   - Monthly partitions keep partitions manageable

8. QUERY PATTERNS OPTIMIZED:
   - Latest vehicle positions (vehicle_latest_state)
   - Trip history with time ranges
   - Organization-level aggregates
   - Real-time dashboards
   - Geofence event queries
   - Alert/event filtering
*/
