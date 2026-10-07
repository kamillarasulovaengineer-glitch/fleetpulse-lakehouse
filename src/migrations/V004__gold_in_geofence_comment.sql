-- The geofence is the geofence_box bundle variable now, not a fixed downtown Chicago box (V002's comment).

ALTER TABLE gold_truck_position
    ALTER COLUMN in_geofence COMMENT 'Latest ping is inside the box set by the geofence_box bundle variable';
