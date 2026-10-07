-- Dispatch wants to see which trucks are currently inside the downtown Chicago box.

ALTER TABLE gold_truck_position
    ADD COLUMN in_geofence BOOLEAN COMMENT 'Latest ping is inside the downtown Chicago geofence';

UPDATE gold_truck_position
SET in_geofence = latitude BETWEEN 41.80 AND 41.95 AND longitude BETWEEN -87.75 AND -87.55
WHERE in_geofence IS NULL;
