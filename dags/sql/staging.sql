-- DROP Staging tables
DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;
--Event Dim
SELECT e.id,
          e.name,
          e.description,
          e.rsvp_limit,
          e.fee_amount,
          e.fee_required,
          e.link
INTO staging.dim_event
FROM source.event e
WHERE e.upload_timestamp >= '{}';
-- Group Dim
SELECT g.id,
       g.name,
       g.description,
       g.link
INTO staging.dim_group
FROM source.group g
WHERE g.upload_timestamp >= '{}';
-- Venue Dim
SELECT v.id,
       v.name,
       v.lat,
       v.lon,
       v.city,
       v.state,
       v.zip,
       v.country
INTO staging.dim_venue
FROM source.venue v
WHERE v.upload_timestamp >= '{}';
-- Topic Dim
SELECT t.id,
       t.name
INTO staging.dim_topic 
FROM source.topic t
WHERE t.upload_timestamp >= '{}';
-- Topic Group Bridge
SELECT topic_id, group_id
INTO staging.topic_group_bridge
FROM source.topic__group;
-- Fact Table
SELECT e.id AS "event_id",
       e.local_datetime AS "event_datetime",
       g.id AS "group_id",
       v.id AS "venue_id",
       e.attendance_count,
       e.yes_rsvp_count,
       e.manual_attendance_count,
       e.comment_count,
       e.waitlist_count
INTO staging.fact_event
FROM source.event e
JOIN source.group g ON g.id=e.group_id
LEFT JOIN source.venue__event ve ON ve.event_id=e.id
LEFT JOIN source.venue v ON v.id=ve.venue_id
WHERE e.upload_timestamp >= '{}';