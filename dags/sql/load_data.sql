--Load Event Table
INSERT INTO prod.dim_event
			(event_id, name, description, rsvp_limit, fee_amount, fee_required, link)
SELECT e.id, e.name, e.description, e.rsvp_limit, e.fee_amount, e.fee_required, e.link
FROM staging.dim_event e;
--Load Group Table
INSERT INTO prod.dim_group
			(group_id, name, description, link)
SELECT g.id, g.name, g.description, g.link
FROM staging.dim_group g;
--Load Topic Table
INSERT INTO prod.dim_topic
			(topic_id, name)
SELECT t.id, t.name
FROM staging.dim_topic t;
--Load Venue Table
INSERT INTO prod.dim_venue
			(venue_id, name, lat, lon, city, state, zip, country)
SELECT v.id, v.name, v.lat, v.lon, v.city, v.state, v.zip, v.country
FROM staging.dim_venue v;
-- Load Bridge Table FULL
TRUNCATE prod.topic_group_bridge;
INSERT INTO prod.topic_group_bridge
			(topic_key, group_key)
SELECT t.key, g.key
FROM prod.dim_topic t
JOIN staging.topic_group_bridge tgb ON tgb.topic_id=t.topic_id
JOIN prod.dim_group g ON g.group_id=tgb.group_id;
--Fact Table
INSERT INTO prod.fact_event
			(event_key, group_key, venue_key, date_key, event_datetime, attendance_count, manual_attendance_count, yes_rsvp_count, comment_count, waitlist_count)
SELECT e.key, g.key, v.key, d.key, fe.event_datetime, fe.attendance_count, fe.manual_attendance_count, fe.yes_rsvp_count, fe.comment_count, fe.waitlist_count
FROM staging.fact_event fe
JOIN prod.dim_event e ON e.event_id=fe.event_id
JOIN prod.dim_group g ON g.group_id=fe.group_id
LEFT JOIN prod.dim_venue v ON v.venue_id=fe.venue_id
JOIN prod.dim_date d ON d.key=(10000 * EXTRACT(YEAR FROM fe.event_datetime) + 100 * EXTRACT(MONTH FROM fe.event_datetime) + EXTRACT(DAY FROM fe.event_datetime));