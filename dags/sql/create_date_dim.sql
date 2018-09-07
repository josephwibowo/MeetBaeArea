CREATE TABLE IF NOT EXISTS prod.dim_date (
	key integer PRIMARY KEY,
	date timestamp NOT NULL,
	date_us_format text NOT NULL, 		-- mm/dd/yyyy
	date_us_short_format text NOT NULL, -- m/d/yyyy
	date_iso_format text NOT NULL, 		-- yyyy-mm-dd
	num_year integer NOT NULL,
	num_month_in_year integer NOT NULL,
	num_week_in_year integer NOT NULL,
	num_week_in_month integer NOT NULL,
	num_day_in_year integer NOT NULL,
	num_day_in_month integer NOT NULL,
	num_day_in_week integer NOT NULL,
	is_holiday_us boolean NOT NULL,
	-- Names
	name_month_en text NOT NULL,
	name_month_abbreviated_en text NOT NULL,
	name_day_en text NOT NULL,
	name_day_abbreviated_en text NOT NULL
);

CREATE OR REPLACE FUNCTION get_date_primary_key(ts timestamp) RETURNS integer AS $$
BEGIN
	RETURN 10000 * EXTRACT(YEAR FROM ts) +
		100 * EXTRACT(MONTH FROM ts) +
		EXTRACT(DAY FROM ts);
END;
$$ LANGUAGE plpgsql;

WITH date1 AS (
	SELECT generate_series('2008-01-01'::timestamp, '2021-12-31'::timestamp, '1 day') AS ts
), date2 AS (
	SELECT get_date_primary_key(ts) AS key,
		ts AS date,
		to_char(ts, 'MM/DD/YYYY') AS date_us_format, 
		EXTRACT(MONTH FROM ts) || '/' || EXTRACT(DAY FROM ts) || '/' || EXTRACT(YEAR FROM ts) AS date_us_short_format,
		to_char(ts, 'YYYY-MM-DD') AS date_iso_format,
		EXTRACT(YEAR FROM ts) AS num_year,
		EXTRACT(MONTH FROM ts) AS num_month_in_year,
		EXTRACT(WEEK FROM ts) AS num_week_in_year,
		EXTRACT(WEEK FROM ts) - EXTRACT(WEEK FROM date(date_trunc('MONTH', ts))) + 1 AS num_week_in_month,
		EXTRACT(DOY FROM ts) AS num_day_in_year,
		EXTRACT(DAY FROM ts) AS num_day_in_month,
		EXTRACT(ISODOW FROM ts) AS num_day_in_week
	FROM date1
), date3 AS (
	SELECT *,
		num_month_in_year = 1 AND num_day_in_month = 1 OR 
		num_month_in_year = 7 AND num_day_in_month = 4 OR 
		num_month_in_year = 12 AND num_day_in_month = 25 AS is_holiday_us
	FROM date2
), date4 AS (
	SELECT
		*,
		CASE
			WHEN num_month_in_year = 1 THEN 'January'
			WHEN num_month_in_year = 2 THEN 'February'
			WHEN num_month_in_year = 3 THEN 'March'
			WHEN num_month_in_year = 4 THEN 'April'
			WHEN num_month_in_year = 5 THEN 'May'
			WHEN num_month_in_year = 6 THEN 'June'
			WHEN num_month_in_year = 7 THEN 'July'
			WHEN num_month_in_year = 8 THEN 'August'
	 		WHEN num_month_in_year = 9 THEN 'September'
			WHEN num_month_in_year = 10 THEN 'October'
			WHEN num_month_in_year = 11 THEN 'November'
			WHEN num_month_in_year = 12 THEN 'December'
		END AS name_month_en,
		CASE
			WHEN num_day_in_week = 1 THEN 'Monday'
			WHEN num_day_in_week = 2 THEN 'Tuesday'
			WHEN num_day_in_week = 3 THEN 'Wednesday'
			WHEN num_day_in_week = 4 THEN 'Thursday'
			WHEN num_day_in_week = 5 THEN 'Friday'
			WHEN num_day_in_week = 6 THEN 'Saturday'
			WHEN num_day_in_week = 7 THEN 'Sunday'
		END AS name_day_en
	FROM date3
), date5 AS (
	SELECT *, 
		substring(name_month_en from 1 for 3) AS name_month_abbreviated_en,
		substring(name_day_en from 1 for 2) AS name_day_abbreviated_en
	FROM date4
)
INSERT INTO prod.dim_date
	SELECT key, date,
		date_us_format, date_us_short_format, date_iso_format, 
		num_year, 
		num_month_in_year,
		num_week_in_year,num_week_in_month,
		num_day_in_year, num_day_in_month, num_day_in_week,
		is_holiday_us, 
		name_month_en, name_month_abbreviated_en, name_day_en, name_day_abbreviated_en
	FROM date5;