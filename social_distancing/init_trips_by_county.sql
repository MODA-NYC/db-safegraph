CREATE TABLE IF NOT EXISTS sg_trips_by_county (
    year_week text,
    origin text,
    destination text,
    trips int
);

CREATE TABLE IF NOT EXISTS county_days_included (
    year_week text,
    date text
);