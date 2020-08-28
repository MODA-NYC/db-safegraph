SET myvars.date TO :'DATE';

DO $$
DECLARE
    computed boolean;
    _year_week text;
    query text;
BEGIN
    SELECT to_char(current_setting('myvars.date'), 'IYYY-IW') NOT IN (SELECT DISTINCT year_week FROM sg_trips_by_county) INTO computed;
    SELECT to_char(current_setting('myvars.date'), 'IYYY-IW') INTO _year_week;

    IF computed
    THEN
        SELECT FORMAT(
            $inner$
            INSERT INTO sg_trips_by_county
            SELECT 
            '%s' as year_week,
            origin::text,
            destination::text,
            sum(counts) as trips
            FROM (
            SELECT         
                LEFT(origin_census_block_group, 5) as origin,
                LEFT(desti.key, 5) as destination,
                desti.value::int as counts
            FROM social_distancing."%s",
                json_each_text(destination_cbgs) as desti
            WHERE LEFT(origin_census_block_group, 2) in (
                '36', '34', '09', '42', '25', '44' 
            )
            AND LEFT(desti.key, 2) in (
                '36', '34', '09', '42', '25', '44' 
            )) a
            GROUP BY origin, destination;
        $inner$, current_setting('myvars.date'), current_setting('myvars.date'))
        INTO query;
        EXECUTE query;
    ELSE
        RAISE NOTICE '% is already loaded !', _year_week;
    END IF;
END $$