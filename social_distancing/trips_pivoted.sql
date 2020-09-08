SET myvars.date TO :'DATE';

DO $$
DECLARE
    new_date boolean;
    query text;

BEGIN
    SELECT current_setting('myvars.date')::text NOT IN (SELECT DISTINCT date FROM sg_trips_pivoted) INTO new_date;
    
    IF new_date
    THEN
        RAISE NOTICE 'Loading % to pivoted trips table', current_setting('myvars.date')::text;
        SELECT FORMAT(
            $inner$
            WITH unpivot AS (
            SELECT 
            '%s' as date,
            origin::text,
            CASE 
                WHEN destination IN (SELECT DISTINCT county FROM region) THEN destination
                ELSE 'O31CR'
            END as destination,
            sum(counts) as trips
            FROM (
            SELECT         
                LEFT(origin_census_block_group, 5) as origin,
                LEFT(desti.key, 5) as destination,
                desti.value::int as counts
            FROM social_distancing."%s",
                json_each_text(destination_cbgs) as desti
            WHERE LEFT(origin_census_block_group, 5) in (
                '36005','36061','36081','36047','36085')
            ) a
            GROUP BY origin, destination)

            INSERT INTO sg_trips_pivoted
            SELECT
                date,
                origin,
                -- NYC
                SUM(CASE WHEN destination='36005' THEN trips ELSE NULL END) as "36005",
                SUM(CASE WHEN destination='36047' THEN trips ELSE NULL END) as "36047",
                SUM(CASE WHEN destination='36061' THEN trips ELSE NULL END) as "36061",
                SUM(CASE WHEN destination='36081' THEN trips ELSE NULL END) as "36081",
                SUM(CASE WHEN destination='36085' THEN trips ELSE NULL END) as "36085",
                -- CT
                SUM(CASE WHEN destination='09001' THEN trips ELSE NULL END) as "09001",
                SUM(CASE WHEN destination='09005' THEN trips ELSE NULL END) as "09005",
                SUM(CASE WHEN destination='09009' THEN trips ELSE NULL END) as "09009",
                -- North NJ
                SUM(CASE WHEN destination='34003' THEN trips ELSE NULL END) as "34003",
                SUM(CASE WHEN destination='34013' THEN trips ELSE NULL END) as "34013",
                SUM(CASE WHEN destination='34017' THEN trips ELSE NULL END) as "34017",
                SUM(CASE WHEN destination='34019' THEN trips ELSE NULL END) as "34019",
                SUM(CASE WHEN destination='34021' THEN trips ELSE NULL END) as "34021",
                SUM(CASE WHEN destination='34023' THEN trips ELSE NULL END) as "34023",
                SUM(CASE WHEN destination='34025' THEN trips ELSE NULL END) as "34025",
                SUM(CASE WHEN destination='34027' THEN trips ELSE NULL END) as "34027",
                SUM(CASE WHEN destination='34029' THEN trips ELSE NULL END) as "34029",
                SUM(CASE WHEN destination='34031' THEN trips ELSE NULL END) as "34031",
                SUM(CASE WHEN destination='34035' THEN trips ELSE NULL END) as "34035",
                SUM(CASE WHEN destination='34037' THEN trips ELSE NULL END) as "34037",
                SUM(CASE WHEN destination='34039' THEN trips ELSE NULL END) as "34039",
                SUM(CASE WHEN destination='34041' THEN trips ELSE NULL END) as "34041",
                -- HV
                SUM(CASE WHEN destination='36027' THEN trips ELSE NULL END) as "36027",
                SUM(CASE WHEN destination='36071' THEN trips ELSE NULL END) as "36071",
                SUM(CASE WHEN destination='36079' THEN trips ELSE NULL END) as "36079",
                SUM(CASE WHEN destination='36087' THEN trips ELSE NULL END) as "36087",
                SUM(CASE WHEN destination='36105' THEN trips ELSE NULL END) as "36105",
                SUM(CASE WHEN destination='36111' THEN trips ELSE NULL END) as "36111",
                SUM(CASE WHEN destination='36119' THEN trips ELSE NULL END) as "36119",
                -- LI
                SUM(CASE WHEN destination='36059' THEN trips ELSE NULL END) as "36059",
                SUM(CASE WHEN destination='36103' THEN trips ELSE NULL END) as "36103",
                -- PA
                SUM(CASE WHEN destination='42025' THEN trips ELSE NULL END) as "42025",
                SUM(CASE WHEN destination='42077' THEN trips ELSE NULL END) as "42077",
                SUM(CASE WHEN destination='42089' THEN trips ELSE NULL END) as "42089",
                SUM(CASE WHEN destination='42095' THEN trips ELSE NULL END) as "42095",
                SUM(CASE WHEN destination='42025' THEN trips ELSE NULL END) as "42103",
                -- Out of region
                SUM(CASE WHEN destination='O31CR' THEN trips ELSE NULL END) as "O31CR"
            FROM unpivot
            GROUP BY origin, date;
        $inner$, current_setting('myvars.date')::text, current_setting('myvars.date')::text)
        INTO query;
        EXECUTE query;
    ELSE
        RAISE NOTICE '% is already loaded in pivoted trips', current_setting('myvars.date')::text;
    END IF;
END $$