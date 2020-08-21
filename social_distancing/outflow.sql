SET myvars.date TO :'DATE';

DO $$
DECLARE
    computed boolean;
    _date text;
BEGIN
    SELECT current_setting('myvars.date')::text NOT IN (SELECT DISTINCT date FROM sg_outflow) INTO computed;
    SELECT current_setting('myvars.date')::text INTO _date;

    IF computed
    THEN
        INSERT INTO sg_outflow
        SELECT 
        current_setting('myvars.date')::text as date,
        origin_county::text,
        target::text,
        sum(counts) as count_all,
        sum(at_home*counts) as count_home
        FROM (
        SELECT 		
            LEFT(origin_census_block_group, 5) as origin_county,
            (origin_census_block_group = desti.key)::int AS at_home,
            (CASE 
                WHEN LEFT(desti.key, 5) in (
                    '36059','36103') THEN 'LI'
                    
                WHEN LEFT(desti.key, 5) in (
                    '09009','09005','09001') THEN 'CT'
                    
                WHEN LEFT(desti.key, 5) in (
                    '36027','36071','36105',
                    '36111') THEN 'Mid Hud'
                    
                WHEN LEFT(desti.key, 5) in (
                    '34019','34021','34025',
                    '34029','34037','34041') THEN 'NJ Out'
                    
                WHEN LEFT(desti.key, 5) in (
                    '34003','34013','34017',
                    '34023','34027','34031',
                    '34035','34039') THEN 'NJ In'
                    
                WHEN LEFT(desti.key, 5) in (
                    '42025','42077','42095',
                    '42089','42103') THEN 'PA'
                    
                WHEN LEFT(desti.key, 5) in (
                    '36005','36061','36081', 
                    '36047','36085') THEN 'NYC'
                    
                WHEN LEFT(desti.key, 5) in (
                    '36079','36087','36119') THEN 'Low Hud'
                    
                ELSE 'O31CR'
                
            END) as target,
            desti.value::int as counts
        FROM social_distancing."current_setting('myvars.date')",
            json_each_text(destination_cbgs) as desti
        WHERE LEFT(origin_census_block_group, 5) in (
            '36005', '36061', '36081', '36047', '36085'
        )) a
        GROUP BY origin_county, target;
    ELSE
        RAISE NOTICE '% is already loaded !', _date;
    END IF;
END $$