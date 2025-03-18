INSERT INTO dim_heroes (hero_id, hero_name, hero_main_role)
SELECT DISTINCT ON ((raw_json->>'id')::INT) 
    (raw_json->>'id')::INT,
    raw_json->>'localized_name',
    raw_json->>'primary_attr'
FROM stg_heroes;