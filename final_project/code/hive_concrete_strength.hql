-- I use a separate database so I do not change my earlier assignments.
CREATE DATABASE IF NOT EXISTS dsc650_final;
USE dsc650_final;

-- The CSV is headerless, and its field order matches this managed table.
CREATE TABLE final_concrete_strength (
  cement_kg_m3 DOUBLE COMMENT 'Cement in kilograms per cubic metre',
  slag_kg_m3 DOUBLE COMMENT 'Blast furnace slag in kilograms per cubic metre',
  fly_ash_kg_m3 DOUBLE COMMENT 'Fly ash in kilograms per cubic metre',
  water_kg_m3 DOUBLE COMMENT 'Water in kilograms per cubic metre',
  superplasticizer_kg_m3 DOUBLE COMMENT 'Superplasticizer in kilograms per cubic metre',
  coarse_aggregate_kg_m3 DOUBLE COMMENT 'Coarse aggregate in kilograms per cubic metre',
  fine_aggregate_kg_m3 DOUBLE COMMENT 'Fine aggregate in kilograms per cubic metre',
  age_days INT COMMENT 'Curing age in days',
  strength_mpa DOUBLE COMMENT 'Observed compressive strength in MPa'
)
COMMENT 'New DSC650 final dataset from Yeh via UCI DOI 10.24432/C5PK67'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- I load only the file that NiFi has already written into HDFS.
-- LOAD DATA moves this file into the managed table directory.
LOAD DATA INPATH '/user/violettakalinicheva/dsc650_final/raw/concrete_strength.csv'
INTO TABLE final_concrete_strength;

SHOW CREATE TABLE final_concrete_strength;
DESCRIBE FORMATTED final_concrete_strength;
SELECT * FROM final_concrete_strength
ORDER BY cement_kg_m3, slag_kg_m3, fly_ash_kg_m3, water_kg_m3,
         superplasticizer_kg_m3, coarse_aggregate_kg_m3,
         fine_aggregate_kg_m3, age_days, strength_mpa
LIMIT 5;

-- I compare these totals with my independently checked source data.
SELECT COUNT(*) AS total_rows,
       ROUND(SUM(strength_mpa), 2) AS total_strength_mpa,
       ROUND(AVG(strength_mpa), 6) AS average_strength_mpa,
       MIN(strength_mpa) AS minimum_strength_mpa,
       MAX(strength_mpa) AS maximum_strength_mpa,
       SUM(CASE WHEN cement_kg_m3 IS NULL OR slag_kg_m3 IS NULL
         OR fly_ash_kg_m3 IS NULL OR water_kg_m3 IS NULL
         OR superplasticizer_kg_m3 IS NULL OR coarse_aggregate_kg_m3 IS NULL
         OR fine_aggregate_kg_m3 IS NULL OR age_days IS NULL
         OR strength_mpa IS NULL THEN 1 ELSE 0 END) AS rows_with_nulls
FROM final_concrete_strength;

-- These are descriptive age groups, not evidence that age caused the difference.
SELECT CASE WHEN age_days <= 7 THEN '01_age_le_7'
            WHEN age_days <= 28 THEN '02_age_8_to_28'
            WHEN age_days <= 90 THEN '03_age_29_to_90'
            ELSE '04_age_gt_90' END AS age_group,
       COUNT(*) AS observations,
       ROUND(SUM(strength_mpa), 2) AS strength_sum_mpa,
       ROUND(AVG(strength_mpa), 6) AS mean_strength_mpa
FROM final_concrete_strength
GROUP BY CASE WHEN age_days <= 7 THEN '01_age_le_7'
              WHEN age_days <= 28 THEN '02_age_8_to_28'
              WHEN age_days <= 90 THEN '03_age_29_to_90'
              ELSE '04_age_gt_90' END
ORDER BY age_group;
