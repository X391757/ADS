CREATE STAGE my_int_stage;
PUT file://D:/dataset/yelp_review_full/yelp_review_full/test-00000-of-00001.parquet  @my_int_stage;
PUT file://D:/dataset/yelp_review_full/yelp_review_full/train-00000-of-00001.parquet  @my_int_stage;


CREATE OR REPLACE TABLE yelp_training(val variant);
CREATE OR REPLACE TABLE yelp_testing(val variant);
copy into yelp_training from @my_int_stage/train-00000-of-00001.parquet file_fromat = training_db.TPCH_SF1.MYPARQUETFORMAT;
copy into yelp_testing from @my_int_stage/test-00000-of-00001.parquet file_fromat = training_db.TPCH_SF1.MYPARQUETFORMAT;

CREATE OR REPLACE TABLE yelp_flattened_training1 AS
SELECT 
    VAL:label::INT AS label, 
    VAL:text::STRING AS text
FROM yelp_training;

CREATE OR REPLACE TABLE yelp_flattened_testing AS
SELECT 
    VAL:label::INT AS label,
    VAL:text::STRING AS text
FROM yelp_testing;