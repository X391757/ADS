#yelp_flattened_training


CREATE OR REPLACE TABLE SQL_PRIOR AS 
SELECT label, COUNT(*)::float / (SELECT COUNT(*) FROM yelp_flattened_training) AS prior
FROM yelp_flattened_training
GROUP BY label;


CREATE OR REPLACE FUNCTION clean_and_split(text STRING)
RETURNS ARRAY
LANGUAGE python
RUNTIME_VERSION = '3.8'
HANDLER = 'clean_and_split_py'
AS
$$
import re

def clean_and_split_py(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    words = text.split()
    return [word for word in words if word]
$$;


CREATE OR REPLACE TABLE word_counts AS
SELECT 
    label, 
    value AS word,  -- The 'value' column from FLATTEN represents each word
    COUNT(*) AS word_count
FROM (
    SELECT 
        label, 
        clean_and_split(text) AS words  -- Apply the UDF to split text into words
    FROM yelp_flattened_training
) AS t,
LATERAL FLATTEN(input => t.words)  -- Unnest the array of words into individual rows
WHERE value != ''  -- Ensure no empty strings are counted
GROUP BY label, value;  -- Group by label and word


CREATE OR REPLACE TABLE word_counts_full AS
WITH Labels AS (
    SELECT DISTINCT LABEL
    FROM word_counts
),
Words AS (
    SELECT DISTINCT WORD
    FROM word_counts
),
-- First, get all distinct labels and words
all_labels_words AS (
    SELECT L.LABEL, W.WORD
    FROM Labels L
    CROSS JOIN Words W
),
-- Then, left join with the original table to get all combinations
expanded_table AS (
    SELECT 
        alw.label,
        alw.word,
        COALESCE(t.word_count, 0) AS word_count
    FROM all_labels_words alw
    LEFT JOIN word_counts t ON alw.label = t.label AND alw.word = t.word
)
-- Finally, select from the expanded table
SELECT 
    label,
    word,
    word_count
FROM expanded_table
ORDER BY word, label;


CREATE OR REPLACE TABLE Con_Pro AS 
WITH total_words_per_label AS (
    SELECT label, SUM(word_count) AS total_word_count
    FROM WORD_COUNTS_FULL
    GROUP BY label
),
vocab_size AS (
    -- Count distinct words in the vocabulary
    SELECT COUNT(DISTINCT word) AS vocab_count FROM WORD_COUNTS_FULL
)
-- Calculate conditional probability with Laplace smoothing
SELECT 
    wc.label, 
    wc.word, 
    LN((wc.word_count + 1)::float / (tw.total_word_count + v.vocab_count)) AS conditional_probability
FROM WORD_COUNTS_FULL wc
JOIN total_words_per_label tw ON wc.label = tw.label
JOIN vocab_size v;



CREATE OR REPLACE TABLE sql_yelp_flatteded AS
with test_document AS (
    SELECT 
        label, 
        clean_and_split(text) AS text
    FROM yelp_flattened_testing
)
SELECT
    label,
    text,
    ROW_NUMBER() OVER (order by 1 ASC) AS id
  FROM test_document;


CREATE OR REPLACE TABLE test_word_counts AS
WITH sql_yelp_flattened_1 AS (
    SELECT
        label,
        id,
        word.value AS word  -- The 'value' column from FLATTEN represents each word
    FROM sql_yelp_flatteded t,  -- Replace 'your_table' with the actual table name
    LATERAL FLATTEN(input => t.text) word  -- Unnest the array of words into individual rows
    WHERE word.value != ''  -- Ensure no empty strings are counted
)
SELECT
    label AS test_label,
    word,
    id
FROM
    sql_yelp_flattened_1;






CREATE OR REPLACE TABLE final AS
select test_word_counts.test_label,test_word_counts.word,test_word_counts.id,CON_PRO.label,CON_PRO.conditional_probability,SQL_PRIOR.prior from test_word_counts 
INNER join CON_PRO on test_word_counts.word = CON_PRO.word 
LEFT JOIN SQL_PRIOR ON SQL_PRIOR.label = CON_PRO.label;

CREATE OR REPLACE TABLE final_1 AS
select id,label, SUM(conditional_probability) as pro from final group by id,label;

CREATE OR REPLACE TABLE final_1_1 AS
SELECT 
    final_1.id,
    final_1.label,
    final_1.pro + LN(SQL_PRIOR.prior) AS pro
FROM 
    final_1 
JOIN 
    SQL_PRIOR 
ON 
    final_1.label = SQL_PRIOR.label;

CREATE OR REPLACE TABLE final_2 AS
WITH axx AS (
    -- Select the maximum PRO for each ID
    SELECT 
        id, 
        MAX(PRO) AS max_pro
    FROM final_1_1
    GROUP BY id
)
SELECT 
    f1.id,
    f1.label,
    axx.max_pro
FROM final_1_1 f1
INNER JOIN axx
    ON f1.id = axx.id
    AND f1.pro = axx.max_pro;
	
CREATE OR REPLACE TABLE final_3 AS
select final_2.id,final_2.label as prediction,syf.label from final_2 inner join sql_yelp_flatteded syf on syf.id=final_2.id;

select count(*) from final_3 where prediction=label;