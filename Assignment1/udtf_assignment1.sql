# just for test
CREATE OR REPLACE TABLE unit_test(
    label INT,
    text STRING
);
INSERT INTO unit_test VALUES
(0, 'just plain boring'),
(0, 'entirely predictable and lacks energy'),
(0, 'no surprises and very few laughs'),
(1, 'very powerful'),
(1, 'the most fun film of the summer');


CREATE OR REPLACE TABLE unit_test_test(
    label INT,
    text STRING
);
INSERT INTO unit_test_test VALUES
(0, 'predictable with no fun');



# start point
CREATE OR REPLACE TABLE combined_yelp AS
SELECT *, 1 AS table_order FROM yelp_flattened_training
UNION ALL
SELECT *, 2 AS table_order FROM yelp_flattened_testing
ORDER BY table_order ASC;

CREATE OR REPLACE FUNCTION naive_bayes_train_and_predict_udtf(
    label INT, 
    review_text STRING, 
    table_order INT
)
RETURNS TABLE (
    label INT, 
    prediction INT, 
    confidence FLOAT, 
    word STRING, 
    word_label INT, 
    word_count INT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'NaiveBayesTrainAndPredictUDTF'
AS $$
import re
from collections import defaultdict
import math

class NaiveBayesTrainAndPredictUDTF:
    def __init__(self):
        self.word_counts = defaultdict(lambda: defaultdict(int))  # word -> label -> count
        self.label_counts = defaultdict(int)  # label -> doc count
        self.label_word_counts = defaultdict(int)  # label -> unique word count
        self.total_docs = 0  # total number of documents
        self.training_data_processed = 0  
        self.test_data = []  # hold test data for later predictions
        self.computation = defaultdict(lambda: defaultdict(int))

    def clean_and_split(self, text):
        text = text.lower()  # convert text to lowercase
        text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
        words = text.split()  # split into words
        return [word for word in words if word]  # filter out empty words

    def train(self, label, words):
        self.total_docs += 1
        self.label_counts[label] += 1
        for word in words:
            self.word_counts[word][label] += 1
            self.label_word_counts[label] += 1  # count unique words in each label

    def predict(self, words):
        scores = defaultdict(float)
        for label in self.label_counts:
            scores[label] = math.log(self.label_counts[label] / self.total_docs)  # prior probability P(label)
            for word in words:
                if word not in self.word_counts:
                    continue
                word_count = self.word_counts[word][label]
                word_prob = math.log((word_count + 1) / (len(self.word_counts) + self.label_word_counts[label]))
                scores[label] += word_prob
                self.computation[word][label] = word_count + 1

        best_label = max(scores, key=scores.get)  # find label with max score
        confidence = scores[best_label]  # return score for best label

        return best_label, confidence

    def process(self, label, review_text, table_order):
        words = self.clean_and_split(review_text)

        if table_order == 1:
            # Training data
            self.train(label, words)
            self.training_data_processed += 1
        elif table_order == 2:
            # Testing data
            self.test_data.append((label, review_text))

    def end_partition(self):
        if self.training_data_processed == 0:
            raise ValueError("No training data processed before testing.")
        
        # Predict for each test data instance
        for label, review_text in self.test_data:
            words = self.clean_and_split(review_text)
            known_words = [word for word in words if word in self.word_counts]
            if len(known_words) == 0:
                continue  # Skip prediction if no known words are found
            else:
                prediction, confidence = self.predict(words)
                yield (label, prediction, confidence, None, None, None)

        # Yield additional computed data
        # yield (len(self.word_counts), None, None, None, None, None)
        # yield (len(self.word_counts) + self.label_word_counts[1], None, None, None, None, None)

        # Yield word count results
        # for word, label_dict in self.computation.items():
        #    for word_label, count in label_dict.items():
        #        yield (None, None, None, word, word_label, count)

$$;










	
	
CREATE OR REPLACE TABLE model AS
SELECT
    nb.*
FROM
    combined_yelp cy,
    TABLE(naive_bayes_train_and_predict_udtf(cy.label, cy.text, cy.table_order) OVER (PARTITION BY 1)) nb;
	
	
select count(*) from model where label = prediction;

