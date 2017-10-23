## Nested data

### Filter on our chosen trigram
SELECT * \
FROM \`bigquery-public-data.samples.trigrams\` \
WHERE ngram = 'terabytes of data'

### Unnest
SELECT \
  ngram, \
  c.volume_count \
FROM \`bigquery-public-data.samples.trigrams\` \
JOIN UNNEST(cell) AS c \
WHERE ngram = 'terabytes of data'

### Aggregate
SELECT \
  ngram, \
  AVG(c.volume_count) AS avg_volume_count \
FROM \`bigquery-public-data.samples.trigrams\` \
JOIN UNNEST(cell) AS c \
WHERE ngram = 'terabytes of data'\
GROUP BY ngram


### A more complicated query
WITH decomposed AS ( \
  SELECT first AS word, c.volume_count \
  FROM \`bigquery-public-data.samples.trigrams\` \
  JOIN UNNEST(cell) AS c \
\
  UNION ALL \
\
  SELECT second AS word, c.volume_count \
  FROM \`bigquery-public-data.samples.trigrams\` \
  JOIN UNNEST(cell) AS c \
\
  UNION ALL \
\
  SELECT third AS word, c.volume_count \
  FROM \`bigquery-public-data.samples.trigrams\` \
  JOIN UNNEST(cell) AS c \
)

SELECT LOWER(word), SUM(volume_count) AS total \
FROM decomposed \
WHERE REGEXP_CONTAINS(word, r'^\\w\*$'') \
GROUP BY LOWER(word) \
ORDER BY total DESC \
LIMIT 50

## Wildcards
SELECT \
  max, \
  ROUND((max-32)\*5\/9,1) celsius, \
  year \
FROM \`bigquery-public-data.noaa_gsod.gsod19\*\` \
WHERE max != 9999.9 # code for missing data \
AND _TABLE_SUFFIX BETWEEN '29' AND '40' \
ORDER BY max DESC

## Counting
### Exact
SELECT COUNT(DISTINCT id) exact \
FROM \`fh-bigquery.reddit_comments.20\*\`

### Approximate
SELECT APPROX_COUNT_DISTINCT(id) approx \
FROM \`fh-bigquery.reddit_comments.20\*\`
