--Since this is my first time analysing this data set from NYC Open Data, I will want to do
--some exploratory data analysis
------------------------------------------------------------------------------
--Step 1: let's run some simple queries to learn more about the dataset.

-- row count
SELECT COUNT(*) AS rows FROM oath_cases;

-- sample 10
SELECT * FROM oath_cases LIMIT 10;

-- min/max hearing date
SELECT MIN(hearing_date) AS min_hearing, MAX(hearing_date) AS max_hearing
FROM oath_cases;

-- null coverage snapshot
SELECT 
  COUNT(*) AS n,
  SUM((case_id IS NULL)::int) AS case_id_nulls,
  SUM((hearing_date IS NULL)::int) AS hearing_date_nulls,
  SUM((violation_type IS NULL)::int) AS violation_type_nulls,
  SUM((decision IS NULL)::int) AS decision_nulls,
  SUM((amount_due IS NULL)::int) AS amount_due_nulls,
  SUM((amount_paid IS NULL)::int) AS amount_paid_nulls
FROM oath_cases;

-- duplicate ticket check
SELECT case_id, COUNT(*) AS dupes
FROM oath_cases
GROUP BY case_id
HAVING COUNT(*) > 1
ORDER BY dupes DESC
LIMIT 20;
------------------------------------------------------------------------------
-- Step 2, let's make a view to clean up the way we can see the data.
-- Drop and recreate a clean, analysis-ready view
DROP VIEW IF EXISTS oath_clean;
CREATE OR REPLACE VIEW oath_clean AS
SELECT
  case_id,
  hearing_date::date AS hearing_date,
  CASE
  WHEN TRIM(violation_type) ILIKE 'TAXI_TLC'                 THEN 'TLC'
  WHEN TRIM(violation_type) ILIKE 'TAXI\_PORT AUTHORITY' ESCAPE '\' THEN 'PORT AUTHORITY'
  WHEN TRIM(violation_type) ILIKE 'DOHMH%'                   THEN 'DOHMH'
  WHEN TRIM(violation_type) IS NULL OR TRIM(violation_type) = '' THEN 'UNKNOWN'
  ELSE TRIM(violation_type)
END AS violation_type,
  CASE
  WHEN UPPER(TRIM(decision)) IN ('DISMISSED') THEN 'DISMISSED'
  WHEN UPPER(TRIM(decision)) IN ('DEFAULT', 'DEFAULT/ NO APPEARANCE', 'DEFAULT/NO APPEARANCE', 'DEFAULT - NO APPEARANCE') THEN 'DEFAULT'
  WHEN UPPER(TRIM(decision)) IN ('IN VIOLATION','SUSTAINED') THEN 'SUSTAINED'
  WHEN TRIM(decision) IS NULL OR TRIM(decision) = '' THEN 'UNKNOWN'
  ELSE UPPER(TRIM(decision))
END AS decision,
  /* ensure numeric */
  amount_due::numeric    AS amount_due,
  amount_paid::numeric   AS amount_paid,
  /* derived */
  COALESCE(amount_due,0) - COALESCE(amount_paid,0) AS outstanding
FROM oath_cases;

--Add indexes here for faster grouping, filtering, and ordering.
CREATE INDEX IF NOT EXISTS idx_oath_hearing_date   ON oath_cases (hearing_date);
CREATE INDEX IF NOT EXISTS idx_oath_violation_type ON oath_cases (violation_type);
CREATE INDEX IF NOT EXISTS idx_oath_decision       ON oath_cases (decision);
------------------------------------------------------------------------------
-- Step 3, let's look at some snapshots at collections, their decisions, and the agencies
-- that issue them.

-- collections snapshot
SELECT
  SUM(amount_due)  AS total_penalties,
  SUM(amount_paid) AS total_paid,
  SUM(outstanding) AS total_outstanding,
  ROUND(100.0 * SUM(CASE WHEN amount_due > 0 THEN amount_paid ELSE 0 END)
        / NULLIF(SUM(amount_due),0), 2) AS collection_rate_pct
FROM oath_clean;

-- decision mix
SELECT decision, COUNT(*) AS cases,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM oath_clean
GROUP BY decision
ORDER BY cases DESC;

-- top issuing agencies by cases
SELECT violation_type, COUNT(*) AS cases
FROM oath_clean
GROUP BY violation_type
ORDER BY cases DESC
LIMIT 15;

-- top agencies by outstanding balance
SELECT violation_type,
       SUM(outstanding) AS outstanding_sum,
       COUNT(*) AS tickets
FROM oath_clean
GROUP BY violation_type
HAVING SUM(outstanding) > 0
ORDER BY outstanding_sum DESC
LIMIT 15;
------------------------------------------------------------------------------
-- Step 4, let's look at monthly trends + cross-tabs

-- cases per month
SELECT date_trunc('month', hearing_date)::date AS month, COUNT(*) AS cases
FROM oath_clean
WHERE hearing_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- penalties vs paid per month + collection rate
WITH m AS (
  SELECT date_trunc('month', hearing_date)::date AS month,
         SUM(amount_due)  AS penalties,
         SUM(amount_paid) AS paid
  FROM oath_clean
  WHERE hearing_date IS NOT NULL
  GROUP BY 1
)
SELECT month, penalties, paid,
       ROUND(100.0 * paid / NULLIF(penalties,0), 2) AS collection_rate_pct
FROM m
ORDER BY month;

-- decisions by agency (top 10 agencies)
WITH top_agencies AS (
  SELECT violation_type
  FROM oath_clean
  GROUP BY violation_type
  ORDER BY COUNT(*) DESC
  LIMIT 10
)
SELECT c.violation_type, c.decision, COUNT(*) AS cases
FROM oath_clean c
JOIN top_agencies t ON c.violation_type = t.violation_type
GROUP BY c.violation_type, c.decision
ORDER BY c.violation_type, cases DESC;

-- 80/20: which agencies drive most outstanding?
WITH by_agency AS (
  SELECT violation_type, SUM(outstanding) AS out_sum
  FROM oath_clean
  GROUP BY violation_type
),
ranked AS (
  SELECT violation_type, out_sum,
         RANK() OVER (ORDER BY out_sum DESC) AS rnk,
         SUM(out_sum) OVER () AS total_out
  FROM by_agency
)
SELECT violation_type, out_sum,
       ROUND(100.0 * out_sum / NULLIF(total_out,0), 2) AS pct_of_total
FROM ranked
ORDER BY out_sum DESC
LIMIT 20;

------------------------------------------------------------------------------
-- Step 5 some basic distributions
-- basic distribution of penalties in $$
SELECT
  COUNT(*)                                AS n,
  ROUND(AVG(amount_due),2)                AS avg_due,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_due) AS median_due,
  MAX(amount_due)                         AS max_due
FROM oath_clean
WHERE amount_due IS NOT NULL;

-- same for outstanding $$
SELECT
  COUNT(*) AS n,
  ROUND(AVG(outstanding),2) AS avg_outstanding,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY outstanding) AS median_outstanding,
  MAX(outstanding) AS max_outstanding
FROM oath_clean
WHERE outstanding > 0;

------------------------------------------------------------------------------
-- Step 6, exports for reviewers
-- Save key analysis outputs to CSV so others can inspect without running queries

------------------------------------------------------------------------------
-- Step 6, exports for reviewers
-- Save key analysis outputs to CSV so others can inspect without running queries

\copy (SELECT * FROM oath_clean LIMIT 100) TO 'C:/Users/Arami/OneDrive/Desktop/nycopendataprojectoath/oath_clean_sample.csv' WITH (FORMAT csv, HEADER true)

\copy (SELECT decision, COUNT(*) AS cases, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct FROM oath_clean GROUP BY decision ORDER BY cases DESC) TO 'C:/Users/Arami/OneDrive/Desktop/nycopendataprojectoath/decision_mix.csv' WITH (FORMAT csv, HEADER true)

\copy (SELECT violation_type, SUM(outstanding) AS outstanding_sum, COUNT(*) AS tickets FROM oath_clean GROUP BY violation_type HAVING SUM(outstanding) > 0 ORDER BY outstanding_sum DESC LIMIT 15) TO 'C:/Users/Arami/OneDrive/Desktop/nycopendataprojectoath/top_outstanding_agencies.csv' WITH (FORMAT csv, HEADER true)

\copy (WITH m AS (SELECT date_trunc('month', hearing_date)::date AS month, SUM(amount_due) AS penalties, SUM(amount_paid) AS paid FROM oath_clean WHERE hearing_date IS NOT NULL GROUP BY 1) SELECT month, penalties, paid, ROUND(100.0 * paid / NULLIF(penalties,0), 2) AS collection_rate_pct FROM m ORDER BY month) TO 'C:/Users/Arami/OneDrive/Desktop/nycopendataprojectoath/monthly_collections.csv' WITH (FORMAT csv, HEADER true)






