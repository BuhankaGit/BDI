-- Create table for transactions dataset
CREATE TABLE gtsyganov_371942.transactions ON CLUSTER kube_clickhouse_cluster 
(
    user_id_out Int64, 
    user_id_in Int64, 
    important Bool, 
    amount Float64, 
    datetime DateTime
) 
ENGINE = MergeTree() 
PARTITION BY toYYYYMM(datetime) 
ORDER BY (user_id_out, user_id_in);

-- Create table distibuted by month
CREATE TABLE gtsyganov_371942.transactions_d ON CLUSTER kube_clickhouse_cluster AS 
gtsyganov_371942.transactions 
ENGINE = Distributed(
    kube_clickhouse_cluster, 
    gtsyganov_371942, 
    transactions, 
    intHash64(toYYYYMM(datetime))
);

/* 
Insert data:
cat shared-data/clickhouse_data/transactions_12M.parquet | clickhouse-client --host=clickhouse-1.clickhouse.clickhouse --user=gtsyganov_371942 --password=password --query="INSERT INTO gtsyganov_371942.transactions_d FORMAT Parquet"
*/

-- MV 1. Average amount for incoming and outcoming transactions by months and days for each user
CREATE MATERIALIZED VIEW gtsyganov_371942.v_avg_by_day ON CLUSTER kube_clickhouse_cluster 
ENGINE = AggregatingMergeTree 
ORDER BY (user_id, date) POPULATE AS 
SELECT 
    A.user_id, 
    A.avg_income, 
    B.avg_outcome, 
    A.date 
FROM 
(
    SELECT 
        user_id_in as user_id, 
        avgState(amount) as avg_income, 
        toDate(datetime) as date
    FROM gtsyganov_371942.transactions_d 
    GROUP BY user_id_in, toDate(datetime)
) A 
    JOIN 
(
    SELECT 
        user_id_out as user_id, 
        avgState(amount) as avg_outcome, 
        toDate(datetime) as date
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id_out, toDate(datetime)
) B 
ON A.user_id = B.user_id AND A.date = B.date;

CREATE MATERIALIZED VIEW gtsyganov_371942.v_avg_by_month ON CLUSTER kube_clickhouse_cluster 
ENGINE = AggregatingMergeTree 
ORDER BY (user_id, month) POPULATE AS 
SELECT 
    A.user_id, 
    A.avg_income, 
    B.avg_outcome, 
    A.month
FROM 
(
    SELECT 
        user_id_in as user_id, 
        avgState(amount) as avg_income,
        toMonth(datetime) as month
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id_in, toMonth(datetime)
) A 
    JOIN 
(
    SELECT 
        user_id_out as user_id, 
        avgState(amount) as avg_outcome,
        toMonth(datetime) as month
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id_out, toMonth(datetime)
) B 
ON A.user_id = B.user_id AND A.month = B.month;

-- MV 3. The sums for incoming and outcoming transactions by months for each user
CREATE MATERIALIZED VIEW gtsyganov_371942.v_sum_by_month ON CLUSTER kube_clickhouse_cluster 
ENGINE = AggregatingMergeTree 
ORDER BY (user_id, month) POPULATE AS 
SELECT 
    A.user_id, 
    A.month, 
    A.amount as income, 
    B.amount as outcome 
FROM 
(
    SELECT 
        user_id_out as user_id, 
        toMonth(datetime) as month, 
        sum(amount) as amount 
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id, toMonth(datetime)
) A 
JOIN 
(
    SELECT 
        user_id_in as user_id, 
        toMonth(datetime) as month, 
        sum(amount) as amount 
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id, toMonth(datetime)
) B 
ON A.user_id = B.user_id AND A.month = B.month;


-- MV 4. User's saldo (difference between income and expense)
CREATE MATERIALIZED VIEW gtsyganov_371942.v_count_saldo ON CLUSTER kube_clickhouse_cluster 
ENGINE = AggregatingMergeTree 
ORDER BY (user_id, saldo) POPULATE AS 
SELECT 
    A.user_id, 
    (B.income - A.outcome) as saldo 
FROM 
(
    SELECT 
        user_id_out as user_id, 
        sum(amount) as outcome 
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id_out
) A 
JOIN 
(
    SELECT 
        user_id_in as user_id, 
        sum(amount) as income 
    FROM gtsyganov_371942.transactions 
    GROUP BY user_id_in
) B 
ON A.user_id = B.user_id;

-- Check output

SELECT
    user_id,
    avgMerge(avg_income) as avg_income,
    avgMerge(avg_outcome) as avg_outcome,
    date
FROM gtsyganov_371942.v_avg_by_day
WHERE user_id = 123
GROUP BY user_id, date
ORDER BY user_id, date
LIMIT 10;

SELECT
    user_id,
    avgMerge(avg_income) as avg_income,
    avgMerge(avg_outcome) as avg_outcome,
    month
FROM gtsyganov_371942.v_avg_by_month
WHERE user_id = 123
GROUP BY user_id, month
ORDER BY user_id, month;

SELECT 
    user_id, 
    max(income),
    max(outcome) 
FROM gtsyganov_371942.v_sum_by_month
WHERE user_id IN (123, 456, 789) 
GROUP BY user_id;

SELECT 
    user_id, 
    saldo 
FROM gtsyganov_371942.v_count_saldo 
WHERE saldo < 0 
LIMIT 20;
