WITH 
-- Raw data preparation and cleaning
raw_transactions AS (
  SELECT
    transaction_id,
    user_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', transaction_time) AS transaction_time,
    product_id,
    product_category,
    quantity,
    unit_price,
    (quantity * unit_price) AS total_price,
    payment_method,
    shipping_country,
    CASE 
      WHEN device_type IN ('mobile', 'tablet') THEN 'mobile'
      WHEN device_type = 'desktop' THEN 'desktop'
      ELSE 'other'
    END AS device_type_clean,
    is_return
  FROM `project.dataset.transactions`
  WHERE transaction_time BETWEEN '2023-01-01' AND '2023-12-31'
    AND is_test_transaction = FALSE
),

-- Customer metadata enrichment
customer_metadata AS (
  SELECT
    user_id,
    MIN(transaction_time) AS first_purchase_date,
    MAX(transaction_time) AS last_purchase_date,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(CASE WHEN is_return THEN 0 ELSE total_price END) AS total_spend,
    SUM(CASE WHEN is_return THEN 0 ELSE 1 END) AS total_purchases,
    SUM(CASE WHEN is_return THEN quantity ELSE 0 END) AS total_items_returned,
    COUNT(DISTINCT product_category) AS distinct_categories_purchased,
    COUNT(DISTINCT DATE(transaction_time)) AS distinct_purchase_days,
    APPROX_TOP_COUNT(payment_method, 1)[OFFSET(0)].value AS most_used_payment_method,
    APPROX_TOP_COUNT(shipping_country, 1)[OFFSET(0)].value AS most_common_shipping_country,
    APPROX_TOP_COUNT(device_type_clean, 1)[OFFSET(0)].value AS most_common_device_type
  FROM raw_transactions
  GROUP BY user_id
),

-- RFM (Recency, Frequency, Monetary) analysis
rfm_calculation AS (
  SELECT
    user_id,
    DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) AS recency_days,
    total_transactions AS frequency,
    total_spend AS monetary,
    -- RFM scoring (1-5 scale)
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 30 THEN 5
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 60 THEN 4
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 90 THEN 3
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 180 THEN 2
      ELSE 1
    END AS recency_score,
    CASE
      WHEN total_transactions >= 20 THEN 5
      WHEN total_transactions >= 10 THEN 4
      WHEN total_transactions >= 5 THEN 3
      WHEN total_transactions >= 2 THEN 2
      ELSE 1
    END AS frequency_score,
    CASE
      WHEN total_spend >= 1000 THEN 5
      WHEN total_spend >= 500 THEN 4
      WHEN total_spend >= 200 THEN 3
      WHEN total_spend >= 50 THEN 2
      ELSE 1
    END AS monetary_score,
    (CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 30 THEN 5
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 60 THEN 4
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 90 THEN 3
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_purchase_date), DAY) <= 180 THEN 2
      ELSE 1
    END + 
    CASE
      WHEN total_transactions >= 20 THEN 5
      WHEN total_transactions >= 10 THEN 4
      WHEN total_transactions >= 5 THEN 3
      WHEN total_transactions >= 2 THEN 2
      ELSE 1
    END +
    CASE
      WHEN total_spend >= 1000 THEN 5
      WHEN total_spend >= 500 THEN 4
      WHEN total_spend >= 200 THEN 3
      WHEN total_spend >= 50 THEN 2
      ELSE 1
    END) AS rfm_total_score
  FROM customer_metadata
),

-- Customer segmentation based on RFM
customer_segments AS (
  SELECT
    r.*,
    c.* EXCEPT(user_id),
    CASE
      WHEN r.rfm_total_score >= 13 THEN 'Champions'
      WHEN r.rfm_total_score >= 11 THEN 'Loyal Customers'
      WHEN r.rfm_total_score >= 9 THEN 'Potential Loyalists'
      WHEN r.recency_score >= 4 THEN 'New Customers'
      WHEN r.recency_score <= 2 AND r.frequency_score >= 3 THEN 'At Risk'
      WHEN r.recency_score <= 2 AND r.frequency_score <= 2 THEN 'Hibernating'
      WHEN r.monetary_score >= 4 THEN 'Big Spenders'
      ELSE 'Need Attention'
    END AS customer_segment
  FROM rfm_calculation r
  JOIN customer_metadata c ON r.user_id = c.user_id
),

-- Product performance analysis
product_metrics AS (
  SELECT
    product_id,
    product_category,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_price) AS total_revenue,
    SUM(CASE WHEN is_return THEN quantity ELSE 0 END) AS total_quantity_returned,
    SUM(CASE WHEN is_return THEN total_price ELSE 0 END) AS total_revenue_returned,
    COUNT(DISTINCT transaction_id) AS transaction_count,
    AVG(quantity) AS avg_quantity_per_transaction,
    AVG(total_price) AS avg_revenue_per_transaction,
    (SUM(CASE WHEN is_return THEN quantity ELSE 0 END) / SUM(quantity)) AS return_rate,
    COUNT(DISTINCT CASE WHEN device_type_clean = 'mobile' THEN user_id END) AS mobile_customers,
    COUNT(DISTINCT CASE WHEN device_type_clean = 'desktop' THEN user_id END) AS desktop_customers
  FROM raw_transactions
  GROUP BY product_id, product_category
),

-- Category-level analysis
category_metrics AS (
  SELECT
    product_category,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(total_quantity_sold) AS category_quantity_sold,
    SUM(total_revenue) AS category_revenue,
    SUM(total_quantity_returned) AS category_quantity_returned,
    SUM(total_revenue_returned) AS category_revenue_returned,
    SUM(unique_customers) AS category_unique_customers,
    SUM(transaction_count) AS category_transaction_count,
    AVG(avg_quantity_per_transaction) AS category_avg_quantity,
    AVG(avg_revenue_per_transaction) AS category_avg_revenue,
    (SUM(total_quantity_returned) / SUM(total_quantity_sold)) AS category_return_rate,
    PERCENTILE_CONT(avg_revenue_per_transaction, 0.5) OVER() AS median_revenue_per_transaction
  FROM product_metrics
  GROUP BY product_category
),

-- Time-based analysis (daily, weekly, monthly)
time_analysis AS (
  SELECT
    DATE(transaction_time) AS transaction_date,
    EXTRACT(ISOWEEK FROM transaction_time) AS week_of_year,
    EXTRACT(MONTH FROM transaction_time) AS month_of_year,
    EXTRACT(QUARTER FROM transaction_time) AS quarter_of_year,
    COUNT(DISTINCT transaction_id) AS daily_transactions,
    COUNT(DISTINCT user_id) AS daily_customers,
    SUM(total_price) AS daily_revenue,
    SUM(CASE WHEN is_return THEN 0 ELSE total_price END) AS daily_net_revenue,
    SUM(CASE WHEN is_return THEN total_price ELSE 0 END) AS daily_returns,
    SUM(CASE WHEN is_return THEN 0 ELSE quantity END) AS daily_quantity_sold,
    AVG(CASE WHEN is_return THEN 0 ELSE total_price END) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN device_type_clean = 'mobile' THEN user_id END) AS mobile_customers,
    COUNT(DISTINCT CASE WHEN device_type_clean = 'desktop' THEN user_id END) AS desktop_customers,
    APPROX_TOP_COUNT(product_category, 1)[OFFSET(0)].value AS top_category,
    APPROX_TOP_COUNT(payment_method, 1)[OFFSET(0)].value AS top_payment_method
  FROM raw_transactions
  GROUP BY transaction_date, week_of_year, month_of_year, quarter_of_year
),

-- Customer purchase sequences and patterns
customer_purchase_patterns AS (
  SELECT
    user_id,
    ARRAY_AGG(
      STRUCT(
        transaction_time,
        transaction_id,
        product_id,
        product_category,
        total_price
      )
      ORDER BY transaction_time
    ) AS purchase_history,
    ARRAY(
      SELECT AS STRUCT 
        LEAD(product_category) OVER (PARTITION BY user_id ORDER BY transaction_time) AS next_category,
        product_category AS current_category,
        LEAD(transaction_time) OVER (PARTITION BY user_id ORDER BY transaction_time) AS next_purchase_time,
        transaction_time AS current_purchase_time,
        DATE_DIFF(
          LEAD(transaction_time) OVER (PARTITION BY user_id ORDER BY transaction_time),
          transaction_time,
          DAY
        ) AS days_to_next_purchase
      FROM raw_transactions
      WHERE user_id = cm.user_id AND NOT is_return
      ORDER BY transaction_time
    ) AS purchase_sequence,
    -- Calculate average time between purchases
    (
      SELECT AVG(days_to_next_purchase)
      FROM UNNEST(purchase_sequence)
      WHERE days_to_next_purchase IS NOT NULL
    ) AS avg_days_between_purchases,
    -- Calculate most common next category
    (
      SELECT next_category
      FROM (
        SELECT next_category, COUNT(*) AS cnt
        FROM UNNEST(purchase_sequence)
        WHERE next_category IS NOT NULL
        GROUP BY next_category
        ORDER BY cnt DESC
        LIMIT 1
      )
    ) AS most_common_next_category
  FROM customer_metadata cm
),

-- Cohort analysis (monthly cohorts)
monthly_cohorts AS (
  SELECT
    cohort_month,
    cohort_size,
    months_since_cohort,
    retained_users,
    ROUND(retained_users / cohort_size * 100, 2) AS retention_rate,
    total_spend,
    ROUND(total_spend / cohort_size, 2) AS avg_spend_per_user,
    total_orders,
    ROUND(total_orders / cohort_size, 2) AS avg_orders_per_user
  FROM (
    SELECT
      DATE_TRUNC(DATE(first_purchase_date), MONTH) AS cohort_month,
      COUNT(DISTINCT user_id) AS cohort_size,
      DATE_DIFF(DATE_TRUNC(DATE(transaction_time), MONTH), DATE_TRUNC(DATE(first_purchase_date), MONTH), MONTH) AS months_since_cohort,
      COUNT(DISTINCT user_id) AS retained_users,
      SUM(total_price) AS total_spend,
      COUNT(DISTINCT transaction_id) AS total_orders
    FROM raw_transactions rt
    JOIN customer_metadata cm ON rt.user_id = cm.user_id
    WHERE NOT is_return
    GROUP BY cohort_month, months_since_cohort
    HAVING months_since_cohort >= 0
  )
  ORDER BY cohort_month, months_since_cohort
),

-- Predictive metrics (using simple heuristics for demonstration)
predictive_metrics AS (
  SELECT
    cs.user_id,
    cs.customer_segment,
    cs.last_purchase_date,
    cs.total_spend,
    cs.total_transactions,
    -- Simple churn prediction (heuristic)
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cs.last_purchase_date), DAY) > 180 THEN 'High Risk'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cs.last_purchase_date), DAY) > 90 THEN 'Medium Risk'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cs.last_purchase_date), DAY) > 30 THEN 'Low Risk'
      ELSE 'Active'
    END AS churn_risk,
    -- Next purchase prediction (heuristic)
    DATE_ADD(
      cs.last_purchase_date, 
      INTERVAL CAST(
        COALESCE(
          cpp.avg_days_between_purchases,
          CASE 
            WHEN cs.total_transactions = 1 THEN 90
            ELSE 30
          END
        ) AS INT64
      ) DAY
    ) AS predicted_next_purchase_date,
    -- Predicted next category (simple heuristic)
    COALESCE(
      cpp.most_common_next_category,
      CASE
        WHEN cs.total_transactions = 1 THEN 
          (SELECT product_category 
           FROM raw_transactions 
           WHERE user_id = cs.user_id 
           ORDER BY transaction_time DESC 
           LIMIT 1)
        ELSE NULL
      END
    ) AS predicted_next_category,
    -- Lifetime value projection (simple heuristic)
    CASE
      WHEN cs.customer_segment = 'Champions' THEN cs.total_spend * 1.5
      WHEN cs.customer_segment = 'Loyal Customers' THEN cs.total_spend * 1.2
      WHEN cs.customer_segment = 'Potential Loyalists' THEN cs.total_spend * 1.1
      WHEN cs.customer_segment = 'New Customers' THEN cs.total_spend * 0.8
      ELSE cs.total_spend * 0.5
    END AS projected_lifetime_value
  FROM customer_segments cs
  LEFT JOIN customer_purchase_patterns cpp ON cs.user_id = cpp.user_id
)

-- Final comprehensive analysis combining all metrics
SELECT
  cs.user_id,
  cs.first_purchase_date,
  cs.last_purchase_date,
  cs.total_transactions,
  cs.total_spend,
  cs.total_purchases,
  cs.total_items_returned,
  cs.distinct_categories_purchased,
  cs.distinct_purchase_days,
  cs.most_used_payment_method,
  cs.most_common_shipping_country,
  cs.most_common_device_type,
  cs.customer_segment,
  rfm.recency_days,
  rfm.frequency,
  rfm.monetary,
  rfm.recency_score,
  rfm.frequency_score,
  rfm.monetary_score,
  rfm.rfm_total_score,
  pm.unique_customers AS products_unique_customers,
  pm.total_quantity_sold AS products_total_quantity_sold,
  pm.total_revenue AS products_total_revenue,
  pm.total_quantity_returned AS products_total_quantity_returned,
  pm.total_revenue_returned AS products_total_revenue_returned,
  pm.transaction_count AS products_transaction_count,
  pm.avg_quantity_per_transaction AS products_avg_quantity,
  pm.avg_revenue_per_transaction AS products_avg_revenue,
  pm.return_rate AS products_return_rate,
  cm.unique_products AS category_unique_products,
  cm.category_quantity_sold,
  cm.category_revenue,
  cm.category_quantity_returned,
  cm.category_revenue_returned,
  cm.category_unique_customers,
  cm.category_transaction_count,
  cm.category_avg_quantity,
  cm.category_avg_revenue,
  cm.category_return_rate,
  cm.median_revenue_per_transaction,
  cpp.avg_days_between_purchases,
  cpp.most_common_next_category,
  mc.cohort_month,
  mc.cohort_size,
  mc.months_since_cohort,
  mc.retained_users,
  mc.retention_rate,
  mc.total_spend AS cohort_total_spend,
  mc.avg_spend_per_user,
  mc.total_orders AS cohort_total_orders,
  mc.avg_orders_per_user,
  pm.churn_risk,
  pm.predicted_next_purchase_date,
  pm.predicted_next_category,
  pm.projected_lifetime_value,
  -- Additional complex calculations
  SAFE_DIVIDE(cs.total_spend, cs.total_transactions) AS avg_spend_per_transaction,
  SAFE_DIVIDE(cs.total_spend, cs.distinct_purchase_days) AS avg_spend_per_day,
  SAFE_DIVIDE(cs.total_items_returned, cs.total_purchases) AS return_rate,
  -- Time since first purchase
  DATE_DIFF(CURRENT_DATE(), DATE(cs.first_purchase_date), DAY) AS days_since_first_purchase,
  -- Growth metrics
  CASE
    WHEN DATE_DIFF(DATE(cs.last_purchase_date), DATE(cs.first_purchase_date), DAY) = 0 THEN 0
    ELSE SAFE_DIVIDE(cs.total_spend, DATE_DIFF(DATE(cs.last_purchase_date), DATE(cs.first_purchase_date), DAY)) * 30
  END AS monthly_spend_rate,
  -- Seasonality flags
  CASE
    WHEN EXTRACT(MONTH FROM cs.last_purchase_date) IN (11, 12) THEN 'Holiday Season'
    WHEN EXTRACT(MONTH FROM cs.last_purchase_date) IN (6, 7) THEN 'Summer Season'
    ELSE 'Regular Season'
  END AS last_purchase_season,
  -- Customer value tier
  CASE
    WHEN cs.total_spend >= 1000 THEN 'Platinum'
    WHEN cs.total_spend >= 500 THEN 'Gold'
    WHEN cs.total_spend >= 200 THEN 'Silver'
    ELSE 'Bronze'
  END AS value_tier,
  -- Cross-category behavior
  CASE
    WHEN cs.distinct_categories_purchased >= 5 THEN 'Multi-Category'
    WHEN cs.distinct_categories_purchased >= 3 THEN 'Several Categories'
    WHEN cs.distinct_categories_purchased = 2 THEN 'Two Categories'
    ELSE 'Single Category'
  END AS category_behavior
FROM customer_segments cs
LEFT JOIN rfm_calculation rfm ON cs.user_id = rfm.user_id
LEFT JOIN (
  SELECT 
    user_id, 
    ARRAY_AGG(STRUCT(product_id, product_category, total_quantity_sold, total_revenue)) AS product_stats
  FROM raw_transactions rt
  JOIN product_metrics pm ON rt.product_id = pm.product_id
  GROUP BY user_id
) AS user_products ON cs.user_id = user_products.user_id
LEFT JOIN category_metrics cm ON 1=1  -- This would normally join on category
LEFT JOIN customer_purchase_patterns cpp ON cs.user_id = cpp.user_id
LEFT JOIN monthly_cohorts mc ON DATE_TRUNC(DATE(cs.first_purchase_date), MONTH) = mc.cohort_month
LEFT JOIN predictive_metrics pm ON cs.user_id = pm.user_id
ORDER BY cs.total_spend DESC, cs.total_transactions DESC
LIMIT 10000;