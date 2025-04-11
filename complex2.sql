-- This complex BigQuery SQL query analyzes e-commerce data across multiple dimensions
-- including customer behavior, product performance, and sales trends

-- Configuration settings
DECLARE analysis_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);
DECLARE analysis_end_date DATE DEFAULT CURRENT_DATE();
DECLARE min_purchase_threshold NUMERIC DEFAULT 10.00;
DECLARE high_value_customer_threshold NUMERIC DEFAULT 1000.00;
DECLARE customer_segment_count INT64 DEFAULT 5;

-- Initial data extraction: Customer base information
WITH customer_base AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.registration_date,
        c.country,
        c.city,
        c.postal_code,
        DATE_DIFF(CURRENT_DATE(), c.registration_date, DAY) AS customer_tenure_days,
        CASE
            WHEN c.registration_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'New'
            WHEN c.registration_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'Regular'
            ELSE 'Loyal'
        END AS customer_category,
        COALESCE(cs.subscription_tier, 'None') AS subscription_tier,
        COALESCE(cs.subscription_start_date, NULL) AS subscription_start_date,
        COALESCE(cs.subscription_end_date, NULL) AS subscription_end_date,
        COALESCE(cs.monthly_fee, 0) AS subscription_monthly_fee
    FROM
        `project.dataset.customers` c
    LEFT JOIN
        `project.dataset.customer_subscriptions` cs
    ON
        c.customer_id = cs.customer_id
        AND CURRENT_DATE() BETWEEN cs.subscription_start_date AND COALESCE(cs.subscription_end_date, DATE('9999-12-31'))
),

-- Order history with detailed metrics
order_history AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.total_amount,
        o.discount_amount,
        o.shipping_cost,
        o.tax_amount,
        (o.total_amount - o.discount_amount + o.shipping_cost + o.tax_amount) AS final_amount,
        o.payment_method,
        o.order_status,
        o.delivery_address_id,
        o.shipping_method,
        o.estimated_delivery_date,
        o.actual_delivery_date,
        CASE
            WHEN o.actual_delivery_date IS NOT NULL THEN
                DATE_DIFF(o.actual_delivery_date, o.order_date, DAY)
            ELSE
                NULL
        END AS delivery_time_days,
        CASE
            WHEN o.actual_delivery_date IS NOT NULL AND o.estimated_delivery_date IS NOT NULL THEN
                DATE_DIFF(o.actual_delivery_date, o.estimated_delivery_date, DAY)
            ELSE
                NULL
        END AS delivery_delay_days,
        CASE
            WHEN o.order_status = 'Cancelled' THEN 1
            ELSE 0
        END AS is_cancelled,
        CASE
            WHEN o.order_status = 'Returned' THEN 1
            ELSE 0
        END AS is_returned,
        TIMESTAMP_DIFF(o.order_timestamp, o.cart_creation_timestamp, MINUTE) AS decision_time_minutes,
        (SELECT COUNT(DISTINCT session_id) FROM `project.dataset.user_sessions` us 
         WHERE us.customer_id = o.customer_id AND us.session_timestamp BETWEEN 
         TIMESTAMP_SUB(o.order_timestamp, INTERVAL 30 DAY) AND o.order_timestamp) AS sessions_before_purchase
    FROM
        `project.dataset.orders` o
    WHERE
        o.order_date BETWEEN analysis_start_date AND analysis_end_date
),

-- Order items with product details
order_items_detail AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_percentage,
        oi.unit_price * oi.quantity AS gross_item_amount,
        oi.unit_price * oi.quantity * (1 - oi.discount_percentage/100) AS net_item_amount,
        p.product_name,
        p.category_id,
        p.subcategory_id,
        p.brand_id,
        p.supplier_id,
        p.unit_cost,
        (oi.unit_price * (1 - oi.discount_percentage/100) - p.unit_cost) * oi.quantity AS item_profit,
        ((oi.unit_price * (1 - oi.discount_percentage/100) - p.unit_cost) / p.unit_cost) * 100 AS profit_margin_percentage,
        p.weight_kg * oi.quantity AS total_weight_kg,
        p.is_perishable,
        p.shelf_life_days,
        pc.category_name,
        psc.subcategory_name,
        b.brand_name,
        s.supplier_name,
        s.country AS supplier_country
    FROM
        `project.dataset.order_items` oi
    JOIN
        `project.dataset.products` p ON oi.product_id = p.product_id
    JOIN
        `project.dataset.product_categories` pc ON p.category_id = pc.category_id
    JOIN
        `project.dataset.product_subcategories` psc ON p.subcategory_id = psc.subcategory_id
    JOIN
        `project.dataset.brands` b ON p.brand_id = b.brand_id
    JOIN
        `project.dataset.suppliers` s ON p.supplier_id = s.supplier_id
    JOIN
        order_history oh ON oi.order_id = oh.order_id
),

-- Customer purchase patterns
customer_purchase_patterns AS (
    SELECT
        oh.customer_id,
        COUNT(DISTINCT oh.order_id) AS total_orders,
        SUM(oh.final_amount) AS total_spent,
        AVG(oh.final_amount) AS average_order_value,
        MIN(oh.order_date) AS first_purchase_date,
        MAX(oh.order_date) AS last_purchase_date,
        DATE_DIFF(MAX(oh.order_date), MIN(oh.order_date), DAY) AS customer_lifespan_days,
        CASE
            WHEN COUNT(DISTINCT oh.order_id) > 0 THEN
                DATE_DIFF(MAX(oh.order_date), MIN(oh.order_date), DAY) / NULLIF(COUNT(DISTINCT oh.order_id) - 1, 0)
            ELSE
                NULL
        END AS average_days_between_orders,
        SUM(CASE WHEN oh.is_cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(CASE WHEN oh.is_returned = 1 THEN 1 ELSE 0 END) AS returned_orders,
        ARRAY_AGG(DISTINCT oh.payment_method IGNORE NULLS) AS payment_methods_used,
        (SELECT COUNT(DISTINCT oid.product_id) 
         FROM order_items_detail oid 
         WHERE oid.order_id IN (SELECT order_id FROM order_history WHERE customer_id = oh.customer_id)
        ) AS unique_products_purchased,
        (SELECT COUNT(DISTINCT oid.category_id) 
         FROM order_items_detail oid 
         WHERE oid.order_id IN (SELECT order_id FROM order_history WHERE customer_id = oh.customer_id)
        ) AS unique_categories_purchased,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), MAX(oh.order_date), DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(CURRENT_DATE(), MAX(oh.order_date), DAY) BETWEEN 31 AND 90 THEN 'At Risk'
            WHEN DATE_DIFF(CURRENT_DATE(), MAX(oh.order_date), DAY) BETWEEN 91 AND 180 THEN 'Lapsed'
            ELSE 'Churned'
        END AS recency_segment,
        NTILE(customer_segment_count) OVER (ORDER BY SUM(oh.final_amount) ASC) AS monetary_segment,
        NTILE(customer_segment_count) OVER (ORDER BY COUNT(DISTINCT oh.order_id) ASC) AS frequency_segment
    FROM
        order_history oh
    GROUP BY
        oh.customer_id
),

-- Product performance metrics
product_performance AS (
    SELECT
        oid.product_id,
        oid.product_name,
        oid.category_id,
        oid.category_name,
        oid.subcategory_id,
        oid.subcategory_name,
        oid.brand_id,
        oid.brand_name,
        oid.supplier_id,
        oid.supplier_name,
        COUNT(DISTINCT oid.order_id) AS order_count,
        SUM(oid.quantity) AS total_quantity_sold,
        SUM(oid.gross_item_amount) AS gross_revenue,
        SUM(oid.net_item_amount) AS net_revenue,
        SUM(oid.item_profit) AS total_profit,
        AVG(oid.profit_margin_percentage) AS average_profit_margin,
        (SELECT COUNT(DISTINCT oh.customer_id) 
         FROM order_history oh 
         JOIN order_items_detail oid2 ON oh.order_id = oid2.order_id 
         WHERE oid2.product_id = oid.product_id
        ) AS unique_customers,
        (SELECT AVG(review_rating) 
         FROM `project.dataset.product_reviews` pr 
         WHERE pr.product_id = oid.product_id AND pr.review_date BETWEEN analysis_start_date AND analysis_end_date
        ) AS average_rating,
        (SELECT COUNT(*) 
         FROM `project.dataset.product_reviews` pr 
         WHERE pr.product_id = oid.product_id AND pr.review_date BETWEEN analysis_start_date AND analysis_end_date
        ) AS review_count,
        (SELECT SUM(CASE WHEN inventory_level <= reorder_point THEN 1 ELSE 0 END) 
         FROM `project.dataset.inventory` i 
         WHERE i.product_id = oid.product_id AND i.warehouse_id IN 
         (SELECT warehouse_id FROM `project.dataset.warehouses` WHERE is_active = TRUE)
        ) AS low_stock_warehouses,
        RANK() OVER (PARTITION BY oid.category_id ORDER BY SUM(oid.net_item_amount) DESC) AS category_revenue_rank,
        PERCENT_RANK() OVER (ORDER BY SUM(oid.net_item_amount) DESC) AS overall_revenue_percentile
    FROM
        order_items_detail oid
    GROUP BY
        oid.product_id,
        oid.product_name,
        oid.category_id,
        oid.category_name,
        oid.subcategory_id,
        oid.subcategory_name,
        oid.brand_id,
        oid.brand_name,
        oid.supplier_id,
        oid.supplier_name
),

-- Time-based sales analysis
time_based_sales AS (
    SELECT
        DATE_TRUNC(oh.order_date, MONTH) AS month,
        oid.category_id,
        oid.category_name,
        SUM(oid.net_item_amount) AS monthly_revenue,
        COUNT(DISTINCT oh.order_id) AS monthly_order_count,
        COUNT(DISTINCT oh.customer_id) AS monthly_customer_count,
        SUM(oid.quantity) AS monthly_units_sold,
        SUM(oid.item_profit) AS monthly_profit,
        LAG(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH)) AS prev_month_revenue,
        CASE
            WHEN LAG(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH)) IS NOT NULL THEN
                (SUM(oid.net_item_amount) - LAG(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH))) / 
                NULLIF(LAG(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH)), 0) * 100
            ELSE
                NULL
        END AS month_over_month_growth,
        AVG(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS three_month_moving_avg,
        SUM(SUM(oid.net_item_amount)) OVER (PARTITION BY oid.category_id ORDER BY DATE_TRUNC(oh.order_date, MONTH) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
    FROM
        order_history oh
    JOIN
        order_items_detail oid ON oh.order_id = oid.order_id
    GROUP BY
        DATE_TRUNC(oh.order_date, MONTH),
        oid.category_id,
        oid.category_name
),

-- Geographic sales distribution
geographic_sales AS (
    SELECT
        cb.country,
        cb.city,
        COUNT(DISTINCT oh.order_id) AS total_orders,
        COUNT(DISTINCT oh.customer_id) AS total_customers,
        SUM(oh.final_amount) AS total_revenue,
        AVG(oh.final_amount) AS average_order_value,
        SUM(oh.shipping_cost) AS total_shipping_cost,
        AVG(oh.shipping_cost) AS average_shipping_cost,
        SUM(CASE WHEN oh.delivery_delay_days > 0 THEN 1 ELSE 0 END) AS delayed_deliveries,
        AVG(CASE WHEN oh.delivery_delay_days > 0 THEN oh.delivery_delay_days ELSE NULL END) AS average_delay_days,
        (SELECT category_name 
         FROM (
             SELECT oid.category_name, SUM(oid.net_item_amount) AS cat_revenue,
             ROW_NUMBER() OVER (ORDER BY SUM(oid.net_item_amount) DESC) AS rn
             FROM order_items_detail oid
             JOIN order_history oh2 ON oid.order_id = oh2.order_id
             JOIN customer_base cb2 ON oh2.customer_id = cb2.customer_id
             WHERE cb2.country = cb.country AND cb2.city = cb.city
             GROUP BY oid.category_name
         ) WHERE rn = 1
        ) AS top_category,
        (SELECT brand_name 
         FROM (
             SELECT oid.brand_name, SUM(oid.net_item_amount) AS brand_revenue,
             ROW_NUMBER() OVER (ORDER BY SUM(oid.net_item_amount) DESC) AS rn
             FROM order_items_detail oid
             JOIN order_history oh2 ON oid.order_id = oh2.order_id
             JOIN customer_base cb2 ON oh2.customer_id = cb2.customer_id
             WHERE cb2.country = cb.country AND cb2.city = cb.city
             GROUP BY oid.brand_name
         ) WHERE rn = 1
        ) AS top_brand
    FROM
        customer_base cb
    JOIN
        order_history oh ON cb.customer_id = oh.customer_id
    GROUP BY
        cb.country,
        cb.city
),

-- Customer segmentation based on RFM analysis
customer_rfm AS (
    SELECT
        cpp.customer_id,
        cpp.recency_segment,
        cpp.frequency_segment,
        cpp.monetary_segment,
        CONCAT(cpp.recency_segment, '-', cpp.frequency_segment, '-', cpp.monetary_segment) AS rfm_segment,
        CASE
            WHEN cpp.recency_segment = 'Active' AND cpp.monetary_segment >= 4 AND cpp.frequency_segment >= 4 THEN 'Champions'
            WHEN cpp.recency_segment = 'Active' AND cpp.monetary_segment >= 3 AND cpp.frequency_segment >= 3 THEN 'Loyal Customers'
            WHEN cpp.recency_segment = 'Active' AND cpp.monetary_segment <= 2 AND cpp.frequency_segment <= 2 THEN 'Promising'
            WHEN cpp.recency_segment = 'At Risk' AND cpp.monetary_segment >= 4 AND cpp.frequency_segment >= 4 THEN 'At Risk Champions'
            WHEN cpp.recency_segment IN ('Lapsed', 'Churned') AND cpp.monetary_segment >= 4 AND cpp.frequency_segment >= 4 THEN 'Lost Champions'
            WHEN cpp.recency_segment IN ('Lapsed', 'Churned') AND cpp.monetary_segment <= 2 AND cpp.frequency_segment <= 2 THEN 'Lost Cause'
            ELSE 'Needs Attention'
        END AS customer_segment,
        cpp.total_orders,
        cpp.total_spent,
        cpp.average_order_value,
        cpp.first_purchase_date,
        cpp.last_purchase_date,
        cpp.customer_lifespan_days,
        cpp.average_days_between_orders,
        cpp.cancelled_orders,
        cpp.returned_orders,
        cpp.unique_products_purchased,
        cpp.unique_categories_purchased,
        (SELECT AVG(review_rating) 
         FROM `project.dataset.product_reviews` pr 
         WHERE pr.customer_id = cpp.customer_id AND pr.review_date BETWEEN analysis_start_date AND analysis_end_date
        ) AS average_review_rating,
        (SELECT COUNT(*) 
         FROM `project.dataset.product_reviews` pr 
         WHERE pr.customer_id = cpp.customer_id AND pr.review_date BETWEEN analysis_start_date AND analysis_end_date
        ) AS review_count,
        (SELECT COUNT(*) 
         FROM `project.dataset.customer_support_tickets` cst 
         WHERE cst.customer_id = cpp.customer_id AND cst.created_date BETWEEN analysis_start_date AND analysis_end_date
        ) AS support_ticket_count,
        (SELECT AVG(satisfaction_score) 
         FROM `project.dataset.customer_support_tickets` cst 
         WHERE cst.customer_id = cpp.customer_id AND cst.created_date BETWEEN analysis_start_date AND analysis_end_date
