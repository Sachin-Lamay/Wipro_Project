-- =============================================================================
-- ADVANCED ANALYTICS QUERIES
-- CTEs · Window Functions · RFM · CLV · Funnel · Cohort
-- =============================================================================

USE SCHEMA ECOM_DW.CURATED;

-- ─── 1. RFM ANALYSIS ──────────────────────────────────────────────────────
WITH rfm_base AS (
    SELECT
        dc.customer_id,
        dc.full_name,
        dc.email,
        dc.country,
        dc.customer_segment,
        MAX(dd.full_date)                       AS last_order_date,
        COUNT(DISTINCT fo.order_id)             AS frequency,
        SUM(fo.net_revenue)                     AS monetary,
        DATEDIFF('day', MAX(dd.full_date), CURRENT_DATE) AS recency_days
    FROM FACT_ORDERS fo
    JOIN DIM_CUSTOMER dc ON fo.customer_sk = dc.customer_sk AND dc.is_current
    JOIN DIM_DATE     dd ON fo.order_date_key = dd.date_key
    WHERE fo.order_status NOT IN ('CANCELLED','REFUNDED')
    GROUP BY 1,2,3,4,5
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)     AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT *,
        CONCAT(r_score::VARCHAR, f_score::VARCHAR, m_score::VARCHAR) AS rfm_code,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                  THEN 'Promising'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
            WHEN r_score >= 3 AND m_score >= 4                  THEN 'Big Spenders'
            WHEN r_score <= 1                                   THEN 'Cant Lose Them'
            ELSE 'Needs Attention'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT * FROM rfm_segments ORDER BY monetary DESC;

-- ─── 2. CUSTOMER LIFETIME VALUE ───────────────────────────────────────────
WITH customer_orders AS (
    SELECT
        dc.customer_id,
        COUNT(DISTINCT fo.order_id)                    AS total_orders,
        SUM(fo.net_revenue)                            AS total_revenue,
        AVG(fo.net_revenue)                            AS avg_order_value,
        MIN(dd.full_date)                              AS first_order_date,
        MAX(dd.full_date)                              AS last_order_date,
        DATEDIFF('day', MIN(dd.full_date), MAX(dd.full_date)) AS lifespan_days
    FROM FACT_ORDERS fo
    JOIN DIM_CUSTOMER dc ON fo.customer_sk = dc.customer_sk
    JOIN DIM_DATE     dd ON fo.order_date_key = dd.date_key
    WHERE fo.order_status NOT IN ('CANCELLED','REFUNDED')
    GROUP BY 1
),
clv_calc AS (
    SELECT *,
        total_orders / NULLIF(CEIL(lifespan_days / 30.0), 0) AS purchase_frequency_monthly,
        -- Historical CLV
        total_revenue AS historical_clv,
        -- Predicted CLV (simplified BG/NBD proxy)
        avg_order_value
          * (total_orders / NULLIF(CEIL(lifespan_days / 30.0), 0))
          * 12
          * 0.65     AS predicted_clv_365d,          -- 0.65 = avg churn-adjusted retention
        avg_order_value
          * (total_orders / NULLIF(CEIL(lifespan_days / 30.0), 0))
          * 3
          * 0.65     AS predicted_clv_90d
    FROM customer_orders
)
SELECT *,
    CASE
        WHEN predicted_clv_365d >= 5000 THEN 'Platinum'
        WHEN predicted_clv_365d >= 2000 THEN 'Gold'
        WHEN predicted_clv_365d >= 500  THEN 'Silver'
        ELSE 'Bronze'
    END AS clv_tier
FROM clv_calc
ORDER BY predicted_clv_365d DESC;

-- ─── 3. COHORT RETENTION ANALYSIS ─────────────────────────────────────────
WITH cohort_base AS (
    SELECT
        dc.customer_id,
        DATE_TRUNC('month', MIN(dd.full_date))              AS cohort_month,
        DATE_TRUNC('month', dd.full_date)                   AS order_month
    FROM FACT_ORDERS fo
    JOIN DIM_CUSTOMER dc ON fo.customer_sk = dc.customer_sk
    JOIN DIM_DATE     dd ON fo.order_date_key = dd.date_key
    GROUP BY 1, 3
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_customers
    FROM cohort_base GROUP BY 1
),
cohort_activity AS (
    SELECT
        cb.cohort_month,
        DATEDIFF('month', cb.cohort_month, cb.order_month) AS period_number,
        COUNT(DISTINCT cb.customer_id) AS active_customers
    FROM cohort_base cb
    GROUP BY 1, 2
)
SELECT
    ca.cohort_month,
    ca.period_number,
    ca.active_customers,
    cs.cohort_customers,
    ROUND(ca.active_customers::FLOAT / cs.cohort_customers * 100, 1) AS retention_pct
FROM cohort_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
ORDER BY 1, 2;

-- ─── 4. REVENUE GROWTH & MOM/YOY ──────────────────────────────────────────
WITH monthly_revenue AS (
    SELECT
        dd.year,
        dd.month,
        dd.month_name,
        TO_CHAR(dd.full_date, 'YYYY-MM')         AS period,
        SUM(fo.net_revenue)                       AS revenue,
        COUNT(DISTINCT fo.order_sk)               AS orders,
        COUNT(DISTINCT fo.customer_sk)            AS customers,
        AVG(fo.net_revenue)                       AS aov
    FROM FACT_ORDERS fo
    JOIN DIM_DATE dd ON fo.order_date_key = dd.date_key
    WHERE fo.order_status NOT IN ('CANCELLED','REFUNDED')
    GROUP BY 1,2,3,4
)
SELECT
    period,
    year,
    month_name,
    revenue,
    orders,
    customers,
    ROUND(aov, 2)                                                         AS avg_order_value,
    LAG(revenue) OVER (ORDER BY year, month)                              AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY year, month))
          / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100, 2) AS mom_growth_pct,
    LAG(revenue, 12) OVER (ORDER BY year, month)                          AS prev_year_revenue,
    ROUND((revenue - LAG(revenue, 12) OVER (ORDER BY year, month))
          / NULLIF(LAG(revenue, 12) OVER (ORDER BY year, month), 0) * 100, 2) AS yoy_growth_pct,
    SUM(revenue) OVER (PARTITION BY year ORDER BY month
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ytd_revenue
FROM monthly_revenue
ORDER BY year, month;

-- ─── 5. PRODUCT PERFORMANCE WITH WINDOW FUNCTIONS ─────────────────────────
WITH product_sales AS (
    SELECT
        dp.product_id,
        dp.product_name,
        dc_cat.category_name,
        dp.brand,
        SUM(foi.quantity)                         AS units_sold,
        SUM(foi.line_revenue)                     AS revenue,
        SUM(foi.line_margin)                      AS margin,
        AVG(fr.rating)                            AS avg_rating,
        COUNT(DISTINCT fr.review_sk)              AS review_count
    FROM FACT_ORDER_ITEMS foi
    JOIN DIM_PRODUCT dp    ON foi.product_sk = dp.product_sk AND dp.is_current
    JOIN DIM_CATEGORY dc_cat ON dp.category_sk = dc_cat.category_sk
    LEFT JOIN FACT_REVIEWS fr ON foi.product_sk = fr.product_sk
    GROUP BY 1,2,3,4
)
SELECT
    product_id,
    product_name,
    category_name,
    brand,
    units_sold,
    ROUND(revenue, 2)                                                   AS revenue,
    ROUND(margin, 2)                                                    AS margin,
    ROUND(margin / NULLIF(revenue, 0) * 100, 2)                         AS margin_pct,
    ROUND(avg_rating, 2)                                                AS avg_rating,
    review_count,
    RANK()        OVER (ORDER BY revenue DESC)                          AS revenue_rank,
    RANK()        OVER (PARTITION BY category_name ORDER BY revenue DESC) AS rank_in_category,
    PERCENT_RANK() OVER (ORDER BY revenue DESC)                         AS revenue_percentile,
    SUM(revenue) OVER (ORDER BY revenue DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
    SUM(revenue) OVER ()                                                AS total_revenue,
    ROUND(SUM(revenue) OVER (ORDER BY revenue DESC
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          / NULLIF(SUM(revenue) OVER (), 0) * 100, 2)                  AS cum_revenue_pct
FROM product_sales
ORDER BY revenue DESC;

-- ─── 6. CONVERSION FUNNEL BY CHANNEL ──────────────────────────────────────
WITH funnel_steps AS (
    SELECT
        ch.channel_name,
        fc.event_type,
        COUNT(DISTINCT fc.session_id)  AS session_count
    FROM ECOM_DW.CURATED.FACT_CLICKSTREAM fc
    LEFT JOIN ECOM_DW.CURATED.DIM_CHANNEL ch ON fc.event_date_key = ch.channel_sk
    WHERE fc.event_type IN ('page_view','product_view','add_to_cart','checkout','purchase')
    GROUP BY 1, 2
),
pivoted AS (
    SELECT
        channel_name,
        MAX(CASE WHEN event_type = 'page_view'    THEN session_count END) AS page_views,
        MAX(CASE WHEN event_type = 'product_view' THEN session_count END) AS product_views,
        MAX(CASE WHEN event_type = 'add_to_cart'  THEN session_count END) AS add_to_cart,
        MAX(CASE WHEN event_type = 'checkout'     THEN session_count END) AS checkouts,
        MAX(CASE WHEN event_type = 'purchase'     THEN session_count END) AS purchases
    FROM funnel_steps GROUP BY 1
)
SELECT *,
    ROUND(product_views::FLOAT / NULLIF(page_views,0) * 100, 2)   AS pv_to_view_pct,
    ROUND(add_to_cart::FLOAT  / NULLIF(product_views,0) * 100, 2) AS view_to_atc_pct,
    ROUND(checkouts::FLOAT    / NULLIF(add_to_cart,0) * 100, 2)   AS atc_to_checkout_pct,
    ROUND(purchases::FLOAT    / NULLIF(checkouts,0) * 100, 2)     AS checkout_to_purchase_pct,
    ROUND(purchases::FLOAT    / NULLIF(page_views,0) * 100, 2)    AS overall_conversion_pct
FROM pivoted;

-- ─── 7. SEMI-STRUCTURED: CLICKSTREAM JSON QUERIES ─────────────────────────
-- Parse VARIANT clickstream payload
SELECT
    event_id,
    raw_payload:event_type::STRING         AS event_type,
    raw_payload:user_agent:device::STRING  AS device,
    raw_payload:geo:country::STRING        AS country,
    raw_payload:product:id::STRING         AS product_id,
    raw_payload:product:price::FLOAT       AS product_price,
    raw_payload:cart:items::ARRAY          AS cart_items,
    ARRAY_SIZE(raw_payload:cart:items)     AS cart_size,
    raw_payload:session:duration_seconds::INTEGER AS session_duration
FROM ECOM_DW.CURATED.FACT_CLICKSTREAM
WHERE raw_payload IS NOT NULL
LIMIT 1000;

-- ─── 8. PERFORMANCE: QUERY TUNING INDEX CHECK ─────────────────────────────
-- Check clustering depth
SELECT SYSTEM$CLUSTERING_INFORMATION('ECOM_DW.CURATED.FACT_ORDERS', '(order_date_key, customer_sk)');
SELECT SYSTEM$CLUSTERING_INFORMATION('ECOM_DW.CURATED.FACT_CLICKSTREAM', '(event_date_key, event_type)');

-- Query profile helper
-- ALTER SESSION SET USE_CACHED_RESULT = FALSE;
