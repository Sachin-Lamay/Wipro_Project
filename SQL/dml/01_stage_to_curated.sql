-- =============================================================================
-- DML: MERGE / UPSERT patterns for Stage → Curated
-- =============================================================================

USE SCHEMA ECOM_DW.CURATED;

-- ─── POPULATE DIM_DATE (generate 5-year calendar) ─────────────────────────
INSERT INTO DIM_DATE
WITH date_spine AS (
    SELECT DATEADD(day, seq4(), '2020-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1825))
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'))          AS date_key,
    full_date,
    YEAR(full_date)                                    AS year,
    QUARTER(full_date)                                 AS quarter,
    MONTH(full_date)                                   AS month,
    MONTHNAME(full_date)                               AS month_name,
    WEEKOFYEAR(full_date)                              AS week_of_year,
    DAY(full_date)                                     AS day_of_month,
    DAYOFWEEK(full_date)                               AS day_of_week,
    DAYNAME(full_date)                                 AS day_name,
    DAYOFWEEK(full_date) IN (0,6)                      AS is_weekend,
    FALSE                                              AS is_holiday,
    YEAR(full_date)                                    AS fiscal_year,
    QUARTER(full_date)                                 AS fiscal_quarter
FROM date_spine;

-- ─── MERGE: STG_CUSTOMERS → DIM_CUSTOMER (SCD Type 2) ─────────────────────
MERGE INTO DIM_CUSTOMER tgt
USING (
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        TRIM(s.first_name || ' ' || s.last_name) AS full_name,
        s.email,
        s.phone,
        s.date_of_birth,
        CASE
            WHEN DATEDIFF('year', s.date_of_birth, CURRENT_DATE) < 25  THEN '18-24'
            WHEN DATEDIFF('year', s.date_of_birth, CURRENT_DATE) < 35  THEN '25-34'
            WHEN DATEDIFF('year', s.date_of_birth, CURRENT_DATE) < 45  THEN '35-44'
            WHEN DATEDIFF('year', s.date_of_birth, CURRENT_DATE) < 55  THEN '45-54'
            ELSE '55+'
        END AS age_group,
        s.gender,
        s.country,
        s.city,
        s.zip_code,
        s.registration_date::DATE AS registration_date,
        s.segment AS customer_segment
    FROM ECOM_DW.STAGE.STG_CUSTOMERS s
    WHERE s._load_timestamp >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
) src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
WHEN MATCHED AND (
    tgt.email <> src.email OR tgt.city <> src.city OR tgt.customer_segment <> src.customer_segment
) THEN UPDATE SET
    tgt.is_current = FALSE,
    tgt.effective_end_date = CURRENT_DATE,
    tgt.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, full_name, email, phone,
    date_of_birth, age_group, gender, country, city, zip_code,
    registration_date, customer_segment, effective_start_date, is_current
) VALUES (
    src.customer_id, src.first_name, src.last_name, src.full_name, src.email,
    src.phone, src.date_of_birth, src.age_group, src.gender, src.country,
    src.city, src.zip_code, src.registration_date, src.customer_segment,
    CURRENT_DATE, TRUE
);

-- ─── MERGE: STG_PRODUCTS → DIM_PRODUCT ─────────────────────────────────────
MERGE INTO DIM_PRODUCT tgt
USING (
    SELECT
        p.product_id, p.product_name, c.category_sk,
        p.brand, p.sku, p.unit_price, p.cost_price,
        ROUND((p.unit_price - p.cost_price) / NULLIF(p.unit_price, 0) * 100, 2) AS gross_margin_pct,
        p.weight_kg, p.is_active, p.launch_date
    FROM ECOM_DW.STAGE.STG_PRODUCTS p
    LEFT JOIN ECOM_DW.CURATED.DIM_CATEGORY c ON p.category_id = c.category_id
) src
ON tgt.product_id = src.product_id AND tgt.is_current = TRUE
WHEN MATCHED AND (tgt.unit_price <> src.unit_price OR tgt.is_active <> src.is_active) THEN
    UPDATE SET tgt.is_current = FALSE, tgt.effective_end_date = CURRENT_DATE
WHEN NOT MATCHED THEN INSERT (
    product_id, product_name, category_sk, brand, sku, unit_price, cost_price,
    gross_margin_pct, weight_kg, is_active, launch_date, effective_start_date, is_current
) VALUES (
    src.product_id, src.product_name, src.category_sk, src.brand, src.sku,
    src.unit_price, src.cost_price, src.gross_margin_pct, src.weight_kg,
    src.is_active, src.launch_date, CURRENT_DATE, TRUE
);

-- ─── INSERT: FACT_ORDERS from Stage ────────────────────────────────────────
INSERT INTO FACT_ORDERS (
    order_id, customer_sk, order_date_key, channel_sk, geo_sk,
    order_status, shipping_method, gross_revenue, discount_amount,
    shipping_cost, tax_amount, net_revenue, item_count, currency
)
SELECT
    so.order_id,
    dc.customer_sk,
    TO_NUMBER(TO_CHAR(so.order_date::DATE, 'YYYYMMDD'))  AS order_date_key,
    ch.channel_sk,
    geo.geo_sk,
    so.status,
    so.shipping_method,
    so.total_amount,
    so.discount_amount,
    so.shipping_cost,
    so.tax_amount,
    so.total_amount - so.discount_amount - so.shipping_cost AS net_revenue,
    COUNT(oi.order_item_id) AS item_count,
    so.currency
FROM ECOM_DW.STAGE.STG_ORDERS so
JOIN ECOM_DW.CURATED.DIM_CUSTOMER dc ON so.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN ECOM_DW.CURATED.DIM_CHANNEL ch ON so.channel = ch.channel_name
LEFT JOIN ECOM_DW.CURATED.DIM_GEOGRAPHY geo
       ON dc.country = geo.country AND dc.city = geo.city
LEFT JOIN ECOM_DW.STAGE.STG_ORDER_ITEMS oi ON so.order_id = oi.order_id
WHERE so.order_id NOT IN (SELECT order_id FROM ECOM_DW.CURATED.FACT_ORDERS)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,14;

-- ─── REFRESH CONSUMPTION: AGG_DAILY_REVENUE ────────────────────────────────
INSERT OVERWRITE INTO ECOM_DW.CONSUMPTION.AGG_DAILY_REVENUE
SELECT
    dd.date_key,
    dd.full_date           AS report_date,
    ch.channel_name        AS channel,
    geo.country,
    COUNT(fo.order_sk)     AS total_orders,
    SUM(fo.gross_revenue)  AS total_revenue,
    SUM(fo.discount_amount) AS total_discounts,
    SUM(fo.tax_amount)     AS total_tax,
    SUM(fo.net_revenue)    AS net_revenue,
    AVG(fo.net_revenue)    AS avg_order_value,
    COUNT(DISTINCT CASE WHEN dc.registration_date = dd.full_date THEN fo.customer_sk END) AS new_customers,
    COUNT(DISTINCT CASE WHEN dc.registration_date < dd.full_date  THEN fo.customer_sk END) AS returning_customers,
    NULL::FLOAT            AS conversion_rate    -- populated from clickstream join
FROM ECOM_DW.CURATED.FACT_ORDERS fo
JOIN ECOM_DW.CURATED.DIM_DATE     dd  ON fo.order_date_key = dd.date_key
JOIN ECOM_DW.CURATED.DIM_CUSTOMER dc  ON fo.customer_sk = dc.customer_sk
LEFT JOIN ECOM_DW.CURATED.DIM_CHANNEL  ch  ON fo.channel_sk = ch.channel_sk
LEFT JOIN ECOM_DW.CURATED.DIM_GEOGRAPHY geo ON fo.geo_sk = geo.geo_sk
GROUP BY 1,2,3,4;
