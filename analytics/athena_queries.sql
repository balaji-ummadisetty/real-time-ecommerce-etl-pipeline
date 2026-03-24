-- =============================================================================
-- athena_queries.sql
-- Ad-hoc analytics queries for the e-commerce data lake (Athena / Glue catalog)
-- =============================================================================

-- Set the workgroup and output location before running:
--   Workgroup  : ecommerce-analytics
--   Output S3  : s3://YOUR-BUCKET/athena-results/


-- =============================================================================
-- 1. DAILY REVENUE TREND (last 30 days)
-- =============================================================================
SELECT
    date,
    country_code,
    payment_currency,
    COUNT(DISTINCT order_id)                         AS orders,
    ROUND(SUM(order_total), 2)                       AS gross_revenue,
    ROUND(AVG(order_total), 2)                       AS avg_order_value,
    COUNT(DISTINCT user_id)                          AS unique_buyers,
    SUM(item_count)                                  AS units_sold
FROM ecommerce_processed.purchases
WHERE payment_status = 'success'
  AND date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2, 3
ORDER BY date DESC, gross_revenue DESC;


-- =============================================================================
-- 2. TOP 10 PRODUCTS BY REVENUE (current month)
-- =============================================================================
SELECT
    product_id,
    product_name,
    category,
    brand,
    SUM(quantity)       AS total_units_sold,
    ROUND(SUM(item_revenue), 2)  AS total_revenue,
    ROUND(AVG(unit_price), 2)    AS avg_unit_price,
    ROUND(AVG(discount_pct) * 100, 1) AS avg_discount_pct
FROM ecommerce_processed.order_items
WHERE payment_status = 'success'
  AND year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY 1, 2, 3, 4
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 3. CONVERSION FUNNEL (last 7 days)
-- =============================================================================
WITH funnel_steps AS (
    SELECT
        date,
        event_type,
        COUNT(DISTINCT session_id) AS sessions
    FROM ecommerce_processed.user_activity
    WHERE date >= DATE_ADD('day', -7, CURRENT_DATE)
      AND event_type IN ('page_view','product_view','add_to_cart','checkout_start')
    GROUP BY 1, 2
),
pivot AS (
    SELECT
        date,
        MAX(CASE WHEN event_type = 'page_view'       THEN sessions END) AS page_views,
        MAX(CASE WHEN event_type = 'product_view'    THEN sessions END) AS product_views,
        MAX(CASE WHEN event_type = 'add_to_cart'     THEN sessions END) AS add_to_cart,
        MAX(CASE WHEN event_type = 'checkout_start'  THEN sessions END) AS checkout_start
    FROM funnel_steps
    GROUP BY date
)
SELECT
    date,
    page_views,
    product_views,
    add_to_cart,
    checkout_start,
    ROUND(product_views   * 100.0 / NULLIF(page_views,    0), 2) AS page_to_pdp_pct,
    ROUND(add_to_cart     * 100.0 / NULLIF(product_views, 0), 2) AS pdp_to_cart_pct,
    ROUND(checkout_start  * 100.0 / NULLIF(add_to_cart,   0), 2) AS cart_to_checkout_pct
FROM pivot
ORDER BY date DESC;


-- =============================================================================
-- 4. REVENUE BY DEVICE TYPE & CHANNEL (last 14 days)
-- =============================================================================
SELECT
    p.device_type,
    a.channel,
    COUNT(DISTINCT p.order_id)          AS orders,
    ROUND(SUM(p.order_total), 2)        AS revenue,
    ROUND(AVG(p.order_total), 2)        AS avg_order_value
FROM ecommerce_processed.purchases p
JOIN ecommerce_processed.user_activity a
    ON p.session_id = a.session_id
   AND a.event_type = 'checkout_start'
WHERE p.payment_status = 'success'
  AND p.date >= DATE_ADD('day', -14, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY revenue DESC;


-- =============================================================================
-- 5. CUSTOMER SEGMENTATION SUMMARY
-- =============================================================================
SELECT
    user_segment,
    COUNT(DISTINCT user_id)             AS user_count,
    ROUND(AVG(customer_order_count), 1) AS avg_orders,
    ROUND(AVG(customer_total_spend), 2) AS avg_ltv,
    ROUND(SUM(order_total), 2)          AS segment_revenue
FROM ecommerce_processed.purchases
WHERE payment_status = 'success'
  AND date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY user_segment
ORDER BY segment_revenue DESC;


-- =============================================================================
-- 6. PAYMENT METHOD BREAKDOWN
-- =============================================================================
SELECT
    payment_method,
    payment_currency,
    COUNT(order_id)                         AS total_orders,
    SUM(CASE WHEN payment_status = 'success' THEN 1 ELSE 0 END)  AS successful,
    SUM(CASE WHEN payment_status = 'failed'  THEN 1 ELSE 0 END)  AS failed,
    ROUND(SUM(CASE WHEN payment_status = 'failed' THEN 1.0 ELSE 0 END)
          / COUNT(order_id) * 100, 2)        AS failure_rate_pct,
    ROUND(SUM(CASE WHEN payment_status = 'success' THEN order_total ELSE 0 END), 2) AS gross_revenue
FROM ecommerce_processed.purchases
WHERE date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY gross_revenue DESC;


-- =============================================================================
-- 7. GEOGRAPHIC REVENUE HEATMAP
-- =============================================================================
SELECT
    country,
    country_code,
    city,
    COUNT(DISTINCT order_id)        AS orders,
    ROUND(SUM(order_total), 2)      AS revenue,
    COUNT(DISTINCT user_id)         AS buyers,
    ROUND(AVG(order_total), 2)      AS avg_order_value
FROM ecommerce_processed.purchases
WHERE payment_status = 'success'
  AND year  = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY 1, 2, 3
ORDER BY revenue DESC
LIMIT 50;


-- =============================================================================
-- 8. HOURLY TRAFFIC PATTERN (last 48 hours)
-- =============================================================================
SELECT
    date,
    hour,
    event_type,
    COUNT(event_id)              AS events,
    COUNT(DISTINCT session_id)   AS sessions,
    COUNT(DISTINCT user_id)      AS users,
    ROUND(AVG(session_duration_sec) / 60.0, 2) AS avg_session_min
FROM ecommerce_processed.user_activity
WHERE date >= DATE_ADD('day', -2, CURRENT_DATE)
GROUP BY 1, 2, 3
ORDER BY date DESC, hour DESC, events DESC;


-- =============================================================================
-- 9. COUPON / DISCOUNT EFFECTIVENESS
-- =============================================================================
SELECT
    coupon_code,
    COUNT(DISTINCT order_id)            AS orders_used,
    ROUND(AVG(discount_amount), 2)      AS avg_discount,
    ROUND(AVG(effective_discount_pct) * 100, 1) AS avg_discount_pct,
    ROUND(SUM(order_total), 2)          AS total_revenue_after_discount,
    ROUND(SUM(subtotal), 2)             AS total_pre_discount_revenue
FROM ecommerce_processed.purchases
WHERE payment_status = 'success'
  AND coupon_code IS NOT NULL
  AND date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY coupon_code
HAVING COUNT(DISTINCT order_id) > 5
ORDER BY total_revenue_after_discount DESC;


-- =============================================================================
-- 10. AB TEST RESULTS
-- =============================================================================
SELECT
    ab_test_variant,
    COUNT(DISTINCT session_id)                  AS sessions,
    COUNT(DISTINCT user_id)                     AS users,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)  AS add_to_cart_events,
    ROUND(SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) * 100.0
          / COUNT(DISTINCT session_id), 2)       AS add_to_cart_rate_pct
FROM ecommerce_processed.user_activity
WHERE date >= DATE_ADD('day', -14, CURRENT_DATE)
  AND ab_test_variant IS NOT NULL
GROUP BY ab_test_variant
ORDER BY add_to_cart_rate_pct DESC;
