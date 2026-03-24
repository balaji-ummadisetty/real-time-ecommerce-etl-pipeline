-- =============================================================================
-- redshift_schema.sql
-- Redshift table DDLs + warehouse analytics queries
-- =============================================================================


-- =============================================================================
-- SCHEMA SETUP
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ecommerce;


-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.dim_product (
    product_id      VARCHAR(20)     NOT NULL,
    product_name    VARCHAR(200)    NOT NULL,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    sku             VARCHAR(50),
    base_price      DECIMAL(10,2),
    created_at      TIMESTAMP       DEFAULT GETDATE(),
    updated_at      TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (product_id)
)
DISTSTYLE ALL   -- small dimension → broadcast to all nodes
SORTKEY (category, subcategory);


CREATE TABLE IF NOT EXISTS ecommerce.dim_user (
    user_id             VARCHAR(100)    NOT NULL,
    user_segment        VARCHAR(50),
    account_age_days    INTEGER,
    country_code        CHAR(2),
    city                VARCHAR(100),
    PRIMARY KEY (user_id)
)
DISTSTYLE ALL
SORTKEY (user_id);


CREATE TABLE IF NOT EXISTS ecommerce.dim_date (
    date_key        INTEGER         NOT NULL,   -- YYYYMMDD
    full_date       DATE            NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(12),
    week_of_year    SMALLINT,
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);


-- =============================================================================
-- FACT TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.fact_orders (
    order_id                VARCHAR(50)     NOT NULL,
    event_id                VARCHAR(50)     NOT NULL,
    user_id                 VARCHAR(100),
    session_id              VARCHAR(100),
    order_date              DATE,
    date_key                INTEGER,        -- FK → dim_date
    item_count              INTEGER,
    subtotal                DECIMAL(12,2),
    tax                     DECIMAL(10,2),
    shipping_cost           DECIMAL(10,2),
    discount_amount         DECIMAL(10,2),
    order_total             DECIMAL(12,2),
    coupon_code             VARCHAR(50),
    shipping_method         VARCHAR(50),
    warehouse_id            VARCHAR(30),
    is_gift                 BOOLEAN,
    payment_method          VARCHAR(30),
    payment_status          VARCHAR(20),
    payment_currency        CHAR(3),
    transaction_id          VARCHAR(100),
    device_type             VARCHAR(20),
    country_code            CHAR(2),
    city                    VARCHAR(100),
    order_size              VARCHAR(20),
    customer_order_count    INTEGER,
    customer_total_spend    DECIMAL(14,2),
    created_at              TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (order_id)
)
DISTKEY (user_id)           -- collocate with user activity
SORTKEY (order_date, payment_status);


CREATE TABLE IF NOT EXISTS ecommerce.fact_order_items (
    item_surrogate_key  BIGINT          IDENTITY(1,1),
    order_id            VARCHAR(50)     NOT NULL,
    user_id             VARCHAR(100),
    order_date          DATE,
    date_key            INTEGER,
    product_id          VARCHAR(20),    -- FK → dim_product
    sku                 VARCHAR(50),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    unit_price          DECIMAL(10,2),
    quantity            INTEGER,
    discount_pct        DECIMAL(5,4),
    item_revenue        DECIMAL(12,2),
    payment_status      VARCHAR(20),
    country_code        CHAR(2),
    PRIMARY KEY (item_surrogate_key)
)
DISTKEY (order_id)
SORTKEY (order_date, category);


CREATE TABLE IF NOT EXISTS ecommerce.fact_user_activity (
    event_id                VARCHAR(50)     NOT NULL,
    event_type              VARCHAR(50),
    event_ts                TIMESTAMP,
    date_key                INTEGER,
    user_id                 VARCHAR(100),
    session_id              VARCHAR(100),
    anonymous               BOOLEAN,
    user_segment            VARCHAR(50),
    page_url                VARCHAR(500),
    referrer                VARCHAR(500),
    search_query            VARCHAR(500),
    product_id              VARCHAR(20),
    category                VARCHAR(100),
    session_duration_sec    INTEGER,
    page_views_in_session   INTEGER,
    is_bounce               BOOLEAN,
    is_high_value_session   BOOLEAN,
    channel                 VARCHAR(50),
    device_type             VARCHAR(20),
    os                      VARCHAR(30),
    browser                 VARCHAR(50),
    country_code            CHAR(2),
    city                    VARCHAR(100),
    ab_test_variant         VARCHAR(50),
    PRIMARY KEY (event_id)
)
DISTKEY (user_id)
SORTKEY (event_ts, event_type);


-- =============================================================================
-- SUMMARY / REPORTING TABLES (loaded by Glue ETL)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.daily_sales (
    date                DATE            NOT NULL,
    country_code        CHAR(2),
    payment_currency    CHAR(3),
    orders              INTEGER,
    gross_revenue       DECIMAL(14,2),
    total_discounts     DECIMAL(14,2),
    net_revenue         DECIMAL(14,2),
    avg_order_value     DECIMAL(10,2),
    unique_buyers       INTEGER,
    units_sold          INTEGER,
    gift_orders         INTEGER,
    processing_date     DATE,
    PRIMARY KEY (date, country_code, payment_currency)
)
DISTSTYLE ALL
SORTKEY (date, country_code);


CREATE TABLE IF NOT EXISTS ecommerce.category_performance (
    date                DATE,
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    revenue             DECIMAL(14,2),
    units_sold          INTEGER,
    orders_with_category INTEGER,
    avg_discount_pct    DECIMAL(5,4),
    unique_buyers       INTEGER,
    processing_date     DATE
)
DISTSTYLE ALL
SORTKEY (date, category);


CREATE TABLE IF NOT EXISTS ecommerce.funnel_metrics (
    date                DATE,
    event_type          VARCHAR(50),
    country_code        CHAR(2),
    device_type         VARCHAR(20),
    channel             VARCHAR(50),
    event_count         INTEGER,
    sessions            INTEGER,
    users               INTEGER,
    avg_session_sec     DECIMAL(10,2),
    bounces             INTEGER,
    bounce_rate         DECIMAL(5,4),
    processing_date     DATE
)
DISTSTYLE ALL
SORTKEY (date, event_type);


-- =============================================================================
-- WAREHOUSE ANALYTICS QUERIES
-- =============================================================================

-- ── Rolling 7-day revenue with day-over-day growth ────────────────────────

SELECT
    date,
    SUM(gross_revenue)          AS daily_revenue,
    SUM(orders)                 AS daily_orders,
    LAG(SUM(gross_revenue), 1)  OVER (ORDER BY date)  AS prev_day_revenue,
    ROUND(
        (SUM(gross_revenue) - LAG(SUM(gross_revenue), 1) OVER (ORDER BY date))
        / NULLIF(LAG(SUM(gross_revenue), 1) OVER (ORDER BY date), 0) * 100
    , 2)                        AS revenue_growth_pct
FROM ecommerce.daily_sales
WHERE date >= DATEADD(day, -7, CURRENT_DATE)
GROUP BY date
ORDER BY date;


-- ── Month-to-date revenue vs previous month ────────────────────────────────

SELECT
    current_month.total_revenue    AS mtd_revenue,
    current_month.total_orders     AS mtd_orders,
    prev_month.total_revenue       AS prev_month_revenue,
    ROUND(
        (current_month.total_revenue - prev_month.total_revenue)
        / NULLIF(prev_month.total_revenue, 0) * 100
    , 2)                            AS mom_growth_pct
FROM (
    SELECT SUM(gross_revenue) AS total_revenue, SUM(orders) AS total_orders
    FROM ecommerce.daily_sales
    WHERE date_part('year',  date) = date_part('year',  CURRENT_DATE)
      AND date_part('month', date) = date_part('month', CURRENT_DATE)
) current_month
CROSS JOIN (
    SELECT SUM(gross_revenue) AS total_revenue
    FROM ecommerce.daily_sales
    WHERE date_part('year',  date) = date_part('year',  DATEADD(month, -1, CURRENT_DATE))
      AND date_part('month', date) = date_part('month', DATEADD(month, -1, CURRENT_DATE))
) prev_month;


-- ── Top customers by LTV (last 90 days) ────────────────────────────────────

SELECT
    o.user_id,
    u.user_segment,
    u.country_code,
    COUNT(DISTINCT o.order_id)          AS order_count,
    SUM(o.order_total)                  AS total_spend,
    AVG(o.order_total)                  AS avg_order_value,
    MIN(o.order_date)                   AS first_order_date,
    MAX(o.order_date)                   AS last_order_date,
    DATEDIFF(day, MIN(o.order_date), MAX(o.order_date)) AS customer_lifespan_days
FROM ecommerce.fact_orders o
LEFT JOIN ecommerce.dim_user u ON o.user_id = u.user_id
WHERE o.payment_status = 'success'
  AND o.order_date >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY 1, 2, 3
ORDER BY total_spend DESC
LIMIT 100;


-- ── Category revenue contribution ─────────────────────────────────────────

SELECT
    category,
    SUM(revenue)                                    AS category_revenue,
    SUM(units_sold)                                 AS units_sold,
    ROUND(SUM(revenue) * 100.0
          / SUM(SUM(revenue)) OVER ()
    , 2)                                            AS revenue_share_pct,
    ROUND(AVG(avg_discount_pct) * 100, 1)           AS avg_discount_pct
FROM ecommerce.category_performance
WHERE date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY category
ORDER BY category_revenue DESC;
