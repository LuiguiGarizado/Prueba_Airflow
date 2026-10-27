-- Esquemas para el proyecto
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

-- 1. Tabla de productos: guarda el codigo y el nombre oficial (la moda)
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    stock_code            TEXT PRIMARY KEY,
    canonical_description TEXT NOT NULL,
    category              TEXT        DEFAULT 'Sin categoría',
    is_active             BOOLEAN     DEFAULT TRUE,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Transacciones validas (ventas y devoluciones)
CREATE TABLE IF NOT EXISTS analytics.fact_transactions (
    transaction_id        BIGSERIAL    PRIMARY KEY,
    run_id                TEXT         NOT NULL,
    source_file           TEXT         NOT NULL,
    source_row_index      BIGINT,
    invoice_no            TEXT         NOT NULL,
    stock_code            TEXT         NOT NULL,
    description           TEXT,
    canonical_description TEXT,
    quantity              NUMERIC(14,3) NOT NULL,
    unit_price            NUMERIC(14,4) NOT NULL,
    gross_revenue         NUMERIC(18,4) NOT NULL,
    record_type           TEXT         NOT NULL
                          CHECK (record_type IN ('sale','return_or_adjustment')),
    transaction_at        TIMESTAMPTZ  NOT NULL,
    transaction_date      DATE         NOT NULL,
    customer_id           TEXT,
    is_guest              BOOLEAN      NOT NULL DEFAULT FALSE,
    country               TEXT,
    created_at            TIMESTAMPTZ  DEFAULT NOW(),
    CONSTRAINT uq_fact_source UNIQUE (source_file, source_row_index)
);

CREATE INDEX IF NOT EXISTS ix_ft_date        ON analytics.fact_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS ix_ft_stock       ON analytics.fact_transactions(stock_code);
CREATE INDEX IF NOT EXISTS ix_ft_record_type ON analytics.fact_transactions(record_type);
CREATE INDEX IF NOT EXISTS ix_ft_customer    ON analytics.fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS ix_ft_country     ON analytics.fact_transactions(country);

-- 3. Tabla para guardar los registros que tienen error (precios <= 0 o nulos)
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    rejection_id      BIGSERIAL    PRIMARY KEY,
    run_id            TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    source_row_index  BIGINT,
    invoice_no        TEXT,
    stock_code        TEXT,
    quantity          NUMERIC(14,3),
    unit_price        NUMERIC(14,4),
    customer_id       TEXT,
    rejection_reason  TEXT         NOT NULL,
    raw_record        JSONB        NOT NULL,
    rejected_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_rr_run    ON audit.rejected_records(run_id);
CREATE INDEX IF NOT EXISTS ix_rr_reason ON audit.rejected_records(rejection_reason);

-- 4. Resumen diario por producto: calcula ventas brutas y netas
CREATE TABLE IF NOT EXISTS analytics.daily_product_revenue (
    summary_id            BIGSERIAL    PRIMARY KEY,
    transaction_date      DATE         NOT NULL,
    stock_code            TEXT         NOT NULL,
    canonical_description TEXT,
    sales_quantity        NUMERIC(14,3) DEFAULT 0,
    returns_quantity      NUMERIC(14,3) DEFAULT 0,
    net_quantity          NUMERIC(14,3) DEFAULT 0,
    gross_revenue         NUMERIC(18,4) DEFAULT 0,
    returns_adjustment    NUMERIC(18,4) DEFAULT 0,
    net_revenue           NUMERIC(18,4) DEFAULT 0,
    updated_at            TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_daily_rev UNIQUE (transaction_date, stock_code)
);

CREATE INDEX IF NOT EXISTS ix_dr_date  ON analytics.daily_product_revenue(transaction_date);
CREATE INDEX IF NOT EXISTS ix_dr_stock ON analytics.daily_product_revenue(stock_code);
