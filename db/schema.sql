-- =====================================================================
-- Deadbolt: High-Concurrency Micro-Batching Engine & Deadlock Resolver
-- Schema + seed data. Idempotent: safe to run on every startup.
-- =====================================================================

-- ---------------------------------------------------------------------
-- products: the high-contention inventory rows.
--   available = total_stock - reserved_stock
--
-- NOTE: there is deliberately NO  CHECK (reserved_stock <= total_stock).
-- In a real system you would add it as a last line of defence, but here
-- it would hide the very failure the naive strategy is meant to expose
-- (overselling). Integrity is enforced by the locking strategies and is
-- verified continuously by the dashboard instead.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    id              INTEGER        PRIMARY KEY,
    name            TEXT           NOT NULL,
    total_stock     INTEGER        NOT NULL CHECK (total_stock >= 0),
    reserved_stock  INTEGER        NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    version         BIGINT         NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- orders: one row per confirmed line item.
--   request_id ties every line back to the originating request and to
--   audit_logs, so a cart of 3 items produces 3 rows sharing request_id.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL   PRIMARY KEY,
    request_id  UUID        NOT NULL,
    user_id     INTEGER     NOT NULL,
    product_id  INTEGER     NOT NULL REFERENCES products (id),
    quantity    INTEGER     NOT NULL CHECK (quantity > 0),
    status      TEXT        NOT NULL DEFAULT 'CONFIRMED'
                            CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- audit_logs: one row per request outcome, for every strategy.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_logs (
    id                 BIGSERIAL     PRIMARY KEY,
    request_id         UUID          NOT NULL,
    strategy_used      TEXT          NOT NULL
                                     CHECK (strategy_used IN ('naive', 'pessimistic', 'optimistic', 'batched')),
    execution_time_ms  NUMERIC(12, 3) NOT NULL,
    status             TEXT          NOT NULL
                                     CHECK (status IN ('CONFIRMED', 'SOLD_OUT', 'DEADLOCK', 'CONFLICT', 'ERROR', 'REJECTED')),
    error_message      TEXT,
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orders_product_id      ON orders (product_id);
CREATE INDEX IF NOT EXISTS idx_orders_request_id      ON orders (request_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at  ON audit_logs (created_at);
CREATE INDEX IF NOT EXISTS idx_audit_logs_strategy    ON audit_logs (strategy_used, status);

-- ---------------------------------------------------------------------
-- Seed: 5 "hot" flash-sale products. Product 1 is the hottest row and
-- receives most of the traffic in the default load profile.
-- ---------------------------------------------------------------------
INSERT INTO products (id, name, total_stock, reserved_stock, price, version) VALUES
    (1, 'Stadium Concert - Gold Circle Pass',      600, 0,  12500.00, 0),
    (2, 'Cricket Final - North Stand Ticket',      400, 0,   4800.00, 0),
    (3, 'Flagship Phone - Launch Day Unit',        300, 0, 139900.00, 0),
    (4, 'Game Console - Founders Pre-order Slot',  200, 0,  59990.00, 0),
    (5, 'Sneaker Drop - Limited Retro High',       150, 0,  17999.00, 0)
ON CONFLICT (id) DO NOTHING;
