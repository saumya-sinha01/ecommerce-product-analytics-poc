-- ==========================================
-- E-commerce Product Analytics POC Schema
-- Postgres
-- ==========================================

-- Helpful for reruns (optional). Comment out if you prefer not to drop.
-- DROP TABLE IF EXISTS events;
-- DROP TABLE IF EXISTS experiment_assignments;
-- DROP TABLE IF EXISTS sessions;
-- DROP TABLE IF EXISTS products;
-- DROP TABLE IF EXISTS users;

-- =========================
-- Dimension tables
-- =========================

CREATE TABLE IF NOT EXISTS users (
  user_id         BIGINT PRIMARY KEY,
  signup_ts       TIMESTAMP NOT NULL,
  country         TEXT NOT NULL,
  device_type     TEXT NOT NULL CHECK (device_type IN ('mobile','desktop')),
  is_new_user     BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  product_id      BIGINT PRIMARY KEY,
  category        TEXT NOT NULL,
  base_price      NUMERIC(10,2) NOT NULL CHECK (base_price >= 0)
);

-- =========================
-- Core fact tables
-- =========================

CREATE TABLE IF NOT EXISTS sessions (
  session_id        TEXT PRIMARY KEY,
  user_id           BIGINT NOT NULL REFERENCES users(user_id),
  session_start_ts  TIMESTAMP NOT NULL,
  session_end_ts    TIMESTAMP NOT NULL,
  device_type       TEXT NOT NULL CHECK (device_type IN ('mobile','desktop')),
  CHECK (session_end_ts >= session_start_ts)
);

-- One assignment per (experiment, user). This is typical for user-level randomization.
CREATE TABLE IF NOT EXISTS experiment_assignments (
  experiment_name   TEXT NOT NULL,
  user_id           BIGINT NOT NULL REFERENCES users(user_id),
  variant           TEXT NOT NULL CHECK (variant IN ('control','treatment')),
  assignment_ts     TIMESTAMP NOT NULL,
  PRIMARY KEY (experiment_name, user_id)
);

-- Events are the clickstream. We keep a small set of explicit columns for speed,
-- and add an optional JSONB properties column for event-specific attributes.
CREATE TABLE IF NOT EXISTS events (
  event_id          TEXT PRIMARY KEY,
  event_ts          TIMESTAMP NOT NULL,

  user_id           BIGINT NOT NULL REFERENCES users(user_id),
  session_id        TEXT NOT NULL REFERENCES sessions(session_id),
  product_id        BIGINT NULL REFERENCES products(product_id),

  event_type        TEXT NOT NULL CHECK (
    event_type IN (
      'session_start',
      'view_home',
      'search',
      'view_product',
      'add_to_cart',
      'begin_checkout',
      'purchase',
      'logout'
    )
  ),

  -- Purchase-related fields (only meaningful for purchase events; else NULL)
  price_paid        NUMERIC(10,2) NULL CHECK (price_paid IS NULL OR price_paid >= 0),
  quantity          INT NULL CHECK (quantity IS NULL OR quantity > 0),
  discount_amount   NUMERIC(10,2) NULL CHECK (discount_amount IS NULL OR discount_amount >= 0),

  -- Optional properties: search query, referrer, UI variant metadata, etc.
  properties        JSONB NULL
);

-- =========================
-- Indexes (important for analytics queries)
-- =========================

CREATE INDEX IF NOT EXISTS idx_events_user_ts
  ON events (user_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_events_session_ts
  ON events (session_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_events_type_ts
  ON events (event_type, event_ts);

CREATE INDEX IF NOT EXISTS idx_events_product_ts
  ON events (product_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_sessions_user_start
  ON sessions (user_id, session_start_ts);

CREATE INDEX IF NOT EXISTS idx_assignments_user
  ON experiment_assignments (user_id);

CREATE INDEX IF NOT EXISTS idx_assignments_exp_variant
  ON experiment_assignments (experiment_name, variant);
