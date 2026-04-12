-- evkollen D1 schema

CREATE TABLE IF NOT EXISTS models (
  id          TEXT PRIMARY KEY,
  make        TEXT NOT NULL,
  model       TEXT NOT NULL,
  powertrain  TEXT NOT NULL  -- 'bev' | 'phev'
);

CREATE TABLE IF NOT EXISTS listings (
  id              TEXT PRIMARY KEY,  -- "{source}:{external_id_or_slug}"
  model_id        TEXT NOT NULL REFERENCES models(id),
  source          TEXT NOT NULL,     -- 'coches_net'
  url             TEXT NOT NULL,
  title           TEXT,
  version         TEXT,              -- trim/variant as shown in ad
  year            INTEGER,
  km              INTEGER,
  price           INTEGER,           -- EUR kontantpris
  price_financed  INTEGER,           -- EUR finansierat pris
  image_url       TEXT,
  province        TEXT,
  dealer_name     TEXT,
  is_professional INTEGER NOT NULL DEFAULT 1,
  first_seen      TEXT NOT NULL,     -- ISO date
  last_seen       TEXT NOT NULL,     -- ISO date
  UNIQUE(source, url)
);

CREATE TABLE IF NOT EXISTS price_snapshots (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  listing_id  TEXT NOT NULL REFERENCES listings(id),
  price       INTEGER NOT NULL,
  km          INTEGER,
  scraped_at  TEXT NOT NULL          -- ISO datetime
);

CREATE INDEX IF NOT EXISTS idx_listings_model    ON listings(model_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_listing ON price_snapshots(listing_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_date    ON price_snapshots(scraped_at);
