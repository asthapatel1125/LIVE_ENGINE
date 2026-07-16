-- Cloud storage schema for the hosted ThetaData dashboard.
-- Recommended database: Supabase Postgres, Neon Postgres, or Timescale/Tiger Cloud.
-- If TimescaleDB is available, the hypertable section below makes history queries faster.

create table if not exists option_greek_points (
  ts timestamptz not null,
  symbol text not null,
  expiration_mode text not null default 'near_dte',
  source text not null default 'thetadata',
  underlying double precision not null default 0,
  row_count integer not null default 0,
  delta double precision not null default 0,
  theta double precision not null default 0,
  vega double precision not null default 0,
  rho double precision not null default 0,
  gamma double precision not null default 0,
  vanna double precision not null default 0,
  charm double precision not null default 0,
  vomma double precision not null default 0,
  speed double precision not null default 0,
  zomma double precision not null default 0,
  color double precision not null default 0,
  ultima double precision not null default 0,
  created_at timestamptz not null default now(),
  primary key (symbol, expiration_mode, ts)
);

create index if not exists option_greek_points_symbol_ts_desc
  on option_greek_points (symbol, ts desc);

-- Optional raw snapshot archive registry.
-- Store the actual raw CSV in S3/R2/Supabase Storage, not in this table.
create table if not exists raw_snapshot_archives (
  ts timestamptz not null,
  symbol text not null,
  row_count integer not null default 0,
  storage_url text not null,
  created_at timestamptz not null default now(),
  primary key (symbol, ts)
);

-- Timescale/Tiger Cloud optimization. Run only when the extension is available.
-- create extension if not exists timescaledb;
-- select create_hypertable('option_greek_points', by_range('ts'), if_not_exists => true);
-- alter table option_greek_points set (
--   timescaledb.compress,
--   timescaledb.compress_segmentby = 'symbol, expiration_mode'
-- );
-- select add_compression_policy('option_greek_points', interval '7 days', if_not_exists => true);
