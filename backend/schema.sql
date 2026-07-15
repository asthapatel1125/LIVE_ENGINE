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

create table if not exists stream_control (
  control_key text primary key,
  enabled boolean not null default true,
  updated_at timestamptz not null default now()
);

insert into stream_control (control_key, enabled)
select 'thetadata_ingest', true
where not exists (
  select 1
  from stream_control
  where control_key = 'thetadata_ingest'
);

create table if not exists trade_alerts (
  id bigserial primary key,
  ts timestamptz not null,
  symbol text not null,
  action text not null check (action in ('LONG', 'SHORT')),
  horizon_minutes integer not null default 20,
  target_nq_points double precision not null default 50,
  estimated_nq_points double precision not null default 0,
  confidence double precision not null default 0,
  score double precision not null default 0,
  model text not null default 'greek_confluence_v1',
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists trade_alerts_symbol_ts_desc
  on trade_alerts (symbol, ts desc);

create table if not exists raw_snapshot_archives (
  ts timestamptz not null,
  symbol text not null,
  row_count integer not null default 0,
  storage_url text not null,
  created_at timestamptz not null default now(),
  primary key (symbol, ts)
);
