create table if not exists wineries (
  id uuid primary key default gen_random_uuid(),

  -- identity
  name text not null unique,
  website_url text not null,
  source_page_url text not null,

  -- core description
  description text,

  -- structured content
  family_spirit jsonb not null default '{}'::jsonb,
  history_timeline jsonb not null default '[]'::jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists products (
  id uuid primary key default gen_random_uuid(),

  -- relation
  winery_id uuid references wineries(id) on delete cascade,

  -- identity
  name text not null,

  product_url text not null unique,
  source_page_url text not null,

  -- core description
  description text,

  -- technical attributes (only stable fields)
  dosage_g_per_l numeric(6,2),
  aging text,
  operating_temperature text,
  crus_assembles text,
  millennium text,

  -- grape composition
  grape_chardonnay_percent numeric(5,2),
  grape_pinot_noir_percent numeric(5,2),
  grape_meunier_percent numeric(5,2),

  -- awards / ratings
  awards_and_ratings jsonb not null default '[]'::jsonb,

  -- document
  data_sheet_url text,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  unique (winery_id, name)
);

create table if not exists media (
  id uuid primary key default gen_random_uuid(),

  media_type text not null check (media_type in ('image','video')),
  url text not null unique,

  source_page_url text not null,

  created_at timestamptz not null default now()
);