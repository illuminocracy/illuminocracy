PRAGMA foreign_keys = ON;

CREATE TABLE ads (
  id int PRIMARY KEY,
  creation_time timestamp,
  creative_body text,
  creative_link_caption text,
  creative_link_description text,
  creative_link_title text,
  delivery_start_time timestamp,
  delivery_stop_time timestamp,
  currency text,
  snapshot_url text,
  page_id text,
  page_name text,
  funding_entity text,
  spend_lower int,
  spend_upper int,
  impressions_lower int,
  impressions_upper int,
  capture_date_time timestamp,
  uploaded boolean
);

CREATE TABLE ad_keywords (
  ad_id int,
  keyword text
  --FOREIGN KEY(ad_id) REFERENCES ads(id)   -- constraint removed so keywords can be retained if ad is updated
);

CREATE TABLE ads_distribution_region (
  ad_id int,
  region text,
  percentage real,
  FOREIGN KEY(ad_id) REFERENCES ads(id)
);

CREATE TABLE ads_distribution_demographics (
  ad_id int,
  age text,
  gender text,
  percentage real,
  FOREIGN KEY(ad_id) REFERENCES ads(id)
);

CREATE TABLE exchange_rates (
  currency text,
  rate_date date,
  gbp_value real
);

CREATE TABLE funding_entity_affiliations (
  funding_entity text,
  affiliation_party text check(affiliation_party in ('conservative', 'labour', 'libdem', 'green', 'brexit', 'snp', 'plaid')),
  affiliation_brexit text check(affiliation_brexit in ('leave', 'remain', 'neutral')),
  date_added timestamp
);

CREATE TABLE search_terms (
  term text,
  date_added timestamp
);

INSERT INTO search_terms (term, date_added) VALUES
  ('brexit', '2019-11-10 12:00:00'),
  ('conservative', '2019-11-10 12:00:00'),
  ('labour', '2019-11-10 12:00:00'),
  ('liberal democrats', '2019-11-10 12:00:00'),
  ('vote', '2019-11-10 12:00:00'),
  ('election', '2019-11-10 12:00:00'),
  ('boris', '2019-11-10 12:00:00'),
  ('corbyn', '2019-11-10 12:00:00'),
  ('tactical', '2019-11-10 12:00:00'),
  ('candidate', '2019-11-10 12:00:00'),
  ('climate', '2019-11-10 12:00:00'),
  ('peoples vote', '2019-11-10 12:00:00'),
  ('best for britain', '2019-11-10 12:00:00'),
  ('government', '2019-11-10 12:00:00')
  ;
