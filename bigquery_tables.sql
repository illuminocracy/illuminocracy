-- generate in bigquery - truncate and reload - grouped by creative to remove duplicates

CREATE TABLE illuminocracy.top_ads (
  delivery_day date not null,
  funding_entity string,
  affiliation_party string,
  affiliation_brexit string,
  spend_gbp_lower float64,
  spend_gbp_upper float64,
  spend_gbp_mid float64,
  impressions_lower float64,
  impressions_upper float64,
  impressions_mid float64,
  creative_body string,
  creative_link_caption string,
  creative_link_description string,
  creative_link_title string,
  snapshot_url string,
  page_id string,
  page_name string,
  instances int64
);

-- generate in bigquery - truncate and reload - new version aggregated

CREATE TABLE illuminocracy.ads_daily_agg (
  delivery_day date not null,
  funding_entity string,
  affiliation_party string,
  affiliation_brexit string,
  spend_gbp_lower float64,
  spend_gbp_upper float64,
  spend_gbp_mid float64,
  impressions_lower float64,
  impressions_upper float64,
  impressions_mid float64
);

-- generate in bigquery - incremental load - new version without demographics

CREATE TABLE illuminocracy.ads_daily (
  ad_id int64 not null,
  delivery_day date not null,
  funding_entity string,
  affiliation_party string,
  affiliation_brexit string,
  page_id string,
  page_name string,
  spend_gbp_lower float64,
  spend_gbp_upper float64,
  spend_gbp_mid float64,
  impressions_lower float64,
  impressions_upper float64,
  impressions_mid float64,
  added_date_time timestamp not null,
  batch_id int64
);

-- generate in bigquery - incremental load

CREATE TABLE illuminocracy.ads_daily_with_demographics (
  ad_id int64 not null,
  delivery_day date not null,
  funding_entity string,
  affiliation_party string,
  affiliation_brexit string,
  page_id string,
  page_name string,
  spend_gbp_lower float64,
  spend_gbp_upper float64,
  spend_gbp_mid float64,
  impressions_lower float64,
  impressions_upper float64,
  impressions_mid float64,
  age string,
  gender string,
  added_date_time timestamp not null,
  batch_id int64
);

-- generate in bigquery - truncate this table every time and repopulate it to make populating ads_daily easier

CREATE TABLE illuminocracy.ads_delivery (
  ad_id int64 not null,
  delivery_start date,
  delivery_end date,
  currently_running bool,
  days_running int64,
  batch_id int64
);

-- local ad staging table - upload to bigquery incrementally

CREATE TABLE illuminocracy.ads (
  id int64 not null,
  creation_time timestamp,
  creative_body string,
  creative_link_caption string,
  creative_link_description string,
  creative_link_title string,
  delivery_start_time timestamp,
  delivery_stop_time timestamp,
  currency string,
  snapshot_url string,
  page_id string,
  page_name string,
  funding_entity string,
  spend_lower int64,
  spend_upper int64,
  impressions_lower int64,
  impressions_upper int64,
  capture_date_time timestamp not null,
  batch_id int64,
  active bool,
  became_inactive timestamp
);

-- local ad staging table - upload to bigquery incrementally

CREATE TABLE illuminocracy.ad_keywords (
  ad_id int64 not null,
  keyword string not null,
  batch_id int64
);

-- local ad staging table - upload to bigquery incrementally

CREATE TABLE illuminocracy.ads_distribution_region (
  ad_id int64 not null,
  region string not null,
  percentage float64 not null,
  batch_id int64
);

-- local ad staging table - upload to bigquery incrementally

CREATE TABLE illuminocracy.ads_distribution_demographics (
  ad_id int64 not null,
  age string not null,
  gender string not null,
  percentage float64 not null,
  batch_id int64
);

-- local table - truncate and reload to bigquery

CREATE TABLE illuminocracy.exchange_rates (
  currency string not null,
  rate_date date not null,
  gbp_value float64 not null
);

-- local table - truncate and reload to bigquery

CREATE TABLE illuminocracy.funding_entity_affiliations (
  funding_entity string not null,
  affiliation_party string,
  affiliation_brexit string,
  date_added timestamp
);

-- local table - truncate and reload to bigquery

CREATE TABLE illuminocracy.days (
  day date not null
);
