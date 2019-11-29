INSERT INTO ads_daily (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  age,
  gender,
  added_date_time,
  uploaded)
  SELECT
    A.id, D.day, A.funding_entity, F.affiliation_party, F.affiliation_brexit,
    A.page_id, A.page_name,
    ((A.spend_lower / R.gbp_value) * DM.percentage) / DD.days_running,
    ((A.spend_upper / R.gbp_value) * DM.percentage) / DD.days_running,
    (((((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) / R.gbp_value) * DM.percentage) / DD.days_running,
    (A.impressions_lower * DM.percentage) / DD.days_running,
    (A.impressions_upper * DM.percentage) / DD.days_running,
    ((((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) * DM.percentage) / DD.days_running,
    DM.age,
    DM.gender,
    '2019-11-25', 0
  from
    ads A
      INNER JOIN ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
      LEFT OUTER JOIN funding_entity_affiliations F ON A.funding_entity = F.funding_entity
      INNER JOIN exchange_rates R ON R.currency = A.currency
      INNER JOIN ads_distribution_demographics DM ON A.id = DM.ad_id

m njmn,
  SELECT A.id, D.day, DD.days_running, DD.delivery_start, DD.delivery_end, R.currency, R.gbp_value, A.spend_lower,
  A.impressions_lower, A.spend_lower / R.gbp_value / DD.days_running, A.impressions_lower / DD.days_running
  FROM illuminocracy.ads A
    INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
    INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
    INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
    WHERE DD.delivery_start > '2019-11-01'
  LIMIT 10;

  SELECT A.id, D.day, DD.days_running, DD.delivery_start, DD.delivery_end, R.currency, R.gbp_value, A.spend_lower,
  A.impressions_lower, A.spend_lower / R.gbp_value / DD.days_running, A.impressions_lower / DD.days_running, A.funding_entity, F.affiliation_party, F.affiliation_brexit
  FROM illuminocracy.ads A
    INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
    INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
    INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
    LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
    WHERE DD.delivery_start > '2019-06-01'
    AND A.spend_lower > 0 --AND A.currency != 'GBP'
    AND A.funding_entity is not null
  LIMIT 10;


  SELECT A.id, D.day, DD.days_running, DD.delivery_start, DD.delivery_end, R.currency, R.gbp_value, DM.age, DM.gender, A.spend_lower,
  A.impressions_lower, (A.spend_lower / R.gbp_value / DD.days_running) * DM.percentage,
  (A.impressions_lower / DD.days_running) * DM.percentage, A.funding_entity, F.affiliation_party, F.affiliation_brexit
  FROM illuminocracy.ads A
    INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
    INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
    INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
    INNER JOIN illuminocracy.ads_distribution_demographics DM ON A.id = DM.ad_id
    LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
    WHERE DD.delivery_start > '2019-06-01'
    AND A.spend_lower > 0 AND A.currency = 'GBP'
    AND A.funding_entity is not null
  LIMIT 10;











------------------- POPULATE ads_daily_with_demographics ---- not used anymore

----- ADS IN OTHER CURRENCIES

INSERT INTO illuminocracy.ads_daily_with_demographics (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  age,
  gender,
  added_date_time)
  SELECT
    A.id, D.day, A.funding_entity, F.affiliation_party, F.affiliation_brexit,
    A.page_id, A.page_name,
    ((A.spend_lower / R.gbp_value) * DM.percentage) / DD.days_running,
    ((A.spend_upper / R.gbp_value) * DM.percentage) / DD.days_running,
    (((((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) / R.gbp_value) * DM.percentage) / DD.days_running,
    (A.impressions_lower * DM.percentage) / DD.days_running,
    (A.impressions_upper * DM.percentage) / DD.days_running,
    ((((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) * DM.percentage) / DD.days_running,
    DM.age,
    DM.gender,
    CURRENT_TIMESTAMP()
  FROM
    illuminocracy.ads A
      INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
      LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
      INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
      INNER JOIN illuminocracy.ads_distribution_demographics DM ON A.id = DM.ad_id
    WHERE A.currency != 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'
    AND A.capture_date_time > ?



------ ADS IN GBP

INSERT INTO illuminocracy.ads_daily_with_demographics (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  age,
  gender,
  added_date_time)
  SELECT
    A.id, D.day, A.funding_entity, F.affiliation_party, F.affiliation_brexit,
    A.page_id, A.page_name,
    (A.spend_lower * DM.percentage) / DD.days_running,
    (A.spend_upper * DM.percentage) / DD.days_running,
    ((((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) * DM.percentage) / DD.days_running,
    (A.impressions_lower * DM.percentage) / DD.days_running,
    (A.impressions_upper * DM.percentage) / DD.days_running,
    ((((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) * DM.percentage) / DD.days_running,
    DM.age,
    DM.gender,
    CURRENT_TIMESTAMP()
  FROM
    illuminocracy.ads A
      INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
      LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
      INNER JOIN illuminocracy.ads_distribution_demographics DM ON A.id = DM.ad_id
    WHERE A.currency = 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'
    AND A.capture_date_time > ?


    -- queries to find large funding entities

    select count(*) from (
     SELECT A.funding_entity
      FROM ads A
      LEFT OUTER JOIN funding_entity_affiliations B ON A.funding_entity = B.funding_entity
      GROUP BY A.funding_entity
      HAVING sum(A.spend_upper) > 10000)

    -- query to find large funding entities that have at least one affiliation recorded

    select count(*) from (
     SELECT A.funding_entity
      FROM ads A
      LEFT OUTER JOIN funding_entity_affiliations B ON A.funding_entity = B.funding_entity AND (B.affiliation_party IS NOT NULL OR B.affiliation_brexit IS NOT NULL)
      GROUP BY A.funding_entity
      HAVING sum(A.spend_upper) > 10000)


----------------------- POPULATE BIGQUERY TABLES ---------------------------

------------------- POPULATE ads_daily -
-- this excludes funding entities with less than 10k overall spend
-- and funding entities with no affilation of any kind and less than 100k overall spend

DELETE FROM illuminocracy.ads_daily WHERE 1=1

----- ADS IN OTHER CURRENCIES


INSERT INTO illuminocracy.ads_daily (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  added_date_time)
  WITH large_funding_entities AS (
    SELECT funding_entity FROM illuminocracy.ads GROUP BY funding_entity
    HAVING SUM(spend_upper) > 10000)
  SELECT
    A.id, D.day, A.funding_entity, F.affiliation_party, F.affiliation_brexit,
    A.page_id, A.page_name,
    (A.spend_lower / R.gbp_value) / DD.days_running,
    (A.spend_upper / R.gbp_value) / DD.days_running,
    ((((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) / R.gbp_value) / DD.days_running,
    A.impressions_lower / DD.days_running,
    A.impressions_upper / DD.days_running,
    (((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) / DD.days_running,
    CURRENT_TIMESTAMP()
  FROM
    illuminocracy.ads A
      INNER JOIN large_funding_entities ON A.funding_entity = large_funding_entities.funding_entity
      INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
      LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
      INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
    WHERE A.currency != 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'
    AND A.capture_date_time > ?



------ ADS IN GBP

INSERT INTO illuminocracy.ads_daily (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  added_date_time)
  WITH large_funding_entities AS (
    SELECT funding_entity FROM illuminocracy.ads GROUP BY funding_entity
    HAVING SUM(spend_upper) > 10000)
  SELECT
    A.id, D.day, A.funding_entity, F.affiliation_party, F.affiliation_brexit,
    A.page_id, A.page_name,
    A.spend_lower / DD.days_running,
    A.spend_upper / DD.days_running,
    (((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) / DD.days_running,
    A.impressions_lower / DD.days_running,
    A.impressions_upper / DD.days_running,
    (((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) / DD.days_running,
    CURRENT_TIMESTAMP()
  FROM
    illuminocracy.ads A
      INNER JOIN large_funding_entities ON A.funding_entity = large_funding_entities.funding_entity
      INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
      LEFT OUTER JOIN illuminocracy.funding_entity_affiliations F ON A.funding_entity = F.funding_entity
    WHERE A.currency = 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'
    AND A.capture_date_time > ?

-- ADS with null funding entity

INSERT INTO illuminocracy.ads_daily (ad_id,
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  page_id,
  page_name,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  added_date_time)
  SELECT
    A.id, D.day, A.funding_entity, NULL, NULL,
    A.page_id, A.page_name,
    A.spend_lower / DD.days_running,
    A.spend_upper / DD.days_running,
    (((A.spend_upper - A.spend_lower) / 2) + A.spend_lower) / DD.days_running,
    A.impressions_lower / DD.days_running,
    A.impressions_upper / DD.days_running,
    (((A.impressions_upper - A.impressions_lower) / 2) + A.impressions_lower) / DD.days_running,
    CURRENT_TIMESTAMP()
  FROM
    illuminocracy.ads A
      INNER JOIN illuminocracy.ads_delivery DD ON A.id = DD.ad_id
      INNER JOIN illuminocracy.days D ON D.day >= DD.delivery_start AND D.day <= DD.delivery_end
    WHERE A.funding_entity IS NULL
    AND A.currency = 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'
    AND A.capture_date_time > ?


-- populate ads_daily_agg


DELETE FROM illuminocracy.ads_daily_agg WHERE 1=1


INSERT INTO illuminocracy.ads_daily_agg (
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid)
  SELECT delivery_day, funding_entity, affiliation_party, affiliation_brexit,
    SUM(spend_gbp_lower), SUM(spend_gbp_upper), SUM(spend_gbp_mid),
    SUM(impressions_lower), SUM(impressions_upper), SUM(impressions_mid)
  FROM illuminocracy.ads_daily
  GROUP BY delivery_day, funding_entity, affiliation_party, affiliation_brexit


-- populate top_ads

DELETE FROM illuminocracy.top_ads WHERE 1=1

INSERT INTO illuminocracy.top_ads (
  delivery_day,
  funding_entity,
  affiliation_party,
  affiliation_brexit,
  spend_gbp_lower,
  spend_gbp_upper,
  spend_gbp_mid,
  impressions_lower,
  impressions_upper,
  impressions_mid,
  creative_body,
  creative_link_caption,
  creative_link_description,
  creative_link_title,
  snapshot_url,
  page_id,
  page_name,
  instances)
  SELECT AD.delivery_day, AD.funding_entity, AD.affiliation_party, AD.affiliation_brexit,
    SUM(AD.spend_gbp_lower), SUM(AD.spend_gbp_upper), SUM(AD.spend_gbp_mid),
    SUM(AD.impressions_lower), SUM(AD.impressions_upper), SUM(AD.impressions_mid),
    A.creative_body, A.creative_link_caption, A.creative_link_description, A.creative_link_title,
    MIN(A.snapshot_url),MIN(A.page_id),MIN(A.page_name), count(distinct A.id)
    FROM illuminocracy.ads_daily AD INNER JOIN illuminocracy.ads A ON AD.ad_id = A.id
    GROUP BY AD.delivery_day, AD.funding_entity, AD.affiliation_party, AD.affiliation_brexit,
      A.creative_body, A.creative_link_caption, A.creative_link_description, A.creative_link_title
    HAVING SUM(AD.spend_gbp_mid) > 100
