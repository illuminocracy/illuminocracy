#!/usr/bin/env python3

#python 3.6.5

from google.cloud import bigquery
from google.cloud import storage
import sqlite3
import datetime

gc_service_account = 'illuminocracy'
gc_service_account_key = 'data-transparency-8a55cfd7dd39.json'
bq_dataset = 'illuminocracy'

#today = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
#current_time = datetime.datetime.now()

# populate temporary ads_delivery table to handle conversion of delivery times
# to dates and ads that are currently running, and calculate number of days
# ad has been running for

def bq_execute(sql, query_params=[]):
    bigquery_client = bigquery.Client.from_service_account_json(gc_service_account_key)
    dataset_ref = bigquery_client.dataset(bq_dataset)

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    #job_config.dry_run = True
    #job_config.use_query_cache = False

    print("Executing BQ SQL: {}".format(sql))

    query_job = bigquery_client.query((sql),
        # Location must match that of the dataset(s) referenced in the query.
        #location="",
        job_config=job_config
    )

    try:
        result = query_job.result(timeout=600)  # Waits for 10 mins for query to complete.
    except Exception as e:
        import traceback, sys
        print(traceback.format_exception(None, # <- type(e) by docs, but ignored
                                 e, e.__traceback__),
                            file=sys.stderr, flush=True)
        print(query_job.errors)
        exit()

    assert query_job.state == "DONE"

    print("This query processed {} bytes.".format(query_job.total_bytes_processed))
    print("Timeline: {}".format(str(query_job.timeline)))

    return result

    #print("The query data:")
    #for row in query_job:
    #    # Row values can be accessed by field name or index.
    #    print("name={}, count={}".format(row[0], row["total_people"]))


result = bq_execute("SELECT MAX(added_date_time) AS latest_refresh FROM illuminocracy.ads_daily")

for row in result:
    latest_refresh = row['latest_refresh']

if latest_refresh is None:
    latest_refresh = '2019-01-01 00:00:00'

print("updating any ads since latest refresh of ads_daily at %s" % (latest_refresh))


bq_execute("DELETE FROM illuminocracy.ads_delivery WHERE 1=1")
bq_execute("""INSERT INTO illuminocracy.ads_delivery (ad_id, delivery_start, delivery_end, currently_running)
  SELECT id, DATE(delivery_start_time), DATE(delivery_stop_time), false
  FROM illuminocracy.ads
""")
bq_execute("""UPDATE illuminocracy.ads_delivery SET delivery_end = CURRENT_DATE(), currently_running = true WHERE (delivery_end IS NULL OR delivery_end > CURRENT_DATE())""")
bq_execute("""UPDATE illuminocracy.ads_delivery SET days_running = DATE_DIFF(delivery_end, delivery_start, DAY) + 1 WHERE 1=1""")


# add any new rows needed to ads_daily

# find most recently added row to ads_daily

result = bq_execute("SELECT MAX(added_date_time) AS latest_refresh FROM illuminocracy.ads_daily")
for row in result:
    latest_refresh = row['latest_refresh']

if latest_refresh is None:
    latest_refresh = '2019-01-01 00:00:00'

print("updating any ads since latest refresh of ads_daily at %s" % (latest_refresh))

# delete any ads that are still in the ads_daily table from the last refresh but
# have been superseded by ads uploaded by the current refresh (i.e. ads that have
# been updated by the facebook API rather than newly created)

# for some reason this isn't working at the moment, so switching to full refresh for now

#query_params = [
#    bigquery.ScalarQueryParameter("latest_refresh", "TIMESTAMP", latest_refresh)
#]
#bq_execute("""DELETE FROM illuminocracy.ads_daily WHERE ad_id IN
#    (SELECT id FROM illuminocracy.ads WHERE capture_date_time > @latest_refresh)""", query_params)

bq_execute("""DELETE FROM illuminocracy.ads_daily WHERE 1=1""")


# run query to insert non GBP ads

bq_execute("""INSERT INTO illuminocracy.ads_daily (ad_id,
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
    HAVING SUM(spend_upper) > 10000),
    affiliations AS (SELECT funding_entity, affiliation_party, affiliation_brexit
    FROM illuminocracy.funding_entity_affiliations GROUP BY funding_entity, affiliation_party, affiliation_brexit)
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
      LEFT OUTER JOIN affiliations F ON A.funding_entity = F.funding_entity
      INNER JOIN illuminocracy.exchange_rates R ON R.currency = A.currency AND R.rate_date = D.day
    WHERE A.currency != 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'

  """)#, query_params)
#    AND A.capture_date_time > @latest_refresh

# run query to insert GBP ads

bq_execute("""INSERT INTO illuminocracy.ads_daily (ad_id,
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
    HAVING SUM(spend_upper) > 10000),
    affiliations AS (SELECT funding_entity, affiliation_party, affiliation_brexit
    FROM illuminocracy.funding_entity_affiliations GROUP BY funding_entity, affiliation_party, affiliation_brexit)
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
      LEFT OUTER JOIN affiliations F ON A.funding_entity = F.funding_entity
    WHERE A.currency = 'GBP'
    AND DD.delivery_end > '2019-08-24'
    AND D.day > '2019-08-24'

  """)#, query_params)
#     AND A.capture_date_time > @latest_refresh

# run query to insert ads with null funding entity

bq_execute("""INSERT INTO illuminocracy.ads_daily (ad_id,
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

  """)#, query_params)
#    AND A.capture_date_time > @latest_refresh




# populate ads_daily_agg


bq_execute("""DELETE FROM illuminocracy.ads_daily_agg WHERE 1=1""")


bq_execute("""INSERT INTO illuminocracy.ads_daily_agg (
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
  GROUP BY delivery_day, funding_entity, affiliation_party, affiliation_brexit""")


# populate top_ads

bq_execute("""DELETE FROM illuminocracy.top_ads WHERE 1=1""")

bq_execute("""INSERT INTO illuminocracy.top_ads (
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
    HAVING SUM(AD.spend_gbp_mid) > 100""")
