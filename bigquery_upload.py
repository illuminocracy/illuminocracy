#!/usr/bin/env python3

#python 3.6.5

from google.cloud import bigquery
from google.cloud import storage
import sqlite3
import datetime
import csv
import glob
import os
import time


gs_bucket = 'illuminocracy-facebook-ads'
gc_service_account = 'illuminocracy'
gc_service_account_key = 'data-transparency-8a55cfd7dd39.json'
bq_dataset = 'illuminocracy'

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect('staging.db')
conn.row_factory = dict_factory
c = conn.cursor()
c.execute("PRAGMA foreign_keys = ON")

today = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
current_time = datetime.datetime.now()

# refresh the list of dates to generate daily rows

c.execute("DELETE FROM days")
c.execute("SELECT MIN(date(SUBSTR(delivery_start_time,1,10))) AS min_date FROM ads")
result = c.fetchall()
min_date = result[0]['min_date']

c.execute("SELECT MAX(date(SUBSTR(delivery_start_time,1,10))) AS max_date FROM ads")
result = c.fetchall()
max_date = result[0]['max_date']

print("Min date of all ads is: %s" % (min_date))
print("Max date of all ads is: %s" % (max_date))

day = datetime.datetime.strptime(min_date, "%Y-%m-%d").date()
last_day = datetime.datetime.strptime(max_date, "%Y-%m-%d").date()

while day < last_day:
    c.execute("INSERT INTO days (day) VALUES (?)",
        [day])
    day += datetime.timedelta(days=1)

# define function to pull data out of sqlite into csv

def query_to_csv(sql, target_file, columns):
    print("Extracting data to %s" % (target_file))

    c.execute(sql)
    result = c.fetchall()
    rows = 0
    while rows < len(result):
        with open(target_file + '_part' + str(rows), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns) # header row
            for row in result[rows:rows+10000]:
                data = []
                for col in columns:
                    data += [row[col]]
                writer.writerow(data)
                rows += 1
    print("Extracted %d rows" % (rows))

def gcs_upload_blob(filename):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(gc_service_account_key)
    bucket = storage_client.get_bucket(gs_bucket)

    files = glob.glob(filename + "*")

    for file in files:
        gs_path = 'data_load/%s' % (file)
        blob = bucket.blob(gs_path)

        blob.upload_from_filename(file)

        print('File {} uploaded to {}.'.format(
            file,
            gs_path))

def bq_load_csv_in_gcs(filename, table, schema, write_disposition = 'WRITE_TRUNCATE'):
    bigquery_client = bigquery.Client.from_service_account_json(gc_service_account_key)
    dataset_ref = bigquery_client.dataset(bq_dataset)

    first_job = True

    files = glob.glob(filename + "*")

    for file in files:
        print('Loading {} to table {} with disposition {}...'.format(file, table, write_disposition))
        job_config = bigquery.LoadJobConfig()

        job_config.schema = schema
        job_config.skip_leading_rows = 1
        job_config.write_disposition = write_disposition
        job_config.allow_quoted_newlines = True
        job_config.quote_character = '"'

        load_job = bigquery_client.load_table_from_uri(
            'gs://%s/data_load/%s' % (gs_bucket, file),
            dataset_ref.table(table),
            job_config=job_config)

        assert load_job.job_type == 'load'

        try:
            load_job.result()  # Waits for table load to complete.
        except Exception as e:
            import traceback, sys
            print(traceback.format_exception(None, # <- type(e) by docs, but ignored
                                     e, e.__traceback__),
                                file=sys.stderr, flush=True)
            print(load_job.errors)

        assert load_job.state == 'DONE'
        print('Load completed, wrote %d rows.' % (load_job.output_rows))

        if first_job == True:
            first_job = False
            write_disposition = 'WRITE_APPEND'

        time.sleep(5) # try to avoid hitting BQ rate limits

def delete_local_csv(filename):
    files = glob.glob(filename + "*")
    for file in files:
        os.remove(file)
        print("Removed local file %s" % (file))

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



########## LOAD: days

query_to_csv("SELECT day FROM days", 'days.csv', ['day'])

gcs_upload_blob('days.csv')

schema = [
    bigquery.SchemaField('day', 'DATE', mode='REQUIRED')
]

bq_load_csv_in_gcs('days.csv', 'days', schema)

delete_local_csv('days.csv')

########## LOAD: funding_entity_affiliations

query_to_csv("SELECT funding_entity, affiliation_party, affiliation_brexit FROM funding_entity_affiliations WHERE funding_entity IS NOT NULL",
    'funding_entity_affiliations.csv',
    ['funding_entity','affiliation_party','affiliation_brexit'])

gcs_upload_blob('funding_entity_affiliations.csv')

schema = [
    bigquery.SchemaField('funding_entity', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('affiliation_party', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('affiliation_brexit', 'STRING')   # defaults to NULLABLE
]

bq_load_csv_in_gcs('funding_entity_affiliations.csv', 'funding_entity_affiliations', schema)

delete_local_csv('funding_entity_affiliations.csv')

########## LOAD: exchange_rates

query_to_csv("SELECT currency, rate_date, gbp_value FROM exchange_rates",
    'exchange_rates.csv',
    ['currency','rate_date','gbp_value'])

gcs_upload_blob('exchange_rates.csv')

schema = [
    bigquery.SchemaField('currency', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('rate_date', 'DATE', mode='REQUIRED'),  # defaults to NULLABLE
    bigquery.SchemaField('gbp_value', 'FLOAT64', mode='REQUIRED')   # defaults to NULLABLE
]

bq_load_csv_in_gcs('exchange_rates.csv', 'exchange_rates', schema)

delete_local_csv('exchange_rates.csv')



########## LOAD: ads

query_to_csv("""SELECT id,
                    strftime("%Y-%m-%d %H:%M:%S", substr(creation_time,1,19)) AS creation_time,
                    creative_body,
                    creative_link_caption,
                    creative_link_description,
                    creative_link_title,
                    strftime("%Y-%m-%d %H:%M:%S", substr(delivery_start_time,1,19)) AS delivery_start_time,
                    strftime("%Y-%m-%d %H:%M:%S", substr(delivery_stop_time,1,19)) AS delivery_stop_time,
                    currency,
                    snapshot_url,
                    page_id,
                    page_name,
                    funding_entity,
                    spend_lower,
                    spend_upper,
                    impressions_lower,
                    impressions_upper,
                    capture_date_time
  FROM ads WHERE uploaded = 0""",
    'ads.csv',
    ['id',
    'creation_time',
    'creative_body',
    'creative_link_caption',
    'creative_link_description',
    'creative_link_title',
    'delivery_start_time',
    'delivery_stop_time',
    'currency',
    'snapshot_url',
    'page_id',
    'page_name',
    'funding_entity',
    'spend_lower',
    'spend_upper',
    'impressions_lower',
    'impressions_upper',
    'capture_date_time'])

gcs_upload_blob('ads.csv')

schema = [
    bigquery.SchemaField('id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('creation_time', 'TIMESTAMP'),  # defaults to NULLABLE
    bigquery.SchemaField('creative_body', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('creative_link_caption', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('creative_link_description', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('creative_link_title', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('delivery_start_time', 'TIMESTAMP'),  # defaults to NULLABLE
    bigquery.SchemaField('delivery_stop_time', 'TIMESTAMP'),  # defaults to NULLABLE
    bigquery.SchemaField('currency', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('snapshot_url', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('page_id', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('page_name', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('funding_entity', 'STRING'),  # defaults to NULLABLE
    bigquery.SchemaField('spend_lower', 'INT64'),  # defaults to NULLABLE
    bigquery.SchemaField('spend_upper', 'INT64'),  # defaults to NULLABLE
    bigquery.SchemaField('impressions_lower', 'INT64'),  # defaults to NULLABLE
    bigquery.SchemaField('impressions_upper', 'INT64'),  # defaults to NULLABLE
    bigquery.SchemaField('capture_date_time', 'TIMESTAMP', mode='REQUIRED')   # defaults to NULLABLE
]

bq_load_csv_in_gcs('ads.csv', 'ads', schema, write_disposition = 'WRITE_APPEND')

c.execute("SELECT MIN(capture_date_time) AS refresh_cutoff FROM ads WHERE uploaded = 0")
result = c.fetchall()
refresh_cutoff = result[0]['refresh_cutoff']

c.execute("UPDATE ads SET uploaded = 1 WHERE uploaded = 0")

delete_local_csv('ads.csv')

# remove old versions of any duplicate ads

query_params = [
    bigquery.ScalarQueryParameter("refresh_cutoff", "TIMESTAMP", refresh_cutoff)
]
bq_execute("""DELETE FROM illuminocracy.ads WHERE id IN
    (SELECT id FROM illuminocracy.ads GROUP BY id HAVING COUNT(*) > 1 ) AND capture_date_time < @refresh_cutoff""", query_params)



########## LOAD: ad_keywords

query_to_csv("SELECT ad_id, keyword FROM ad_keywords WHERE uploaded = 0",
    'ad_keywords.csv',
    ['ad_id', 'keyword'])

gcs_upload_blob('ad_keywords.csv')

schema = [
    bigquery.SchemaField('ad_id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('keyword', 'STRING', mode='REQUIRED')  # defaults to NULLABLE
]

bq_load_csv_in_gcs('ad_keywords.csv', 'ad_keywords', schema, write_disposition = 'WRITE_APPEND')

c.execute("UPDATE ad_keywords SET uploaded = 1 WHERE uploaded = 0")

delete_local_csv('ad_keywords.csv')


########## LOAD: ads_distribution_region

query_to_csv("SELECT ad_id, region, percentage FROM ads_distribution_region WHERE uploaded = 0",
    'ads_distribution_region.csv',
    ['ad_id', 'region', 'percentage'])

gcs_upload_blob('ads_distribution_region.csv')

schema = [
    bigquery.SchemaField('ad_id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('region', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('percentage', 'FLOAT64', mode='REQUIRED')  # defaults to NULLABLE
]

bq_load_csv_in_gcs('ads_distribution_region.csv', 'ads_distribution_region', schema, write_disposition = 'WRITE_APPEND')

c.execute("UPDATE ads_distribution_region SET uploaded = 1 WHERE uploaded = 0")

delete_local_csv('ads_distribution_region.csv')


########## LOAD: ads_distribution_demographics

query_to_csv("SELECT ad_id, age, gender, percentage FROM ads_distribution_demographics WHERE uploaded = 0",
    'ads_distribution_demographics.csv',
    ['ad_id', 'age', 'gender', 'percentage'])

gcs_upload_blob('ads_distribution_demographics.csv')

schema = [
    bigquery.SchemaField('ad_id', 'INT64', mode='REQUIRED'),
    bigquery.SchemaField('age', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('gender', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('percentage', 'FLOAT64', mode='REQUIRED')  # defaults to NULLABLE
]

bq_load_csv_in_gcs('ads_distribution_demographics.csv', 'ads_distribution_demographics', schema, write_disposition = 'WRITE_APPEND')

c.execute("UPDATE ads_distribution_demographics SET uploaded = 1 WHERE uploaded = 0")

delete_local_csv('ads_distribution_demographics.csv')
