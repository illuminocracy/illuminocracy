#!/usr/bin/env python3

#python 3.6.5

import requests
import json
import base64
import sqlite3
import datetime
import yaml
import re

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

with open("config.yaml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

conn = sqlite3.connect('staging.db')
conn.row_factory = dict_factory
c = conn.cursor()
c.execute("PRAGMA foreign_keys = ON")

#db = TinyDB("%s.json" % (search_term))

access_token = cfg['access_token']

print("using facebook access token: '%s'" % (access_token))

#c.execute("SELECT id FROM ads")
#result = c.fetchall()
#all_ids = [r['id'] for r in result] # get first column of each result row
#
#print("existing ads in database: %d" % (len(all_ids)))

c.execute("SELECT term FROM search_terms")
result = c.fetchall()
search_terms = [r['term'] for r in result] # get first column of each result row

print("search terms to download (%d): %s" % (len(search_terms), search_terms))



#exit()

fields_to_return = [
    'page_id',
    'publisher_platforms',
    'impressions',
    'page_name',
    'spend',
    'funding_entity',
    'currency',
    'demographic_distribution',
    'region_distribution',
    'ad_creative_body',
    'ad_delivery_start_time',
    'ad_delivery_stop_time',
    'ad_snapshot_url',
    'ad_creative_link_title',
    'ad_creative_link_description',
    'ad_creative_link_caption',
    'ad_creation_time']

fields_to_return_joined = ",".join(fields_to_return)
#active only
#initial_request = """https://graph.facebook.com/v4.0/ads_archive?search_terms='""" + search_term + """'&ad_reached_countries=['GB']&access_token="""+ access_token+"""&ad_type=POLITICAL_AND_ISSUE_ADS&fields=page_id,impressions,page_name,spend,funding_entity,currency,demographic_distribution,region_distribution,ad_creative_body,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,ad_creative_link_title,ad_creative_link_description,ad_creative_link_caption,ad_creation_time"""

#both active and inactive
#initial_request = """https://graph.facebook.com/v4.0/ads_archive?search_terms='""" + search_term + """'&ad_reached_countries=['GB']&access_token=""" + access_token + """&ad_type=POLITICAL_AND_ISSUE_ADS&ad_active_status=ALL&fields=page_id,impressions,page_name,spend,funding_entity,currency,demographic_distribution,region_distribution,ad_creative_body,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,ad_creative_link_title,ad_creative_link_description,ad_creative_link_caption,ad_creation_time"""

#active only
ads_active = 'ACTIVE'

#active and inactive
#ads_active = 'ALL'

# on all platforms

for search_term in search_terms:

    initial_request = """https://graph.facebook.com/v4.0/ads_archive?search_terms='""" + search_term + """'&ad_reached_countries=['GB']&access_token=""" + access_token + """&publisher_platform=['FACEBOOK','INSTAGRAM','AUDIENCE_NETWORK','MESSENGER','WHATSAPP']&ad_type=POLITICAL_AND_ISSUE_ADS&ad_active_status=""" + ads_active + """&fields=""" + fields_to_return_joined

    #timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


    url = initial_request
    running = True
    iteration = 0

    print("Downloading data for search term '%s'" % (search_term))

    while running:
        print("Requesting URL: %s" % (url))

        # Download ads

        response = requests.get(url)
        data = response.json()

        if 'error' in data.keys():
            print("There was an error: %s" % data['error']['message'])
            break

        ads = data['data']
        print("Received %d ads for iteration %05d" % (len(ads), iteration))

        # Process ads

        for ad in ads:

            # Parse data

            ad_data = {}
            ad_data['id'] = re.findall(r'id=(\d+)\&', ad['ad_snapshot_url'])[0]
            ad['ad_snapshot_url'] = 'https://www.facebook.com/ads/library/?id=' + str(ad_data['id'])
            for field in ('creation_time','creative_body','creative_link_caption','creative_link_description','creative_link_title',
                'delivery_start_time','delivery_stop_time','snapshot_url'):
                ad_data[field] = ad.get('ad_' + field, None)

            for field in ('currency','page_id','page_name','funding_entity'):
                ad_data[field] = ad.get(field, None)

            ad_data['keyword'] = search_term
            if 'spend' in list(ad.keys()):
                ad_data['spend_lower'] = ad['spend'].get('lower_bound', None)
                ad_data['spend_upper'] = ad['spend'].get('upper_bound', None)
            else:
                ad_data['spend_lower'] = None
                ad_data['spend_upper'] = None

            if 'impressions' in list(ad.keys()):
                ad_data['impressions_lower'] = ad['impressions'].get('lower_bound', None)
                ad_data['impressions_upper'] = ad['impressions'].get('upper_bound', None)
            else:
                ad_data['impressions_lower'] = None
                ad_data['impressions_upper'] = None


            capture_date_time = datetime.datetime.now()
            uploaded = 0

            #print(ad_data)

            ad_distribution_region = []
            for region in ad.get('region_distribution', []):
                ad_distribution_region += [{'region': region['region'],
                    'percentage': float(region['percentage'])}]

            #print(ad_distribution_region)

            ad_distribution_demographics = []
            for demographic in ad.get('demographic_distribution', []):
                ad_distribution_demographics += [{'age': demographic['age'],
                    'gender': demographic['gender'], 'percentage': float(demographic['percentage'])}]

            #print(ad_distribution_demographics)

            # Check if this is an ad we have seen before

            ad_to_be_inserted = True

            # Check if we need to update it - get existing ad from db

            # only fetch the fields we are interested in comparing
            c.execute("""SELECT
                --creation_time,
                creative_body,
                creative_link_caption,
                creative_link_description,
                creative_link_title,
                --delivery_start_time,
                --delivery_stop_time,
                currency,
                snapshot_url,
                page_id,
                page_name,
                funding_entity,
                spend_lower,
                spend_upper,
                impressions_lower,
                impressions_upper
                FROM ads WHERE id = ?""", [ad_data['id']])
            result = c.fetchall()

            # if the ad already exists in the DB
            if(len(result) > 0):


                db_ad = result[0]

                ad_to_be_inserted = False
                ad_changed = False

                print("AD EXISTS in database") #:" + str(db_ad))
                for key in db_ad:
                    if str(db_ad[key]) != str(ad_data.get(key, None)):
                        print("AD DIFFERENCE FOR KEY %s LOCAL VER: '%s' DB VER: '%s'" % (key, db_ad[key], ad_data.get(key, None)))
                        ad_changed = True

                if ad_changed == False:
                    c.execute("SELECT region, percentage FROM ads_distribution_region WHERE ad_id = ?", [ad_data['id']])
                    result = c.fetchall()
                    if result != ad_distribution_region:
                        ad_changed = True
                        print("AD DIFFERENCE FOR distribution_region LOCAL VER: '%s' DB VER: '%s'" % (ad_distribution_region, result))

                if ad_changed == False:
                    c.execute("SELECT age, gender, percentage FROM ads_distribution_demographics WHERE ad_id = ?", [ad_data['id']])
                    result = c.fetchall()
                    if result != ad_distribution_demographics:
                        ad_changed = True
                        print("AD DIFFERENCE FOR distribution_demographics LOCAL VER: '%s' DB VER: '%s'" % (ad_distribution_demographics, result))

                if ad_changed == True:
                    c.execute("DELETE FROM ads_distribution_region WHERE ad_id = ?", [ad_data['id']])
                    c.execute("DELETE FROM ads_distribution_demographics WHERE ad_id = ?", [ad_data['id']])
                    c.execute("DELETE FROM ads WHERE id = ?", [ad_data['id']])
                    ad_to_be_inserted = True
                    print("AD HAS CHANGED so will be reinserted")

            if ad_to_be_inserted == True:
                print("INSERTING AD %s" % (ad_data['id']))
                c.execute("""INSERT INTO ads (
                    id,
                    creation_time,
                    creative_body,
                    creative_link_caption,
                    creative_link_description,
                    creative_link_title,
                    delivery_start_time,
                    delivery_stop_time,
                    currency,
                    snapshot_url,
                    page_id,
                    page_name,
                    funding_entity,
                    spend_lower,
                    spend_upper,
                    impressions_lower,
                    impressions_upper,
                    capture_date_time,
                    uploaded)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", [
                        ad_data['id'],
                        ad_data['creation_time'],
                        ad_data['creative_body'],
                        ad_data['creative_link_caption'],
                        ad_data['creative_link_description'],
                        ad_data['creative_link_title'],
                        ad_data['delivery_start_time'],
                        ad_data['delivery_stop_time'],
                        ad_data['currency'],
                        ad_data['snapshot_url'],
                        ad_data['page_id'],
                        ad_data['page_name'],
                        ad_data['funding_entity'],
                        ad_data['spend_lower'],
                        ad_data['spend_upper'],
                        ad_data['impressions_lower'],
                        ad_data['impressions_upper'],
                        datetime.datetime.now(),
                        0
                    ])

                for region in ad_distribution_region:
                    c.execute("INSERT INTO ads_distribution_region (ad_id, region, percentage, uploaded) VALUES (?,?,?,0)",
                        [ad_data['id'], region['region'], region['percentage']])

                for demographic in ad_distribution_demographics:
                    c.execute("INSERT INTO ads_distribution_demographics (ad_id, age, gender, percentage, uploaded) VALUES (?,?,?,?,0)",
                        [ad_data['id'], demographic['age'], demographic['gender'], demographic['percentage']])

            # associate this search term to the ad if it isn't already associated
            c.execute("SELECT keyword FROM ad_keywords WHERE ad_id = ?", [ad_data['id']])
            result = c.fetchall()
            all_ad_keywords = [r['keyword'] for r in result] # get first column of each result row
            if search_term not in all_ad_keywords:
                c.execute("INSERT INTO ad_keywords (ad_id, keyword, uploaded) VALUES (?,?,0)", [ad_data['id'], search_term])

            conn.commit()

        if 'paging' in data:
            if 'next' in data['paging']:
                next_url = data['paging']['next']
                url = next_url
            else:
                running = False
        else:
            running = False

        iteration += 1

        #for ad in ads:
        #    db.insert(ad)
        #    #print("Added ad to DB: %s" % (ad['ad_creative_body']) )

        #print("Database now has %d records" % (len(db)))

conn.commit()
conn.close()
