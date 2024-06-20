import pandas as pd
import psycopg2
import numpy as np
import os
from dotenv import load_dotenv
import datetime
from psycopg2.extensions import register_adapter, AsIs
import traceback
import json


def get_campaign_data(host, database, user, password, port, created):
    conn = psycopg2.connect(host=host,
                            database=database,
                            user=user,
                            password=password,
                            port=port)
    cursor = conn.cursor()
    cursor.execute("with campaigns_adsets_new as "
                    "(select distinct campaign_id, adset_id from campaigns_adsets) "
                    "SELECT fad.date, fad.campaign_id, fad.campaign_name, fad.Spend, fad.Clicks, gam.'Banner revenue', gam.'Video revenue' "
                    "FROM facebook_ads_data fad "
                    "left join campaigns_adsets_new can "
                    "on fad.campaign_id=can.campaign_id "
                    "left join google_ad_manager_revenue_data gam "
                    "on gam.adset_id=can.adset_id and gam.date=fad.date where fad.date > '" + created + "'")
    campaigns_data = pd.DataFrame(cursor.fetchall(),
                          columns=['date', 'campaign_id', 'campaign_name', 'spend', 'clicks', 'banner_revenue',
                                   'video_revenue'])

    return campaigns_data


def transform_campaigns_data(campaigns_data):
    #campaigns_data = pd.read_csv('a.csv')
    campaigns_data['revenue'] = campaigns_data['Banner revenue'] + campaigns_data['Video revenue']
    campaigns_data = campaigns_data.groupby(['campaign_name']).agg(spend=('Spend', 'sum'),
                                                                revenue=('revenue', 'sum'),
                                                               clicks=('Clicks', 'sum'),
                                                                start_date=('date', 'min')).reset_index(drop=False)
    campaigns_data['revenue>spend'] = np.where(campaigns_data['revenue'] > campaigns_data['spend'], True, False)
    campaigns_data['cpc'] = round(campaigns_data['spend']/campaigns_data['clicks'], 2)
    campaigns_data['roas'] = round(campaigns_data['revenue']/campaigns_data['spend']*100, 2)
    campaigns_data[['article_id', 'type', 'version', 'platform', 'author', 'media']] = campaigns_data['campaign_name'].str.split(expand=True)
    campaigns_data = campaigns_data[['campaign_name', 'spend', 'revenue', 'clicks', 'start_date', 'revenue>spend', 'cpc', 'romi',
                                 'article_id', 'author', 'media']]
    return campaigns_data


def save_campaigns_data_to_db(campaigns_data, host, database, user, password, port):
    conn = psycopg2.connect(host=host,
                            database=database,
                            user=user,
                            password=password,
                            port=port)
    cur = conn.cursor()
    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
    records = campaigns_data.to_records(index=False)
    result = list(records)
    args_str = ','.join(
        cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", x).decode("utf-8") for x in
        result)
    cur.execute(
        "INSERT INTO campaigns_data (campaign_name, spend, revenue, clicks, start_date, revenue_vs_spend, cpc, roas, article_id, author, media) VALUES"
        + args_str + "ON CONFLICT (campaign_name) DO UPDATE SET spend = EXCLUDED.spend, revenue = EXCLUDED.revenue, clicks = EXCLUDED.clicks, start_date=EXCLUDED.start_date, revenue_vs_spend = EXCLUDED.revenue_vs_spend, cpc=EXCLUDED.cpc, roas=EXCLUDED.roas, "
                     "article_id=EXCLUDED.article_id, author=EXCLUDED.author, media=EXCLUDED.media")
    conn.commit()
    cur.close()
    conn.close()


def main_amo():
    try:
        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env'))
        created = datetime.date.today() - datetime.timedelta(days=7)
        created = created.strftime('%Y-%m-%d')
        campaigns_data = get_campaign_data(host=os.getenv('host'),
                                        database=os.getenv('database'),
                                        user=os.getenv('user'),
                                        password=os.getenv('password'),
                                        port=os.getenv('port'), created=created)
        campaigns_data = transform_campaigns_data(campaigns_data)
        save_campaigns_data_to_db(campaigns_data, host=os.getenv('visual_host'),
                                        database=os.getenv('visual_database'),
                                        user=os.getenv('visual_user'),
                                        password=os.getenv('visual_password'),
                                        port=os.getenv('visual_port'))
    except Exception:
        with open('amo_exceptions.json', 'w') as fp:
            print(traceback.format_exc())
            json.dump(traceback.format_exc(), fp)

