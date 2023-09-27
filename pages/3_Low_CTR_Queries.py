import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

#Get project credentials
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

#Get required dates
start = (datetime.date.today()-datetime.timedelta(30)).isoformat()
end = datetime.date.today().isoformat()

pre_start = (datetime.date.today()-datetime.timedelta(60)).isoformat()
pre_end = (datetime.date.today()-datetime.timedelta(31)).isoformat()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
def run_query(query):
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

rows = run_query('''
                WITH
                CTR_Data AS (
                SELECT
                    query,
                    SUM(clicks) AS total_clicks,
                    SUM(impressions) AS total_impressions,
                    SAFE_MULTIPLY(SAFE_DIVIDE(SUM(clicks), SUM(impressions)),100) AS CTR
                FROM
                    `bumblebee-233720.searchconsole_domain_property.searchdata_site_impression`
                WHERE
                    data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    AND CURRENT_DATE()
                    AND NOT REGEXP_CONTAINS(query, r'happy|hapy|happysocks|ハッピーソックス')
                    AND search_type = "WEB"
                    AND is_anonymized_query IS FALSE
                    AND impressions > 100
                GROUP BY
                    query ),
                Avg_CTR AS (
                SELECT
                    AVG(CTR) AS average_CTR
                FROM
                    CTR_Data )
                SELECT
                query,
                total_clicks,
                total_impressions,
                CTR
                FROM
                CTR_Data,
                Avg_CTR
                WHERE
                CTR < average_CTR
                ORDER BY
                CTR_Data.total_impressions DESC
                 ''')

st.write('# Queries with low click through rate(CTR)')
st.write('This table displays queries in the last 30 days that has lower CTR compared to the average CTR of all your queires')
st.write('''*The results only include queries from web search, doesn't include image or shopping search. Also only filtered to who queries that has more than 100 impressions*''')
st.write(rows)

st.write('### Suggestion on how to improve this?')
st.write('Try optimising the meta title and description for the respective landing pages. Try using ChatGPT to generate sample meta title and descriptions to improve CTR.')

