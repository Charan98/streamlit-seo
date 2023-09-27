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
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results

rows = run_query('''
                WITH
                Period1 AS (
                SELECT
                    query,
                    ROUND(AVG(sum_top_position),0) AS avg_position_1,
                    SUM(clicks) AS total_clicks_1,
                    SUM(impressions) AS total_impressions_1
                FROM
                    `bumblebee-233720.searchconsole_domain_property.searchdata_site_impression`
                WHERE
                    data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
                    AND NOT REGEXP_CONTAINS(query, r'happy|hapy|happysocks|ハッピーソックス')
                    AND country = 'usa'
                    AND search_type = "WEB"
                    AND is_anonymized_query IS FALSE
                GROUP BY
                    query ),
                Period2 AS (
                SELECT
                    query,
                    ROUND(AVG(sum_top_position),0) AS avg_position_2,
                    SUM(clicks) AS total_clicks_2,
                    SUM(impressions) AS total_impressions_2
                FROM
                    `bumblebee-233720.searchconsole_domain_property.searchdata_site_impression`
                WHERE
                    data_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    AND CURRENT_DATE()
                    AND NOT REGEXP_CONTAINS(query, r'happy|hapy|happysocks|ハッピーソックス')
                    AND country = 'usa'
                    AND search_type = "WEB"
                    AND is_anonymized_query IS FALSE
                GROUP BY
                    query )
                SELECT
                Period1.query,
                Period1.avg_position_1,
                Period2.avg_position_2,
                Period1.total_clicks_1,
                Period2.total_clicks_2,
                Period2.total_clicks_2 - Period1.total_clicks_1 AS click_difference,
                Period1.total_impressions_1,
                Period2.total_impressions_2,
                Period2.total_impressions_2 - Period1.total_impressions_1 AS impression_difference
                FROM
                Period1
                JOIN
                Period2
                ON
                Period1.query = Period2.query
                WHERE
                Period1.avg_position_1 > Period2.avg_position_2
                AND Period2.avg_position_2 BETWEEN 10
                AND 20
                ORDER BY
                click_difference DESC;
                 ''')

def style_arrows(value):
    """Return a styled arrow based on the value."""
    if value > 0:
        return f'<span style="color:green;">&uarr; {value}</span>'
    elif value < 0:
        return f'<span style="color:red;">&darr; {abs(value)}</span>'
    else:
        return str(value)

def display_styled_table(dataframe):
    """Style the dataframe and display it using st.write."""
    dataframe['click_difference_arrow'] = dataframe['click_difference'].apply(style_arrows)
    dataframe['impression_difference_arrow'] = dataframe['impression_difference'].apply(style_arrows)
    
    # Convert the dataframe to HTML and display
    st.write(dataframe.to_html(escape=False, columns=['query', 'avg_position_1', 'avg_position_2', 'total_clicks_1', 'total_clicks_2', 'click_difference_arrow', 'total_impressions_1', 'total_impressions_2', 'impression_difference_arrow']), unsafe_allow_html=True)

# Fetch data and display styled table
display_styled_table(rows)

