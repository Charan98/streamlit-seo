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

rows = run_query('''SELECT ROUND(sum_top_position,0) AS Position, COUNT(query) AS No_of_Queries
                    FROM `bumblebee-233720.searchconsole_domain_property.searchdata_site_impression`
                    WHERE NOT REGEXP_CONTAINS(query, r"^(happy|hapy|appy|ハッピーソックス)")
                        AND is_anonymized_query IS FALSE
                        AND search_type = 'WEB'
                        AND clicks > 10
                        AND sum_top_position < 20
                    GROUP BY Position
                    ORDER BY Position
                 ''')

fig = px.bar(rows, x="Position", y="No_of_Queries", labels={'Position': 'Position', 'No_of_Queries': 'Number of Queries'}, title="Position vs. Number of Queries")

st.plotly_chart(fig)