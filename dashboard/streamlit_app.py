from typing import List, Any

import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import duckdb
from duckdb import DuckDBPyRelation
import math

st.set_page_config(layout="wide")

"""
# Australia Real Estate

The source code of the whole pipeline can be found my [Github repo](https://github.com/jackptoke/dagster-dbt-pipelines).

If you are looking to build a robust data pipeline complete with a dashboard using Dagster, DBT and Streamlit, please,
don't hesitate to contact me at jack.toke@cocolab.com.au or 0455-149-451.
"""

DUCKDB_FILE = './data/staging/realestate.duckdb'


def query_db(q: str) -> pd.DataFrame:
    with duckdb.connect(DUCKDB_FILE, read_only=True) as conn:
        return conn.sql(q).df()

def format_currency(value):
    try:
        num = float(value)
        if math.isnan(num):
            return "-"
    except ValueError:
        return "-"
    return "${:,.2f}".format(num)


suburb_query = "select distinct A.suburb from dim_address A join fct_sold_listing S on S.address_id = A.address_id order by A.suburb asc;"
years_query = "select distinct D.year from dim_date D join fct_sold_listing S on S.date_id = D.date_id order by D.year DESC;"
query = """
select listing_id, num_bedrooms AS 'Number of beds', num_bathrooms AS 'Number of bathrooms', num_parking_spaces, land_size AS 'Land size (㎡)', L.price AS 'Price', A.suburb, D.year AS 'Year' 
from fct_sold_listing L join dim_date D on D.date_id = L.date_id join dim_address A on A.address_id = L.address_id 
where A.suburb = '{suburb}' and D.year = {year} and L.price > 0;
"""

suburbs_df = query_db(suburb_query)
years_df = query_db(years_query)

suburbs = [suburb for suburb in suburbs_df['suburb'].unique()]
years = [year for year in years_df['year'].unique()]

st.header("Australia Real Estate Data")

@st.fragment
def show_realestate_data():
    with st.form(key='realestate_form', clear_on_submit=True):
        suburb_col, year_col = st.columns(2)
        selected_suburb = suburb_col.selectbox(label='Select Suburb', options=suburbs, key="sb_suburbs")
        selected_year = year_col.selectbox(label='Select Year', options=years, key="sb_years")
        submit = st.form_submit_button(label='Submit')

    tarneit_properties_df = query_db(query.format(suburb=selected_suburb, year=selected_year))

    tarneit_fig = px.scatter(data_frame=tarneit_properties_df,
                             x='Land size (㎡)',
                             y='Price',
                             color='Number of beds',
                             size='Number of bathrooms',
                             hover_data=["Year", "Price", "Number of beds", "Number of bathrooms", "Land size (㎡)"],
                             title=f'{selected_suburb} - {selected_year} Properties',)

    # Add tabs
    if submit:
        avg_2_and_under_bedrooms_price = tarneit_properties_df[tarneit_properties_df['Number of beds'] <= 2]["Price"].mean()
        avg_3_bedrooms_price = tarneit_properties_df[tarneit_properties_df['Number of beds'] == 3]["Price"].mean()
        avg_4_bedrooms_price = tarneit_properties_df[tarneit_properties_df['Number of beds'] == 4]["Price"].mean()
        avg_5_bedrooms_and_up_price = tarneit_properties_df[tarneit_properties_df['Number of beds'] >= 5]["Price"].mean()

        avg_col1, avg_col2, avg_col3, avg_col4 = st.columns([1, 1, 1, 1])

        avg_col1.metric(label="Avg. Price < 2 bedrooms", value=format_currency(avg_2_and_under_bedrooms_price))
        avg_col2.metric(label="Avg. Price 3 bedrooms", value=format_currency(avg_3_bedrooms_price))
        avg_col3.metric(label="Avg. Price 4 bedrooms", value=format_currency(avg_4_bedrooms_price))
        avg_col4.metric(label="Avg. Price 5+ bedrooms", value=format_currency(avg_5_bedrooms_and_up_price) if avg_5_bedrooms_and_up_price else '-')

        tarneit_fig.update_layout(height=800)
        st.plotly_chart(tarneit_fig,
                        key="properties_tab",
                        on_select="rerun",
                        theme=None,
                        height=800,
                        use_container_width=True)
    else:
        st.subheader("Please select a suburb and year")

show_realestate_data()
