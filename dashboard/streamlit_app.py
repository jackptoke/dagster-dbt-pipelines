from typing import List, Any

import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import duckdb
from duckdb import DuckDBPyRelation

"""
# Welcome to Coco Data Lab!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:.
If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""

DUCKDB_FILE = './data/staging/realestate.duckdb'


def get_properties(q: str) -> pd.DataFrame:
    with duckdb.connect(DUCKDB_FILE, read_only=True) as conn:
        return conn.sql(q).df()

suburb_query = "select distinct A.suburb from dim_address A join fct_sold_listing S on S.address_id = A.address_id;"
years_query = "select distinct D.year from dim_date D join fct_sold_listing S on S.date_id = D.date_id order by D.year DESC;"

suburbs = get_properties(suburb_query)
years = get_properties(years_query)

st.header("Available Suburbs")

selected_suburb = st.selectbox(label='Select Suburb', options=suburbs["suburb"], key="sb_suburbs")
selected_year = st.selectbox(label='Select Year', options=years["year"])

st.subheader(f"Selected Suburb: {selected_suburb}")

st.header("Scatter Plot of Properties")
tab1, tab2 = st.tabs(["Tarneit Properties", "Baccus Marsh Properties"])

query = """
            select listing_id, num_bedrooms, num_bathrooms, num_parking_spaces, land_size, L.price, A.suburb, D.year 
            from fct_sold_listing L join dim_date D on D.date_id = L.date_id join dim_address A on A.address_id = L.address_id 
            where A.suburb = '{suburb}' and D.year = {year} and L.price > 0;
            """

tarneit_properties_df = get_properties(query.format(suburb=selected_suburb, year=selected_year))

tarneit_fig = px.scatter(data_frame=tarneit_properties_df,
                         x='land_size',
                         y='price',
                         color='num_bedrooms',
                         size='num_bathrooms',
                         hover_data=["year", "price", "num_bedrooms", "num_bathrooms", "land_size"],
                         title=f'{selected_suburb} Properties',)


with tab1:
    st.plotly_chart(tarneit_fig, key="properties_tab", on_select="rerun", theme="streamlit", use_container_width=True)

with tab2:
    st.text("Coming soon ...")
