import math
import os

import duckdb
import matplotlib.colors as mcolors
import pandas as pd
import pydeck as pdk
import streamlit as st

st.set_page_config(layout="wide")

"""
# Australian Real Estate

The source code of the whole pipeline can be found my [Github repo](https://github.com/jackptoke/dagster-dbt-pipelines).

If you are looking to build a robust data pipeline complete with a dashboard using Dagster, DBT and Streamlit, please,
don't hesitate to contact me at jack.toke@cocolab.com.au or 0455-149-451.
"""

DUCKDB_FILE = f'{os.getenv("MOTHER_DUCK_URL")}?motherduck_token={os.getenv("MOTHER_DUCK_TOKEN")}'


def assign_color_based_on_price_and_bedrooms(data_df, price_column, bedroom_column):
    # Define the function to group bedrooms
    def group_bedrooms(bedrooms):
        if bedrooms <= 2:
            return '2 or less'
        elif bedrooms == 3:
            return '3'
        elif bedrooms == 4:
            return '4'
        else:
            return '5 or more'

    # Apply the bedroom grouping function to create a new column
    data_df['Bedroom_Group'] = data_df[bedroom_column].apply(group_bedrooms)

    # Group by the Bedroom_Group and calculate the mean price for each group
    avg_prices = data_df.groupby('Bedroom_Group')[price_column].mean().to_dict()

    # Define the color map
    cmap = mcolors.LinearSegmentedColormap.from_list("", ["green", "yellow", "red"])

    # Create an empty list to store color values
    color_values = []

    # Iterate over the DataFrame rows
    for idx, row in data_df.iterrows():
        bedroom_group = row['Bedroom_Group']
        price = row[price_column]

        # Get the average price for this bedroom group
        avg_price = avg_prices[bedroom_group]

        # Normalize this price such that values close to mean are close to 0.5
        norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=avg_price, vmax=2 * avg_price)

        # Append the color gradient based on the normalized data
        color_values.append(mcolors.to_hex(cmap(norm(price))))

    # Assign the color values to a new column in the DataFrame
    data_df['Color'] = color_values

    return data_df


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


suburb_query = """
select A.suburb, COUNT(S.listing_id) AS num_props 
from dim_address A join fct_sold_listing S on S.address_id = A.address_id 
WHERE S.price > 0 
GROUP BY A.suburb 
HAVING num_props > 1 
order by A.suburb asc"""
years_query = "select distinct D.year from dim_date D join fct_sold_listing S on S.date_id = D.date_id order by D.year DESC;"
query = """
    select listing_id, num_bedrooms AS 'Bedrooms', num_bathrooms AS 'Bathrooms', 
    num_parking_spaces, land_size AS 'Land', L.price AS 'Price', 
    A.latitude AS 'Latitude', A.longitude AS 'Longitude',
    CONCAT_WS(', ', A.street_address, A.suburb, A.state, A.postcode) AS 'Address', A.suburb AS 'Suburb',
    D.year AS 'Year' 
    from fct_sold_listing L 
    join dim_date D on D.date_id = L.date_id 
    join dim_address A on A.address_id = L.address_id 
    where A.suburb = '{suburb}' and D.year = {year} and L.price > 0
    AND A.latitude <> 0 AND A.longitude <> 0
"""
suburb_geo_query = "SELECT suburb, lat AS Latitude, lng AS Longitude FROM realestate_dev.public.raw_suburbs WHERE suburb = '{suburb}' LIMIT 1"

suburbs_df = query_db(suburb_query)
years_df = query_db(years_query)

suburbs = [suburb for suburb in suburbs_df['suburb'].unique()]
years = [year for year in years_df['year'].unique()]

st.header("Australia Real Estate Data")


@st.fragment
def show_realestate_data():
    with st.form(key='realestate_form', clear_on_submit=False):
        suburb_col, year_col = st.columns(2)
        selected_suburb = suburb_col.selectbox(label='Select Suburb', options=suburbs, key="sb_suburbs")
        selected_year = year_col.selectbox(label='Select Year', options=years, key="sb_years")
        submit = st.form_submit_button(label='Submit')

    properties_df = query_db(query.format(suburb=selected_suburb, year=selected_year))
    map_properties_df = properties_df[(properties_df["Latitude"] != 0) & (properties_df["Longitude"] != 0)].copy()

    suburb_geo_data = query_db(suburb_geo_query.format(suburb=selected_suburb))

    # tarneit_fig = px.scatter(data_frame=properties_df,
    #                          x='Land',
    #                          y='Price',
    #                          color='Bedrooms',
    #                          size='Bathrooms',
    #                          hover_data=["Year", "Price", "Bedrooms", "Bathrooms", "Land"],
    #                          title=f'{selected_suburb} - {selected_year} Scatter Chart', )

    # Add tabs
    if submit:
        avg_2_and_under_bedrooms_price = properties_df[properties_df['Bedrooms'] <= 2]["Price"].mean()
        avg_3_bedrooms_price = properties_df[properties_df['Bedrooms'] == 3]["Price"].mean()
        avg_4_bedrooms_price = properties_df[properties_df['Bedrooms'] == 4]["Price"].mean()
        avg_5_bedrooms_and_up_price = properties_df[properties_df['Bedrooms'] >= 5]["Price"].mean()

        avg_col1, avg_col2, avg_col3, avg_col4 = st.columns([1, 1, 1, 1])

        avg_col1.metric(label="Avg. Price < 2 bedrooms", value=format_currency(avg_2_and_under_bedrooms_price))
        avg_col2.metric(label="Avg. Price 3 bedrooms", value=format_currency(avg_3_bedrooms_price))
        avg_col3.metric(label="Avg. Price 4 bedrooms", value=format_currency(avg_4_bedrooms_price))
        avg_col4.metric(label="Avg. Price 5+ bedrooms",
                        value=format_currency(avg_5_bedrooms_and_up_price) if avg_5_bedrooms_and_up_price else '-')

        ## Simple MAP
        st.subheader(f"{selected_suburb} - {selected_year} Properties")

        colour_map_properties = assign_color_based_on_price_and_bedrooms(map_properties_df, "Price",
                                                                         "Bedrooms")

        # st.map(colour_map_properties, latitude='Latitude', longitude='Longitude', size="Bedrooms",
        #        color="Color")

        ## PyDeck Map
        chart_data = colour_map_properties[["Latitude", "Longitude", "Bedrooms", "Bathrooms", "Price", "Address", "Land",
                                            "Color"]].copy()
        avg_price = chart_data["Price"].fillna(0).mean()

        if avg_price > 300000:
            ratio = 10000
        elif avg_price > 100000:
            ratio = 5000
        else:
            ratio = 3000
        chart_data["size"] = chart_data.Price / ratio

        properties_point_layer = pdk.Layer(
            "ScatterplotLayer",
            data=chart_data,
            get_position="[Longitude, Latitude]",
            get_color="[153, 204, 0]",
            pickable=True,
            auto_highlight=True,
            get_radius="size",
        )

        latitude = suburb_geo_data.loc[0, "Latitude"]
        longitude = suburb_geo_data.loc[0, "Longitude"]

        view_state = pdk.ViewState(
            latitude=latitude,
            longitude=longitude,
            zoom=14,
            pitch=50,
        )

        chart = pdk.Deck(
            map_style=None,
            layers=[properties_point_layer],
            initial_view_state=view_state,
            tooltip={"text": """Price: ${Price}  
            Bedrooms: {Bedrooms} 
            Bathrooms: {Bathrooms}
            Land size: {Land} ㎡
            Address: {Address}"""},
        )

        st.pydeck_chart(chart, on_select="ignore", selection_mode="multi-object", height=1000, width=1200)

        ## Scatter Plot
        # tarneit_fig.update_layout(height=800)
        # st.plotly_chart(tarneit_fig,
        #                 key="properties_tab",
        #                 on_select="rerun",
        #                 theme=None,
        #                 height=800,
        #                 use_container_width=True)
    else:
        st.subheader("Please select a suburb and year")


show_realestate_data()
