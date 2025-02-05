from shiny.express import ui, render
from shinywidgets import render_plotly
import plotly.express as px
import folium
import pandas as pd

# Set Shiny Express Page Options
ui.page_opts(title="Weather Data Analysis", fillable=True)

# Data load
df_temp = pd.read_csv("csv_shiny_data/temperature_ranking.csv")
df_weather = pd.read_csv("csv_shiny_data/weather_codes.csv")
df_map = pd.read_csv("csv_shiny_data/map_data.csv")

# Layout
with ui.nav_panel("Analysis"):
    # First row: Two-column layout
    with ui.layout_column_wrap(width="100%"):
        with ui.layout_columns(col_widths=[6, 6]):
            # Temperature Rankings Card
            with ui.card():
                "City Temperature Rankings"

                @render_plotly
                def temp_ranking():
                    fig = px.bar(
                        df_temp,
                        x="source_city",
                        y="avg_temp",
                        title="Average Temperatures by City",
                    )
                    return fig

            # Weather Distribution Card
            with ui.card():
                "Weather Code Distribution"

                @render_plotly
                def weather_ranking():
                    fig = px.bar(
                        df_weather,
                        x="source_city",
                        y="freq",
                        color="weather_description",
                        title="Weather Code Frequency by City",
                    )
                    return fig

    # Second row: Full-width map card
    with ui.layout_column_wrap(width="100%"):
        with ui.card(full_screen=True):
            "Temperature Map"

            @render.ui
            def weather_map():
                # Build folium.Map
                m = folium.Map(location=[52.0, 19.0], zoom_start=6, tiles="CartoDB positron")

                # Add markers for each city
                for idx, row in df_map.iterrows():
                    temp = row["avg_temp"]
                    color = "red" if temp > 25 else "blue" if temp < 0 else "green"

                    folium.CircleMarker(
                        location=[row["latitude"], row["longitude"]],
                        radius=8,
                        popup=f"{row['source_city']}: {temp:.1f}°C",
                        color=color,
                        fill=True,
                        fill_color=color,
                    ).add_to(m)

                # Add legend
                legend_html = """
                    <div style="position: fixed; bottom: 50px; right: 50px; z-index: 1000; 
                              background-color: white; padding: 10px; border: 2px solid grey; 
                              border-radius: 5px">
                        <h4 style="margin-top: 0;">Temperature Ranges:</h4>
                        <p><i class="fa fa-circle" style="color:red"></i> > 25°C (Hot)</p>
                        <p><i class="fa fa-circle" style="color:green"></i> 0-25°C (Moderate)</p>
                        <p><i class="fa fa-circle" style="color:blue"></i> < 0°C (Cold)</p>
                    </div>
                """
                m.get_root().html.add_child(folium.Element(legend_html))

                return ui.HTML(m._repr_html_())