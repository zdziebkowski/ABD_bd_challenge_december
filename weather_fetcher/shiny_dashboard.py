from shiny.express import input, render, ui
from shinywidgets import render_plotly
import plotly.express as px
import folium
from folium import plugins
from utils.database_connection import load_data

# Load data at startup
data = load_data()
df_temp = data['temperature_ranking']
df_weather = data['weather_codes']
df_map = data['map_data']

# Set page title
ui.page_opts(title="Weather Analysis Dashboard", fillable=True)

# Upper charts layout
with ui.layout_columns(col_widths=[6, 6]):
    # First chart - Temperature ranking
    with ui.card():
        ui.card_header("City Temperature Rankings")


        @render_plotly
        def temp_ranking():
            fig = px.bar(
                df_temp,
                x='source_city',
                y='avg_temp',
                title='Average Temperatures by City',
                labels={
                    'source_city': 'City',
                    'avg_temp': 'Average Temperature (°C)'
                }
            )

            fig.update_layout(
                xaxis_tickangle=-45,
                margin=dict(b=100),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )

            fig.update_traces(
                marker_color='#1f77b4',
                hovertemplate="<b>%{x}</b><br>Temperature: %{y:.1f}°C<extra></extra>"
            )

            return fig

    # Second chart - Weather codes
    with ui.card():
        ui.card_header("Weather Code Distribution")


        @render_plotly
        def weather_ranking():
            fig = px.bar(
                df_weather,
                x='source_city',
                y='freq',
                color='weather_description',
                title='Weather Code Frequency by City',
                labels={
                    'source_city': 'City',
                    'freq': 'Frequency',
                    'weather_description': 'Weather Type'
                }
            )

            fig.update_layout(
                xaxis_tickangle=-45,
                margin=dict(b=100),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )

            return fig

# Map section
with ui.card(full_screen=True):
    ui.card_header("Temperature Map")


    @render.ui
    def weather_map():
        # Create base map
        m = folium.Map(
            location=[52.0, 19.0],  # Center of Poland
            zoom_start=6,
            tiles='CartoDB positron'
        )

        # Add markers for each city
        for idx, row in df_map.iterrows():
            # Create marker color based on temperature
            temp = row['avg_temp']
            color = 'red' if temp > 25 else 'blue' if temp < 0 else 'green'

            # Add marker
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=8,
                popup=f"{row['source_city']}: {temp:.1f}°C",
                color=color,
                fill=True,
                fill_color=color
            ).add_to(m)

        # Add legend
        legend_html = '''
            <div style="position: fixed; bottom: 50px; right: 50px; z-index: 1000; background-color: white; 
                      padding: 10px; border: 2px solid grey; border-radius: 5px">
                <h4 style="margin-top: 0;">Temperature Ranges:</h4>
                <p><i class="fa fa-circle" style="color:red"></i> > 25°C (Hot)</p>
                <p><i class="fa fa-circle" style="color:green"></i> 0-25°C (Moderate)</p>
                <p><i class="fa fa-circle" style="color:blue"></i> < 0°C (Cold)</p>
            </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))

        # Convert map to HTML
        return ui.HTML(m._repr_html_())