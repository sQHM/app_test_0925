import pandas as pd
import numpy as np
import json
from dash import Dash, html, dcc, callback, Output, Input, State, ctx
import dash_bootstrap_components as dbc
import plotly
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta

def generate_year_months(start_date, end_date):
        date_format = "%Y-%m-%d"
        start_date_obj = datetime.strptime(start_date, date_format)
        end_date_obj = datetime.strptime(end_date, date_format)

        year_months_list = []
        current_date = start_date_obj

        while current_date <= end_date_obj:
            year_months_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=32)
            current_date = current_date.replace(day=1)

        return year_months_list
    
def revert_okid_to_base(x):
    if x != x:
        return x
    if str(x).startswith('WCNH8'):
        return 'WCNH00' + x[6:]
    elif str(x).startswith('WCNH9'):
        return 'WCNH20' + x[6:]
    else:
        return x

def region_cleaner(x):
    if x  == 'China':
        return 'south'
    else:
        return x.lower()

def build_year_month(x, y):
    if x < 10:
        return str(y) + '-0' + str(x) + '-01'
    else:
        return str(y) + '-' + str(x) + '-01'
    
def type_cleaner(x):
    if x in ['PH', 'Ph', 'OTC']:
        return 'PH'
    elif x in ['HP', 'HP ', 'Hp']:
        return 'HP'
    else:
        return 'Others'


mapbox_access_token = 'pk.eyJ1IjoibWFuaGFvOTg0MyIsImEiOiJjbG1vbmRrYjEwNW1tMmxrN2ZldDY2dXVlIn0.nyjGJokLYQDtYD0goqGMwQ'


df_sales_b_master_init = pd.read_excel('master_sales.xlsx', sheet_name = 'Data')
df_translation_table = pd.read_excel('tranlation_table.xlsx')
df_territory = pd.read_excel('territory.xlsx')
df_translation_table = df_translation_table.drop_duplicates(subset = ['Name_file', 'OKID'])
df_territory = df_territory[df_territory['Covered'] == 'Y'].copy()
df_sales_b_master = df_sales_b_master_init.copy()

tr_dict_brand = {'Anpo IV':'Anpo','Anpo Oral':'Anpo','Oekolp':'Oekolp','Oest 80g':'Oestro','Utro 100mg':'Utro','Utro 200mg':'Utro','Oest 30g':'Oestro'}
tr_dict_cif = {'Utro 100mg':59.31,'Utro 200mg':56.33,'Anpo IV':22.41,'Anpo Oral':32.72,'Oest 80g':89.84,'Oekolp':105.9}

df_sales_b_master['price'] = df_sales_b_master.Product.apply(lambda x: tr_dict_cif.get(x,37.01))
df_sales_b_master['Value_RMB'] = df_sales_b_master.Amount * df_sales_b_master.price
df_sales_b_master['Brand'] = df_sales_b_master.Product.apply(lambda x: tr_dict_brand[x])
df_sales_b_master['Region'] = np.vectorize(region_cleaner)(df_sales_b_master['Region'])
df_sales_b_master['year_month'] = df_sales_b_master.apply(lambda x: build_year_month(x['Month'], x['Year']), axis = 1)
df_sales_b_master['Type_Cleaned'] = df_sales_b_master['Type'].apply(lambda x: type_cleaner(x))

df_sales_b_master_cleaned = pd.merge(df_sales_b_master, df_translation_table[['Name_file', 'OKID']], left_on = 'SFDAHP', right_on = 'Name_file', how = 'left')
assert len(df_sales_b_master_cleaned) == len(df_sales_b_master)

df_sales_b_master_cleaned['OKID'] = df_sales_b_master_cleaned['OKID'].apply(revert_okid_to_base)
df_sales_b_master_cleaned = df_sales_b_master_cleaned[df_sales_b_master_cleaned['Type'] != 'DB'].copy()

df_sales_grouped = df_sales_b_master_cleaned.groupby(['OKID', 'Product','year_month'])[['Amount', 'Value_RMB']].sum().reset_index()
df_base = df_territory[['region', 'ter_ID', 'cities', 'hospital_name', 'province', 'city', 'OKID', 'hospital_potential']].copy()

year_month_list = generate_year_months('2018-01-01', df_sales_grouped.year_month.max())

df_baseYearMonthsAdded = pd.DataFrame()
for year_month in year_month_list:
    df_base['year_month'] = year_month
    df_baseYearMonthsAdded = pd.concat([df_baseYearMonthsAdded, df_base], ignore_index = True)

df_baseYearMonthsAndProductsAdded = pd.DataFrame()
for product in ['Utro 100mg', 'Utro 200mg', 'Anpo IV', 'Anpo Oral', 'Oest 80g', 'Oekolp']:
    df_baseYearMonthsAdded['Product'] = product
    df_baseYearMonthsAndProductsAdded = pd.concat([df_baseYearMonthsAndProductsAdded, df_baseYearMonthsAdded], ignore_index = True)

df_base = pd.merge(df_baseYearMonthsAndProductsAdded, df_sales_grouped, on = ['OKID', 'Product', 'year_month'], how = 'left').fillna(0)
assert len(df_base) == len(df_baseYearMonthsAndProductsAdded)

df_salesb_cleaned_by_product = df_base.copy()

df_sales_grouped_by_type = df_sales_b_master_cleaned.groupby(['OKID', 'year_month', 'Type_Cleaned'])[['Amount', 'Value_RMB']].sum().reset_index()

df_baseYearMonthsAdded.drop('Product', axis = 1, inplace = True)

df_baseYearMonthsAndTypesAdded = pd.DataFrame()
for type_ in ['PH', 'HP', 'Others']:
    df_baseYearMonthsAdded['Type_Cleaned'] = type_
    df_baseYearMonthsAndTypesAdded = pd.concat([df_baseYearMonthsAndTypesAdded, df_baseYearMonthsAdded], ignore_index = True)

df_base_by_type = pd.merge(df_baseYearMonthsAndTypesAdded, df_sales_grouped_by_type, on = ['OKID', 'Type_Cleaned', 'year_month'], how = 'left').fillna(0)
assert len(df_base_by_type) == len(df_baseYearMonthsAndTypesAdded)

df_salesb_cleaned_by_type = df_base_by_type.copy()

base_maps = pd.read_excel("df_map.xlsx")

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

Besins_Logo = 'https://getlogo.net/wp-content/uploads/2021/07/besins-healthcare-logo-vector.png'
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src = Besins_Logo, height = "40px")),
                        dbc.Col(dbc.NavbarBrand("SFE Business Review Application - OKID Level Analysis", className = "ms-1")),
                    ],
                    align = "center",
                    justify = "center",
                    className = "ms-1"
                ),
                href = "https://plotly.com",
                style = {"textDecoration": "none"},
            ),
        ], fluid = True,
    ),
    color = "#303030",
    dark = True,
)

app.layout = html.Div([
    dbc.Row(navbar),
    
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    [
                        dcc.Markdown(children = "### Region"),
                        dcc.Dropdown(['North', 'East', 'South', 'West'], ['North', 'East', 'South', 'West'], id = 'region_id', multi = True, clearable = True)
                    ]
                ), width = 6
            ),
                
            dbc.Col(
                html.Div(
                    [
                        dcc.Markdown(children = "### Territory"),
                        dcc.Dropdown(id = 'territory_id', multi = True, clearable = True),
                    ]
                ), width = 6
            ),
        ], style = {'padding': 10}
    ),
    
    dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dcc.Markdown("### Historical Sales"), 
                                    dbc.RadioItems(
                                        ['Product', 'Type'],
                                        'Product',
                                        id = 'product_or_type_id',
                                        inline = True,
                                        style = {'padding': 10}
                                    ),
                                    dcc.Graph(id = 'fig_bar_chart_id')
                                ]
                            )
                        ]
                    ), 
                    html.Hr(),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dcc.Markdown("### Table"),
                                    dbc.RadioItems(
                                        ['Potential', 'Difference', 'QTD Sales', 'QTD Achievement'],
                                        'QTD Sales',
                                        id = 'sorting-method',
                                        inline = True,
                                        style = {'padding': 10}
                                    ),
                                    dcc.Graph(id = 'fig_table_id'),
                                    dcc.Store(id = 'data_from_map_to_others'),
                                    dcc.Store(id = 'data_to_plot_bar_chart')
                                ]
                            )
                        ]
                    )
                ], width = 6
            ),
            dbc.Col(dcc.Graph(id = 'fig_map_id', style = {'height': '85vh'}), width = 6)
        ], style = {'padding': 10}
    ),
])

@callback(
    Output('data_from_map_to_others', 'data'),
    Input('fig_map_id', 'clickData'),
    Input('fig_map_id', 'selectedData'),
)
def record_data_on_maps(x, y):
    return ctx.triggered

@callback(
    Output('territory_id', 'options'),
    Input('region_id', 'value'))
def set_territory_options(selected_region):
    return base_maps[base_maps.region.isin(selected_region)].Coverage.unique()

@callback(
    Output('fig_map_id', 'figure'),
    Input('region_id', 'value'),
    Input('territory_id', 'value'),
)
def update_fig_map(region, territory):
    hovering_data_base = {
                        'lat': False,
                        'lon': False,
                        'hospital name':False,
                        'Name':True,
                        'hospital_potential':True,
                        'Avg_sales_last_6M':':,.0f',
                        'Avg_sales_last_Q1':':,.0f',
                        'Avg_sales_last_Q2':':,.0f',
                        'Avg_current_Quarter':':,.0f',
                        'Sales 2023-07-01 RMB':':,.0f',
                        'Sales 2023-08-01 RMB':':,.0f',
                        'Achievement 2023-07-01': ':,.0%',
                        'Achievement 2023-08-01': ':,.0%',
                        'QTD_Sales':':,.0f',
                        'QTD_Target':':,.0f',
                        'QTD_Achievement':':,.0%',
                        '2023-04-01 Valid Calls':True,
                        '2023-05-01 Valid Calls':True, 
                        '2023-06-01 Valid Calls':True, 
                        '2023-07-01 Valid Calls':True, 
                        '2023-08-01 Valid Calls':True,
                        'Total_calls_current_Quarter':True,
                        'Total_meetings_current_Quarter':True,
                        'Investment_amount_Q2': ':,.0f',
                        'Investment_amount_Q3':':,.0f',
}
    color_discrete_map_sfe_okid = {  'No visits quarter, need to visit': "darkred",
                'Sales need to recover': "darkorange",
               'Non-Achievement, investigate': "darkslateblue",
                'Hospital well managed': "mediumseagreen",
                               'Investment is not efficient, change?':'dodgerblue'}
    
    if territory == None or len(territory) == 0:
        base_maps_sub = base_maps[base_maps.region.isin(region)]
    else:
        base_maps_sub = base_maps[(base_maps.Coverage.isin(territory)) & (base_maps.region.isin(region))]
    
    center = {'lat': base_maps_sub['lat'].mean(), 'lon': base_maps_sub['lon'].mean()}
    base_maps_okid_full_sub = px.scatter_mapbox(
                                            base_maps_sub.sort_values(by='first_analysis'), 
                                            lat = 'lat', lon = 'lon', 
                                            size = 'Potential_Utro_Oestro_RMB',
                                            zoom = 2, 
                                            mapbox_style = 'carto-positron',
                                            color = 'first_analysis',
                                            size_max=40, 
                                            center = center,
                                            hover_name = 'hospital name',
                                            hover_data = hovering_data_base, 
                                            labels = {'Name':'MR_Name'}, 
                                            color_discrete_map = color_discrete_map_sfe_okid
    )
    
    base_maps_okid_full_sub.update_layout(
                                            hovermode = 'closest',
                                            mapbox_style = 'basic',
                                            mapbox = dict(accesstoken = mapbox_access_token),
                                            margin = {"r":0,"t":0,"l":0,"b":0},
                                            hoverlabel = dict(font_size = 20,),
                                            legend = dict(
                                                            title = '',
                                                            x = 1,
                                                            xanchor = 'right',
                                                            y = 1,
                                                            bgcolor = "LightSteelBlue",
                                                            bordercolor = "Black",
                                                            borderwidth = 2,
                                                            font=dict(
                                                                        family = "Arial",
                                                                        size = 20,
                                                                        color ="black"
                                                            ),
                                              ),
    )
    
    for data in base_maps_okid_full_sub.data:
        hover_template = data.hovertemplate
        hover_template = hover_template.replace('Potential_Utro_Oestro_RMB=%{marker.size}', 'Potential_Utro_Oestro_RMB=%{marker.size:,.0f}')
        hover_template = hover_template.replace('=', ' = ')    
        data.hovertemplate = hover_template
    return base_maps_okid_full_sub

@callback(
    Output('data_to_plot_bar_chart', 'data'),
    Input('region_id', 'value'),
    Input('territory_id', 'value'),
    Input('data_from_map_to_others', 'data'),
)
def decide_what_to_plot_bar_chart(region, territory, data):
    
    # get a hospital list from the map
    info = data[0]['value']
    if info:
        okid_ls_map = []
        for x in info['points']:
            hospital_name = x['hovertext']
            okid = base_maps[base_maps['hospital name'] == hospital_name]['OKID'].tolist()[0]
            okid_ls_map.append(okid)
    else:
        okid_ls_map = []
    
    # get a hospital list from the filters
    if territory == None or territory == []:
        territory = base_maps[base_maps.region.isin(region)]['Coverage'].unique().tolist()
    okid_ls_filter = base_maps[(base_maps.Coverage.isin(territory)) & (base_maps.region.isin(region))]['OKID'].unique().tolist()
    
    # which one to plot?
    if ctx.triggered[0]['prop_id'] == 'region_id.value' or ctx.triggered[0]['prop_id'] == 'territory_id.value':
        okid_ls_plot = okid_ls_filter.copy()
    else:
        if len(okid_ls_map) == 0:
            okid_ls_plot = okid_ls_filter.copy()
        else:
            okid_ls_plot = okid_ls_map.copy()
    
    return json.dumps(okid_ls_plot)

@callback(
    Output('fig_bar_chart_id', 'figure'),
    Input('data_to_plot_bar_chart', 'data'),
    Input('product_or_type_id', 'value'),
)
def update_bar_chart(data, product_or_type):
    global df_salesb_cleaned_by_product, df_salesb_cleaned_by_type
    
    color_discrete_map = {'PH': 'salmon', 'HP': 'skyblue', 'Others': 'grey'}
    type_order = {'HP': 1, 'PH': 2, 'Others': 3}
    
    okid_ls_plot = json.loads(data)
        
    if product_or_type == 'Type':
        df_salesb_cleaned = df_salesb_cleaned_by_type.copy()
    else:
        df_salesb_cleaned = df_salesb_cleaned_by_product.copy()
    
    df_salesb_cleaned = df_salesb_cleaned[(df_salesb_cleaned.year_month >= '2021-01-01')].copy()
    df_plot = df_salesb_cleaned[df_salesb_cleaned['OKID'].isin(okid_ls_plot)]
    
    if product_or_type == 'Product':
        fig = px.bar(
            df_plot.groupby(['year_month', 'Product'])[['Amount', 'Value_RMB']].sum().reset_index(), 
            x = 'year_month', 
            y = 'Value_RMB', 
            color = 'Product',
            template = 'simple_white',

        )
    else:
        fig = px.bar(
            df_plot.groupby(['year_month', 'Type_Cleaned'])[['Amount', 'Value_RMB']].sum().reset_index().sort_values(by = 'Type_Cleaned', key = lambda col: [type_order[i] for i in col.tolist()]), 
            x = 'year_month', 
            y = 'Value_RMB', 
            color = 'Type_Cleaned',
            template = 'simple_white',
            color_discrete_map = color_discrete_map

        )
    fig.update_xaxes(dtick = "M1", tickformat = "%b\n%Y", tickangle = 0, title = None)
    fig.update_yaxes(title = None)
    fig.update_layout(
        margin = {"r":0,"t":0,"l":0,"b":0},
        paper_bgcolor = 'rgb(0,0,0,0)',
        plot_bgcolor = 'rgb(0,0,0,0)',
        bargap = 0.3,
        legend=dict(
            yanchor = "bottom",
            y = 1,
            xanchor = "left",
            x = 0,
            bgcolor = 'rgba(255, 255, 255, 0)',
            bordercolor = 'rgba(255, 255, 255, 0)',
            orientation = "h",
            title = None,
        ),
    )
    return fig

@callback(
    Output('fig_table_id', 'figure'),
    Input('sorting-method', 'value'),
    Input('data_to_plot_bar_chart', 'data')
)
def update_table(sorting_method, data):
    
    okid_list_used = json.loads(data)
    
    headers = [
                ["<b>Name</b>"], ["<b>Hospital</b>"], ["<b>Diff ¥</b>"], 
               ["<b>QTD ¥</b>"], ["<b>QTD %</b>"], 
               ["<b>Potential ¥</b>"], 
               ["<b>July ¥</b>"], ["<b>July %</b>"],
                ["<b>August ¥</b>"],["<b>August %</b>"],
              ]
    
    option_to_col_name_dict = {'Potential': 'Potential_Utro_Oestro_RMB', 'Difference':'Gap', 'QTD Sales':'QTD_Sales', 'QTD Achievement':'QTD_Achievement'}
    
    if option_to_col_name_dict[sorting_method] == 'Gap':
        base_maps_sub = base_maps[base_maps['OKID'].isin(okid_list_used)].sort_values(option_to_col_name_dict[sorting_method], ascending = True)
    else:
        base_maps_sub = base_maps[base_maps['OKID'].isin(okid_list_used)].sort_values(option_to_col_name_dict[sorting_method], ascending = False)
    values = [
        base_maps_sub['Name'],
        base_maps_sub['hospital name'],
        base_maps_sub['Gap'].round(0),
        base_maps_sub['QTD_Sales'].round(0),
        base_maps_sub['QTD_Achievement'],
        base_maps_sub['Potential_Utro_Oestro_RMB'].round(0),
        base_maps_sub['Sales 2023-07-01 RMB'].round(0),
        base_maps_sub['Achievement 2023-07-01'],
        base_maps_sub['Sales 2023-08-01 RMB'].round(0),
        base_maps_sub['Achievement 2023-08-01'],
        
    ]
    row_color_base = ['white'] * len(base_maps_sub)
    table = go.Figure(
        data = [go.Table(
            columnwidth = [80, 240] + [100] * (len(headers) - 2),
            header = dict(
                values = headers,
                line_color = 'darkslategray',
                fill_color = 'royalblue',
                align = ['center'] * len(headers),
                font = dict(color = 'white', size = 12),
                height = 40,
            ),
          cells = dict(
              values = values,
              line_color = 'darkslategray',
              fill = dict(color = [['paleturquoise'] * len(base_maps_sub)] + [row_color_base] * (len(headers) - 1)),
              align = ['center','left'] + ['center'] * (len(headers) - 2), 
              font_size = 12,
              height = 30,
              format = ["", "", "(,", "(,", ".0%", "(,", "(,", ".0%", "(,", ".0%"]
          )
        )]
    )
    table.update_layout(margin = {"r":0,"t":0,"l":0,"b":0})
    return table

if __name__ == '__main__':
    app.run(debug = True)