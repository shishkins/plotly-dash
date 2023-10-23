# Импортирование библиотек
from dash import Dash, html, dash_table, dcc, callback, Output, Input
from query_executer import csv_execute
from get_dataframes import get_data
from datetime import date, datetime
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
import plotly.io as poi

''' GET DATA '''
class data_lake(object):
    '''
    Класс объекта модели данных, принцип работы:
    __init__ инициализирует модель данных через словарь позиционных аргументов **kwargs
    Затем пользователем задаются аттрибуты фильтров
    После этого метод " " выдает отфильтрованный датафрейм на работу
    '''

    def __init__(self, **kwargs):
        '''
        Инициализация объекта с данными
        :param kwargs:
        '''
        self.__dict__.update(kwargs)
        self.main_df = reprices_log_df.merge(calendar_df, left_on='date_reprice', right_on='date', how='left')
        self.main_df = self.main_df.merge(products_reference_df, on='product_id', how='left')
        self.main_df = self.main_df.merge(pricing_types_df, on='algorithm', how='left')
        self.main_df = self.main_df.merge(write_date_df, how='cross')
        self.errors_df = reprices_errors_log_df
        self.picked_data = None
        self.pricked_algorithms = None

    def filter_date_df(self, start_date=None, end_date=None):
        '''
        Метод, который обновляет выбранную пользователем дату
        :param start_date:
        :param end_date:
        :return:
        '''
        limits = pd.DataFrame({'start_date': [start_date],
                               'end_date': [end_date]})
        limits['start_date'] = pd.to_datetime(limits['start_date'], format='ISO8601')
        limits['end_date'] = pd.to_datetime(limits['end_date'], format='ISO8601')
        self.picked_data = limits

    def filter_algorithms(self, algorithms=None):
        '''
        Метод, который обновляет список алгоритмов, выбранных пользователем
        :param algorithms:
        :return:
        '''
        algorithms_df = pd.DataFrame(
            {'algorithm': algorithms}
        )
        self.picked_algorithms = algorithms_df
    def filtered_df(self):
        severed_df = self.main_df.loc[(self.main_df['date_reprice'] >= self.picked_data['start_date'].iloc[0]) &
                                      (self.main_df['date_reprice'] <= self.picked_data['end_date'].iloc[0])]

pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df, write_date_df = get_data()
dict_of_dfs = {'pricing_types_df': pricing_types_df,
               'product_reference_df': products_reference_df,
               'reprices_errors_log_df': reprices_errors_log_df,
               'calendar_df': calendar_df}

main_df = data_lake(pricing_types_df=pricing_types_df,
                    products_reference_df=products_reference_df,
                    reprices_errors_log_df=reprices_errors_log_df,
                    reprices_log_df=reprices_log_df,
                    calendar_df=calendar_df)



''' LAYOUT '''
app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP])

calendar_button = dcc.DatePickerRange(id='date-picker',
                                      min_date_allowed=min(calendar_df['date']),
                                      max_date_allowed=max(calendar_df['date']),
                                      initial_visible_month=reprices_log_df['date_reprice'].mean(),
                                      start_date=min(calendar_df['date']),
                                      end_date=max(calendar_df['date'])
                                      )

algorithm_filter = dcc.Checklist(
    id='check-list-algorithms',
    options=[{'label': option, 'value': option} for option in pricing_types_df['type_name']],
    value=[]
)

reprices_log_fig = dcc.Graph(id='hist-prices-log',
                             figure=go.Figure())
app.layout = html.Div([
    dbc.Row(html.H1('Hello Dash!'),
            style={'margin-bottom': 40}),
    dbc.Row([
        dbc.Col([
            html.Div(id='date-picker-info'),
            html.Div(calendar_button)
        ]),
        dbc.Col([
            html.Div('Выберите тип переоценки'),
            html.Div(algorithm_filter)
        ])
    ], style={'margin-bottom': 40}),
    dbc.Row([
        dbc.Col([
            html.Div('Количество переоценок по датам:',
                     id='test'),
            reprices_log_fig
        ])
    ])
],
    style={'margin-left': '80px',
           'margin-right': '80px'})

''' CALLBACKS '''


@callback(
    Output('hist-prices-log', 'figure'),
    Input('date-picker', 'start_date'),
    Input('date-picker', 'end_date')
)
def update_output(start_date, end_date):

    prices_log_fig = px.histogram(rounded_df, x='date_reprice')
    return prices_log_fig


@callback(
    Output('test', 'children'),
    Input('check-list-algorithms', 'value')
)
def test_update(algorithms):
    print(algorithms)
    return None


if __name__ == '__main__':
    app.run_server(debug=True)