from get_dataframes import get_data
from datetime import date, datetime
import pandas as pd

pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df, write_date_df = get_data()
dict_of_dfs = {'pricing_types_df': pricing_types_df,
               'product_reference_df': products_reference_df,
               'reprices_errors_log_df': reprices_errors_log_df,
               'calendar_df': calendar_df}


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
    def filter_algorithms(self, algorithms = None):
        '''
        Метод, который обновляет список алгоритмов, выбранных пользователем
        :param algorithms:
        :return:
        '''
        algorithms_df = pd.DataFrame(
            {'algorithm':algorithms}
        )
        self.picked_algorithms = algorithms_df



first_df = data_lake(pricing_types_df=pricing_types_df,
                     products_reference_df=products_reference_df,
                     reprices_errors_log_df=reprices_errors_log_df,
                     reprices_log_df=reprices_log_df,
                     calendar_df=calendar_df)


start_date = '2023-09-15'
end_date = '2023-09-18'

first_df.filter_date_df(start_date, end_date)
first_df.filter_algorithms(algorithms=['Малоценка', 'Агрегаты', 'Курсовой модуль', 'Вынужденная переоценка'])

print(first_df.picked_data)
print(first_df.picked_algorithms)

