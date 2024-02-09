import pandas as pd
import numpy as np
import sqlalchemy
from pandasql import sqldf
from connection import db_engine
from config import *
# создаем массив из 10к id пользователей
all_user_ids = np.arange(1, 10000)
# создаем массив из 100 id товаров
all_product_ids = np.arange(1, 100)

n = 10000

# создаем массив из id пользователей так, чтобы
# id могли повторяться
user_ids = np.random.choice(all_user_ids, n)
# создаем массив из id товаров, чтобы товары также повторялись
product_ids = np.random.choice(all_product_ids, n)

# создаем стартовую дату
start_date = pd.to_datetime('2022-01-01')
# создаём массив из 10к дат, начиная со стартовой, с
# временным интервалом в одну минуту
times = pd.date_range(start_date, periods=n, freq='1min')

# создаём датасет с ранее подготовленными данными
user_actions = pd.DataFrame({'user_id': user_ids,
                             'product_id': product_ids,
                             'time': times})
# добавляем столбец actions и заполняем его 
# одинаковыми значениями
user_actions['action'] = 'view'

# функция для создания дополнительных действий по 
# добавлению в корзину и оплате товара
def generate_funel_actions(user_id, product_id, time):
    # устанавливаем вероятность выпадения действия 
    to_cart = 0.2
    to_purchase = 0.4

    df = pd.DataFrame()

    # функция, которая подбрасывает монетку и получает результат
    # с заданной нами вероятностью
    if np.random.binomial(1, to_cart, 1)[0]:
        df = pd.DataFrame({
            'user_id': user_id,
            'product_id': product_id,
            'time': time + pd.Timedelta(5, unit='s'),
            'action': 'add to cart'}, index=[0])
        
        if np.random.binomial(1, to_purchase, 1)[0]:
            df_purchase = pd.DataFrame({
                'user_id': user_id,
                'product_id': product_id,
                'time': time + pd.Timedelta(10, unit='s'),
                'action': 'purchase'}, index=[0])

            df = pd.concat([df, df_purchase])
    return df

to_cart_df = pd.DataFrame()

# перебор строк датафрейма, iterrows возвращает итератор
# с index'ом и строками в виде серии
for index, row in user_actions.iterrows():
    user_df = generate_funel_actions(row['user_id'], row['product_id'], row['time'])
    to_cart_df = pd.concat([to_cart_df, user_df])

user_actions = pd.concat([user_actions, to_cart_df])
# сортировка данных по столбцу времени
user_actions = user_actions.sort_values('time')

# добавляем отдельный столбец для дня
user_actions['date'] = user_actions.time.dt.date

# Количество просмотров товаров, добавлений их в корзину,оплат
# и процент оплат по датам
q = """select date,
              views,
              carts,
              purchases,
              100 * purchases / views as purchase_percentage
        from (
            select date,
                count(case when action = 'view'         then 1 else NULL end) as views,
                count(case when action = 'add to cart'  then 1 else NULL end) as carts,
                count(case when action = 'purchase'     then 1 else NULL end) as purchases
             from user_actions
             group by date);"""

q = (sqldf(q, globals()))

user_actions.to_sql(table_name,
        db_engine,
        schema='public',
        index=False,
        if_exists='replace',  
        dtype={'user_id': sqlalchemy.types.INTEGER,
               'product_id': sqlalchemy.types.INTEGER}
        )
