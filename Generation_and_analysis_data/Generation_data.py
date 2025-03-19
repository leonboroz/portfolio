import random
import pandas as pd
import numpy as np
from datetime import datetime,timedelta

#Параметры 
n_days=365  #Количество дней для моделирования
avg_customers_per_day=10 #Среднее количество регистраций в день
n_products=500 # Количество товаров

def random_date(start,end):
    return start+timedelta(days=random.randint(0,(end-start).days))

def generator_retention_probabilities(n_days):
    # Формируем нулевую матрицу(одномерную)
    retention_prob=np.zeros(n_days)
    # Задаем вероятности
    retention_prob[:7]=0.5
    retention_prob[7:21]=0.3
    retention_prob[21:42]=0.1
    retention_prob[42:]=0.05
    return retention_prob
# Производим генерацию для таблицы клиентов
def generation_customer(n_days,avg_customer_per_day):
    total_customers=n_days * avg_customer_per_day
    customerids = np.arange(1,total_customers+1)
    signup_dates = np.array([datetime(2020, 1, 1) + timedelta(days=i // avg_customer_per_day) for i in range(total_customers)])
    regions=np.random.choice(['Москва','Тюмень','Новосибирск', 'Санкт-Петербург'],total_customers)
    return pd.DataFrame({'customer_id':customerids,'region':regions,'signup_date':signup_dates})

# Генерируем данные для таблицы product
def generate_products(n_products):
    product_ids=range(1,n_products+1)
    categories=['телефоны','наушники','аксессуары','аккумуляторы']
    prices=np.random.uniform(50,2000,n_products)
    return pd.DataFrame({'product_id':product_ids,
                            'category':np.random.choice(categories,n_products),
                            'price':prices.round(2)})
#Генерируем данные для таблицы orders
def generate_orders(customers, products, n_days):
    retention_prob = generator_retention_probabilities(n_days)
    orders = []
    platforms = ['mobile', 'desktop']

    for day in range(n_days):
        order_date = datetime(2020, 1, 1) + timedelta(days=day)
        cohort_customers = customers[customers['signup_date'] <= order_date].copy()

        # Расчет дней с момента регистрации
        cohort_customers['days_since_signup'] = (order_date - cohort_customers['signup_date']).dt.days

        # Вероятность заказа
        cohort_customers['order_probability'] = retention_prob[np.clip(cohort_customers['days_since_signup'], 0, n_days - 1)]

        # Выбор клиентов, которые сделают заказы
        cohort_customers = cohort_customers[cohort_customers['order_probability'] > np.random.rand(len(cohort_customers))]

        if not cohort_customers.empty:
            for _, row in cohort_customers.iterrows():
                # Генерация количества заказов
                num_orders = np.random.choice([1, 2, 3, 4, 5], p=[0.35, 0.25, 0.2, 0.15, 0.05])

                for _ in range(num_orders):
                    product = products.sample(1).iloc[0]  # Случайный продукт
                    orders.append({
                        'order_id': len(orders) + 1,
                        'customer_id': row['customer_id'],
                        'order_date': order_date.date(),  # Только дата
                        'order_amount': round(np.random.uniform(50, 5000), 2),
                        'platform': random.choice(platforms),
                        'category': product['category']
                    })

    return pd.DataFrame(orders)


#Генерация данных
customers=generation_customer(n_days,avg_customers_per_day)
products=generate_products(n_products)
orders=generate_orders(customers,products,n_days)

#Сохранение данных в csv
customers.to_csv('customers.csv',index=False)
products.to_csv('products.csv',index=False)
orders.to_csv('orders.csv',index=False)

print ("Данные успешно сгенерированы с оптимизациями и сохранены в файлы CSV")
