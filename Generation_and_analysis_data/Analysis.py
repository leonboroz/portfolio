# %%
import random
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import math
import matplotlib.pyplot as plt
import seaborn as sns

# %%
orders=pd.read_csv('orders.csv')

# %%
# Оставим исходный файл в нетронутом состоянии
orders1=orders.copy()

# %%
orders1['order_date']=pd.to_datetime(orders['order_date'])
# Для удобства восприятия датафрейма вставим дату первой транзакции в удобное место
orders1.insert(2,'signup', orders1.groupby('customer_id')['order_date'].transform('min').dt.to_period('M'))


# %%
# Вычмслим размеры когорт
size_cohort=orders1.groupby('signup',as_index=False).agg(cnt_customer_in_cohort=('customer_id','nunique'))


# %%
# Создадим промежуточную таблицу где сохраними те транзакции, которые произошли в течение 10 дней с момента первой покупки
prom_table=(orders1.query("(order_date-signup.dt.to_timestamp()).dt.days<10")
 .groupby(['signup','platform','category','customer_id'],as_index=False)
 .agg(amount_per_customer=('order_amount','sum'))
)


# %%
# Трансформируем в агрегатную таблицу с выходными данными по количеству уникальных клиентов в юните и средней величиной трат
raschet_unit=(prom_table.groupby(['signup','platform','category'],as_index=False)
 .agg(cnt_customer_per_unit=('customer_id','nunique'),avg_profit=('amount_per_customer',lambda x: round(x.mean())))
 )


# %%
itog_table=pd.merge(size_cohort,raschet_unit,how='inner',left_on='signup',right_on='signup')


# %%
itog_table['signup']=itog_table['signup'].astype(str)
itog_table.head(2)['cnt_customer_per_unit'].sum()


# %%
# Соберем pivot таблицу для визуального табличного представления, о распределении среднего профита
pivot=itog_table.pivot_table(index='signup',columns=['platform','category'],values='avg_profit')


# %%
# Соберем таблицу для визуального размера когорт и среднего профита в контексте  10-ти дневного среза транзакций
visual=itog_table.groupby(['signup','cnt_customer_in_cohort','platform'])['avg_profit'].mean().reset_index()

# %%
fg,ax=plt.subplots(figsize=(12,6))
ax1=ax.twinx()
sns.barplot(ax=ax1,data=visual,x='signup',y='avg_profit',hue='platform',alpha=0.5)
ax1.legend(loc='best', bbox_to_anchor=(1, 1), fontsize=12)
sns.lineplot(ax=ax, data=size_cohort, x=np.arange(0,12,1).tolist(), y='cnt_customer_in_cohort',lw=2,color='green')
plt.tight_layout()
sns.scatterplot(ax=ax, data=size_cohort, x=np.arange(0,12,1).tolist(), y='cnt_customer_in_cohort')
ax.set_yticks([220,size_cohort['cnt_customer_in_cohort'].min(),size_cohort['cnt_customer_in_cohort'].max(),350])
ax.tick_params(axis='x', rotation=60)  
plt.title('Распределение размера когорт и среднего профита по платформам');


# %%
raschet_unit1=(prom_table.groupby(['signup','platform','category'],as_index=False)
 .agg(profit=('amount_per_customer',lambda x: round(x.sum())))
 
)
itog_table1=pd.merge(size_cohort,raschet_unit1,how='inner',left_on='signup',right_on='signup')
itog_table1['signup']=itog_table1['signup'].astype(str)


# %%
mosaik=[['A','C','B']]

# %%
fig1,ax=plt.subplot_mosaic(mosaic=mosaik,figsize=(12,6),layout="constrained",width_ratios=[1,0.1,1])
fig1.suptitle('Динамика  профита по категориям в рамках платформ',fontsize=18)

sns.barplot(ax=ax['A'],data=itog_table1.query("platform == 'desktop' and category in ('телефоны','наушники')"),x='signup',y='profit',hue='category',legend=False)
ax['A'].tick_params(rotation=80)
ax['A'].set_xlabel(None)
ax['A'].set_title('desktop')

sns.barplot(ax=ax['B'],data=itog_table1.query("platform == 'mobile' and category in ('телефоны','наушники')"),x='signup',y='profit',hue='category')
ax['B'].tick_params(rotation=80)
ax['B'].legend(loc='best', bbox_to_anchor=(1, 1), fontsize=12)
ax['B'].set_ylabel(None)
ax['B'].set_yticks([])
ax['B'].set_xlabel(None)
ax['B'].set_title('mobile')


ax['C'].set_frame_on(False)
ax['C'].set_yticks([])
ax['C'].set_xticks([])
;


