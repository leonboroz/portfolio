# %%
import pandas as pd
import numpy as np
import openpyxl
import matplotlib.pyplot as plt
import seaborn as sns

# %%
products=pd.read_excel('products.xlsx')

# %%
products.info()

# %%
#Переименую колонки для удобства
products.columns=['product_id', 'category', 'subcategory', 'name']

# %%
products[products['name'].isna()]
# Видим что в колонке name много пустых значений

# %%
orders=pd.read_excel('orders.xlsx')

# %%
orders.info()

# %%
# Объединяем таблицы. Я использую inner join
table=pd.merge(products,orders,on='product_id')

# %% [markdown]
# Проверяем на наличие пустых и заполнияем значениями похожих по цене, если нет совпадения называем продукт Unknown

# %%
table.query("name.isna()")
table.loc[table['subcategory']=='Твердые сычужные сыры','name']=table.loc[table['subcategory']=='Твердые сычужные сыры','name'].fillna('Unknown')
table.loc[table['subcategory']=='Майонез','name']=table.loc[table['subcategory']=='Майонез','name'].fillna('Unknown')
table.loc[table['subcategory']=='Не молочное детское питание','name']=table.loc[table['subcategory']=='Не молочное детское питание','name'].fillna('Unknown')
table.loc[table['subcategory']=='Шоколад','name']=table.loc[table['subcategory']=='Шоколад','name'].fillna('Шоколад Ritter Spor')
table.query("name.isna()")

# %%
popul_category=table.groupby('category').agg(count_unit=('quantity','sum')).reset_index().sort_values('count_unit',ascending=False)

# %%
fig,ax=plt.subplots(figsize=(8,6))
sns.barplot(data=popul_category,y='category',x='count_unit',ax=ax,hue='count_unit',palette='YlGn_d' ,legend=False)
plt.title('Категории товаров по количеству проданных штук')
ax.set_ylabel(None)
ax.set_xlabel(None);

# %%
popul_category_sub=table.groupby(['category','subcategory']).agg(count_unit=('quantity','sum')).reset_index()
popul_category_sub['all_sum']=popul_category_sub.groupby('category')['count_unit'].transform(lambda x:x.sum())


# %%
# В моих планах построить распределение значений в рамках категории, но таким образом, чтобы сопоставить с графиком популярности категорий
sorted_df =popul_category_sub.sort_values(by='all_sum', ascending=False )
sorted_df

# %%
fig,ax1=plt.subplots(figsize=(14,6))
sns.boxplot(data=sorted_df,y='category',x='count_unit',hue='category',palette='binary')
plt.grid(alpha=0.4)
plt.title('Распределение количества проданных позиций по подкатегориям')
plt.ylabel(None)
plt.xlabel(None);


# %% [markdown]
# Произведем расчет среднего чека за 13.01.2022

# %%
mean_order_13_01_2022=float(table[table['accepted_at'].dt.normalize()==pd.Timestamp('2022-01-13')].groupby('order_id')[['quantity','price']]
      .apply(lambda x:sum(x['quantity']*x['price'])).reset_index()[0].mean().round(2))


text=f"Средний чек за 13.01.2022г. = {mean_order_13_01_2022} рублей"

# %%
fig,ax=plt.subplots()
fig.set_facecolor('lightgray')
ax.set_xticks([])
ax.set_yticks([])
ax.set_frame_on(False)
ax.text(0.1,0.5,text,fontsize=12)
plt.title('Средний чек',color='green',alpha=0.5);

# %%
table_cheez=table.query("category=='Сыры'")

# %%
table_cheez.loc[:,'is_promo']=np.where((table_cheez['regular_price']!=table_cheez['price']),'promo','stand')
promo_table=table_cheez.groupby('is_promo').agg(cnt=('quantity','sum')).reset_index()

# %%
fig,ax1=plt.subplots(figsize=(10,6))
color=sns.color_palette('bright')
fig.set_facecolor('lightgray')
ax1.pie(data=promo_table,x='cnt',labels=['Promo','Sample'], colors=color,autopct='%1.1f%%',explode=[0.1,0.1],labeldistance=0.7
        ,pctdistance=0.45,textprops={'color':'white','fontweight': 'bold'})
plt.title('Соотношение товаров по промоакциям в категории "Сыры"',color='green',alpha=0.5);

# %%
table.head(2)

# %%
table_margin=(
table.groupby('category')
.apply (lambda d :
pd.Series({'margin_in_rub':((d['price']-d['cost_price'])*d['quantity']).sum(),                                                          
          'margin_in_perc':(((d['price']-d['cost_price'])*d['quantity']).sum()/(d['price']*d['quantity']).sum()*100).round(2)
          
          
          }),include_groups=False).sort_values(by='margin_in_rub',ascending=False).reset_index()                                   

)

# %%
table_margin

# %%
fig,ax=plt.subplots()
sns.barplot(data=table_margin,y='category', x='margin_in_rub',ax=ax,hue='margin_in_rub',palette='pastel',legend=False)
ax.set_frame_on(False)
plt.title('Распределение маржинальности в рублях')
plt.ylabel(None);

# %%
fig,ax=plt.subplots()
sns.barplot(data=table_margin.sort_values(by='margin_in_perc',ascending=False),y='category'
            , x='margin_in_perc',ax=ax,legend=False)
ax.set_frame_on(False)
plt.title('Распределение маржинальности в процентах')
plt.ylabel(None);

# %%
table

# %%
table_ABC=(
(table.groupby('name').apply(lambda x : pd.Series({'all_cnt':x['quantity'].sum()
                                                  ,'all_sale': (x['price']*x['quantity']).sum()
                                                  }),include_groups=False)).reset_index()

)

# %%
table_ABC.head(2)

# %%
table_ABC[['per_cnt','per_sale']]=table_ABC[['all_cnt','all_sale']]/table_ABC[['all_cnt','all_sale']].sum()

# %%
table_ABC['cumsum_cnt']=table_ABC.sort_values(by='per_cnt',ascending=False)['per_cnt'].cumsum()
table_ABC['cumsum_sale']=table_ABC.sort_values(by='per_sale',ascending=False)['per_sale'].cumsum()


# %%
#о Приготовим переменные для категоризации товаров и создадим колонки с категориями
cond=[table_ABC[['cumsum_cnt','cumsum_sale']]<=0.8,table_ABC[['cumsum_cnt','cumsum_sale']]<=0.95]
ret=['A','B']
table_ABC[['gr_cnt','gr_sale']]=np.select(cond,ret,default='C')

# %%
# Создадим объединенную колонку с помощью join. Будет возможность варировать разделителем
table_ABC['fin_group']= table_ABC[['gr_cnt','gr_sale']].agg(' - '.join,axis=1)

# %%
# Посмотрим получившуюся таблицу
table_ABC.sort_values(by='per_cnt',ascending=False,ignore_index=True).head(10)

# %% [markdown]
# В довершению проекта соберем мини-дашборд с помощью mosaic

# %%
mosaik=[['A','B'],['C','D'],[None,None],['E','F']]

# %%
print(plt.style.available)

# %%

fig,ax=plt.subplot_mosaic(mosaic=mosaik,figsize=(12,10),layout="constrained",height_ratios=[2,2,0.1,1],width_ratios=[1,1.5])
fig.suptitle('Итоговый проект по визуализации',fontsize=18)

ax[None].set_frame_on(False)
ax[None].set_xticks([])
ax[None].set_yticks([])

 

sns.barplot(data=popul_category,y='category',x='count_unit',ax=ax['A'],hue='count_unit',palette='YlGn_d' ,legend=False)
ax['A'].set_title('Количество проданных штук',fontsize=12)
ax['A'].set_ylabel(None)
ax['A'].set_xlabel(None)

sns.boxplot(data=sorted_df,y='category',x='count_unit',hue='category',palette='binary',ax=ax['B'])

ax['B'].set_title('Распределение количества в категориях',fontsize=12)
ax['B'].set_ylabel(None)
ax['B'].set_xlabel(None)
ax['B'].set_yticks([])
ax['B'].grid(alpha=0.4)


ax['F'].set_facecolor('lightgray')
ax['F'].set_xticks([])
ax['F'].set_yticks([])
ax['F'].set_frame_on(False)
ax['F'].text(0.1,0.6,text,fontsize=12)

color=sns.color_palette('bright')
ax['E'].set_facecolor('lightgray')
ax['E'].pie(data=promo_table,x='cnt',labels=['Promo','Sample'], colors=color,autopct='%1.1f%%',explode=[0.1,0.1]
        ,pctdistance=0.45,textprops={'color':'black'})
ax['E'].set_title('Категория "Сыры" : промакция',fontsize=12)

sns.barplot(data=table_margin,y='category', x='margin_in_rub',ax=ax['C'],hue='margin_in_rub',palette='pastel',legend=False)
ax['C'].set_frame_on(False)
ax['C'].set_title('Маржа (рубль)',fontsize=12)
ax['C'].set_ylabel(None)
ax['C'].set_xlabel(None)

sns.barplot(data=table_margin.sort_values(by='margin_in_rub',ascending=False),y='category'
            , x='margin_in_perc',ax=ax['D'],legend=False)
ax['D'].set_frame_on(False)
ax['D'].set_title('Маржинальность (%)',fontsize=12)
ax['D'].set_ylabel(None)
ax['D'].set_xlabel(None)
ax['D'].set_yticks([])
ax['D'].grid(alpha=0.4,color='red',linestyle='--')

plt.savefig('my_plot.png')



