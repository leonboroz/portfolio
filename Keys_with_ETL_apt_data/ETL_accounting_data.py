import pandas as pd
import datetime
import numpy as np
import glob
import re
import os
import openpyxl
result_path=f"Результат/{datetime.datetime.today().strftime('%Y-%m-%d')}/"
if  not os.path.exists(result_path):
    os.makedirs(result_path)


path_sbis='Входящие'
path_apteks='Аптеки\csv\correct'
a=["Дата",
"Номер",
"Сумма",
"Статус",
"Примечание",
"Комментарий",
"Контрагент",
"ИНН/КПП",
"Организация",
"ИНН/КПП",
"Тип документа",
"Имя файла",
"Дата",
"Номер 1",
"Сумма 1",
"Сумма НДС",
"Ответственный",
"Подразделение",
"Код",
"Дата",
"Время",
"Тип пакета",
"Идентификатор пакета",
"Запущено в обработку",
"Получено контрагентом",
"Завершено",
"Увеличение суммы",
"НДC",
"Уменьшение суммы",
"НДС"]
columns_names=[j.replace(' ','_') for j in  [i.strip(' ') for i in a]]
lst=[i.strip('"') for i in ["СчФктр", "УпдДоп", "УпдСчфДоп", "ЭДОНакл"]]
apt_columns_names=['№ п/п', 'Штрих-код партии', 'Наименование товара', 'Поставщик',
    'Дата приходного документа', 'Номер приходного документа',
    'Дата накладной', 'Номер накладной', 'Номер счет-фактуры',
    'Сумма счет-фактуры', 'Кол-во',
    'Сумма в закупочных ценах без НДС', 'Ставка НДС поставщика',
    'Сумма НДС', 'Сумма в закупочных ценах с НДС', 'Дата счет-фактуры', 'Сравнение дат']

def load (path_sbis,column_names):
    cnt=1
    dict_df={}
    for i in glob.glob(path_sbis+'/*.csv'):
        df=pd.read_csv (i,sep =";" , encoding='1251',skiprows=1,header=None)
        dict_df[f"df_{cnt}"]=df
        cnt+=1
    df=pd.concat(list(dict_df.values()),ignore_index=True)
    df.columns=column_names
    df1=df.copy()
    df1=df1[df1['Тип_документа'].isin(lst)]
    df1=df1.iloc[:,0:3]
    df1=df1[['Номер','Сумма','Дата']]
    df1['Дата']=pd.to_datetime(df1['Дата'],format='%d.%m.%y').dt.strftime('%d.%m.%Y')
    return df1

sbic=load(path_sbis,columns_names)


for i in glob.glob(path_apteks+'/*.csv'):
    name_file=i[-8:-4]
    df=pd.read_csv(i,sep=';', encoding='1251')    
    df['Номер накладной']=np.where(df['Поставщик'].str.contains('ЕАПТЕКА'),df['Номер накладной']+'/15',df['Номер накладной'])
    df=df.merge(sbic,how='left',left_on='Номер накладной',right_on='Номер')
    df['Сравнение дат']=np.where(df['Дата накладной']==df['Дата'],'','Не совпадает!')
    df.columns=apt_columns_names
    df.to_excel(f"{result_path}{name_file}.xlsx",index=False)
               




