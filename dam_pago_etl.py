#!/usr/bin/env python
# coding: utf-8

# ## processamento arrecadação "base quente"

# #### etapas:
#     
#     1. carregar a base diária;
#     2. complementar consolidado do ano com a nova base;
#     3. salvar novo consolidado
#     4. complementar com os dos outros anos;
#     5. gerar calendário, com feriados inclusos;
#     6. realizar transformações na receita local.

# In[1]:


from sys import platform


# In[2]:


import pandas as pd


# In[3]:


# display de valores
pd.options.display.float_format = '{:,.2f}'.format


# In[34]:


# arquivos
base_dia = "base_dia/Receitas.csv"
consolidado = "consolidado_ultimo_ano/base_consolidada.csv"
ultimo_ano = "base_2014_2020/base_2014_2020.csv"
feriados = "feriados/dados_calendario comex.xlsx"
receita_local = "receita_local/receita_local.xlsx"
output = "output_processamento/dam_pago.csv"


# ### 1. base diária

# In[5]:


df_base_diaria = pd.read_csv(base_dia, sep=";", encoding="latin-1", decimal=".")

# index de data
cols_datas = ["DIA", "MES", "ANO"]
foo = df_base_diaria[cols_datas]
foo.columns = ["day", "month", "year"]

df_base_diaria.index = pd.to_datetime(foo[["year", "month", "day"]])
df_base_diaria.index.name = "data"

df_base_diaria.drop(cols_datas, axis=1, inplace=True)
df_base_diaria.drop("RECEITA", axis=1, inplace=True)
df_base_diaria.columns = ["cd_receita_local", "receita"]
print("\n")
df_base_diaria.info()


# #### data inicial da base diária

# In[6]:


dt_min = df_base_diaria.index.min() - pd.Timedelta(days=1)


# ### 2. base consolidada do ano

# In[7]:


# antigo
# df_cons = pd.read_csv(consolidado, sep=";", decimal=",", thousands=".")
# df_cons.index = pd.to_datetime(df_cons["data"], format="%Y%m%d")

# novo
df_cons = pd.read_csv(consolidado, sep=";")
df_cons.index = pd.to_datetime(df_cons["data"], format="%Y-%m-%d")

df_cons.drop("data", axis=1, inplace=True)
df_cons.sort_index(inplace=True)
# df_cons.info()
print("\n")
df_cons.head()


# In[8]:


df_ano = pd.concat([df_cons[:dt_min.strftime("%Y-%m-%d")], df_base_diaria])


# #### 3. salva o novo consolidado

# In[9]:


df_ano.to_csv(consolidado, sep=";")


# #### 4. anos anteriores

# In[10]:


df_anos_anteriores = pd.read_csv(ultimo_ano, 
                                 usecols=["data", "cd_receita_local", "receita"],
                                 sep=";", thousands=".", decimal=",")
df_anos_anteriores.index = pd.to_datetime(df_anos_anteriores["data"], format="%Y%m%d")
df_anos_anteriores.drop("data", axis=1, inplace=True)


# In[11]:


df_anos_anteriores.head()


# In[12]:


df_anos_anteriores.receita.dtype


# In[13]:


df_todos_anos = pd.concat([df_anos_anteriores, df_ano], axis=0)


# #### 5. calendário

# In[14]:


df_feriados = pd.read_excel(feriados, usecols=["DATA", "FERIADO", "NOME_FERIADO", "DIA_UTIL"])
df_feriados.columns = [col.lower() for col in df_feriados.columns]
df_feriados.index = pd.to_datetime(df_feriados["data"], format="%Y%m%d")
df_feriados.index.name = "data"
df_feriados.drop("data", axis=1, inplace=True)
df_feriados.tail()


# In[15]:


df_arrec_com_feriados = df_todos_anos.join(df_feriados, on="data")
df_arrec_com_feriados.head()


# In[16]:


df_arrec_com_feriados['mes_dia_num'] = [int(n) for n in df_arrec_com_feriados.index.strftime("%m%d")]


# In[17]:


df_arrec_com_feriados.head()


# #### 6. receita local

# In[35]:


df_rec_local = pd.read_excel(receita_local, index_col="cd_receita_local")


# In[36]:


df_calend_rec_local = df_arrec_com_feriados.join(df_rec_local, on="cd_receita_local")


# In[37]:


receita_grupo = list()
for i, col in df_calend_rec_local.iterrows():
    try:
        if col['atribuicao'] == 'SEC. FINANÇAS' or col['subalinea_resumida2'] in ['DÍVIDA ATIVA', 'OUTRAS RECEITAS']:
                receita_grupo.append(col['subalinea_resumida2'])
        else:
            receita_grupo.append(col['subalinea_resumida2'].split('-')[1])
    except AttributeError:
        print(col['cd_receita_local'], i)


# In[38]:


df = df_calend_rec_local.assign(receita_grupo2=receita_grupo)
df.head()


# In[39]:


df = df.\
    assign(ano=df.index.year).\
    assign(mes_num=df.index.month).\
    assign(dia=df.index.day).\
    reset_index()

df.head(3)


# In[40]:


meses = {1: "jan",  2: "fev",  3: "mar",  4: "abr",  5: "mai",  6: "jun",  7: "jul",  8: "ago",  9: "set", 10: "out", 11: "nov", 12: "dez"}


# In[41]:


df["mes"] = df["mes_num"].map(meses)


# In[42]:


df["cd_nm_receita"] = df.cd_receita_local.astype(str) + "-" + df["receita_nome"]


# In[43]:


if platform == "linux":
    df.to_csv("~/base_quente_etl/output_processamento/dam_pago.csv", sep=";", index=False)
else:
    df.to_csv(output, sep=";", index=False)


# In[44]:


print(df.data.max())


# In[45]:


print(df.groupby("ano")["receita"].sum())