import sys
# sys.path.insert(1, "./utils")
# import mail
sys.path.insert(1, "./monitoramento")
import extracao

import pandas as pd
from pandas.tseries.offsets import DateOffset
import plotly.figure_factory as ff

if sys.platform == "linux":
    ARQUIVO = "~/findata/app/dashboards/_datasets/base_quente/dam_pago.csv"
    # IMG_PATH = "~/etl_sefin/base_quente/images"
else:
    ARQUIVO = "C:/Users/User/Documents/sefin/dam_pago/base_quente/dataset" \
              "/dam_pago.csv"
    # IMG_PATH = "C:/Users/User/Documents/mini_projetos/etl_sefin/base_quente" \
    #            "/images"

COLORSCALE = [[0, '#044fa1'], [.5, '#f2f2f2'], [1, '#ffffff']]


def base(com_rajada=False):
    df_ext = pd.read_csv(ARQUIVO, sep=";", decimal=".", thousands=",")
    df_ext['data'] = pd.to_datetime(df_ext['data'], format="%Y-%m-%d")
    df_ext.sort_values('data', inplace=True)

    # df sem rajada que será usada no dashboard
    if com_rajada:
        return df_ext
    return df_ext[df_ext.data < df_ext.data.max()]


def rajada():
    df = base(com_rajada=True)
    return df[df.data == df.data.max()]


def df_sete_dias_uteis(df, timestamp):
    quinze_dias = pd.date_range(end=timestamp, periods=15, freq="D")
    sete_dias_uteis = df[df.data.isin(quinze_dias)
                        & df.dia_util == 1]['data'].unique()[-7:]
    return df[df.data.isin(sete_dias_uteis)]


df_raw = base()

data_referencia = df_raw.data.max()
data_referencia_string = data_referencia.strftime("%d/%m/%Y")
data_anterior = data_referencia + DateOffset(years=-1)
data_mes_anterior = data_referencia + DateOffset(months=-1)

ano_ref = data_referencia.year
ano_anterior = ano_ref - 1

mes_max = data_referencia.month

tributos = ['ISS', 'IPTU', 'ITBI']

query_principal = "ano == {} and " \
                  "receita_grupo2 == @tributos and " \
                  "data <= '{}'"

df_tributos = pd.concat([
    df_raw.query(query_principal.format(ano_ref, data_referencia)),
    df_raw.query(query_principal.format(ano_anterior, data_anterior)),
])

df_ano_grp = df_tributos.groupby(["ano", "receita_grupo2"]).receita.sum()
df_ano_grp.name = "Ano"
df_mes_grp = df_tributos.query("mes_num == @mes_max"). \
    groupby(['ano', 'receita_grupo2']).receita.sum()
df_mes_grp.name = "Mês"

df_7dias_ano_ref = df_sete_dias_uteis(df_tributos, data_referencia)
df_7dias_ano_ant = df_sete_dias_uteis(df_tributos, data_anterior)
df_7dias_grp = pd.concat([df_7dias_ano_ref, df_7dias_ano_ant]). \
    groupby(["ano", "receita_grupo2"]).receita.sum()
df_7dias_grp.name = "7 Dias"

df_arrec = pd.concat([df_ano_grp, df_mes_grp, df_7dias_grp], axis=1)

df_var_perc = (df_arrec.loc[ano_ref] / df_arrec.loc[ano_anterior]) - 1

"""
Mês de referência contra o mês anterior
"""

inicio_data_ref = data_referencia.replace(day=1)
inicio_data_mes_ant = data_mes_anterior.replace(day=1)

df_mes_ref = df_raw.query("receita_grupo2 == @tributos and data >= "
                          "@inicio_data_ref and data <= "
                          "@data_referencia").groupby(
    ['ano', 'receita_grupo2']).receita.sum()

df_mes_anterior = df_raw.query("receita_grupo2 == @tributos and data >= "
                               "@inicio_data_mes_ant and data <= "
                               "@data_mes_anterior").groupby(
    ['ano', 'receita_grupo2']).receita.sum()

df_var_perc_mes = (df_mes_ref / df_mes_anterior) - 1
df_var_perc_mes.name = "Mês Anterior"

df_variacoes = pd.concat([df_var_perc, df_var_perc_mes.loc[2022]], axis=1)

"""
Finalizando tabela
"""

df_arrec.columns = [f"{c} (R$)" for c in df_arrec.columns]
df_variacoes.columns = [f"{c} (Var. %)" for c in df_variacoes.columns]
df_tabela = pd.concat([
    df_arrec.loc[2022].T.applymap('{:,.2f}'.format),
    df_variacoes.T.applymap('{:.1%}'.format)
])
df_tabela.reset_index(inplace=True)
df_tabela.rename(columns={"index": data_referencia_string}, inplace=True)

# fig = ff.create_table(df_tabela, height_constant=20, colorscale=COLORSCALE)
# fig.layout.width = 850

# fig.write_image(f"{IMG_PATH}/tabela.png")

"""
Rajada
"""

df_rajada = rajada()
dt_rajada_string = df_rajada.data.max().strftime("%d/%m/%Y")
dfr_grp = df_rajada.groupby("receita_grupo2", as_index=False)["receita"].sum()
dfr_grp.columns = [dt_rajada_string, "Rajada"]
dfr_grp["Rajada"] = dfr_grp["Rajada"].map('{:,.2f}'.format)

# fig_rajada = ff.create_table(dfr_grp, colorscale=COLORSCALE)

# fig_rajada.write_image(f"{IMG_PATH}/tabela_rajada.png")

"""
email


images = [f"{IMG_PATH}/tabela.png"]
recipients = [
    "bernarducs@gmail.com",
    "bernardo.silva@recife.pe.gov.br"
]
subject = "Arrecadação ISS/IPTU/ITBI"
html_body = \
        '<p>Olá,</p>' \
        '<p>Segue a tabela resumo da arrecadação de ISS, IPTU e ITBI.</p>' \
        '<p>Dados provenientes dos pagamentos de DAMs.' \
        '<p><img src="cid:image0"></p>' \
        '<p>Para mais detalhes, acesso nosso painel no ' \
        '<a href="https://appfindata.ml/auth/login">FinData</a></p>' \
        '<br><br>' \
        '<p>--Equipe de Dados/Sefin</p>' \
        '</body></html>'

"""

if __name__ == "__main__":
    # mail.send_mail(images, recipients, subject, html_body)
    """
    google sheets
    """
    service = extracao.create_service()
    service_sheet = service.spreadsheets()

    sheet_id = "1FLM6-r85QF_R_UC_YwLxNzW0BsEKLRdg3hT7Z-i5RGY"
    cell_target = "Página1!A1"
    vals = df_tabela.values.tolist()
    vals = [list(df_tabela.columns)] + vals
    body = {"values": vals}

    result = service_sheet.values().update(
        spreadsheetId=sheet_id,
        range=cell_target,
        valueInputOption='RAW',
        body=body).execute()
