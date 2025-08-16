from dotenv import dotenv_values
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Integer, Date, MetaData
from sqlalchemy.dialects.postgresql import VARCHAR

# Variáveis de ambiente
ENV = dotenv_values('.env')
PG_USER = ENV['PG_USER']
PG_PASSWORD = ENV['PG_PASSWORD']
PG_HOST = ENV['PG_HOST']
PG_PORT = ENV['PG_PORT']
PG_DATABASE = ENV['PG_DATABASE']

# Cria conexão com o PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')
metadata = MetaData()

# Define a estrutura da tabela dam_pago
dam_pago = Table(
    'dam_pago',
    metadata,
    Column('data', Date),
    Column('receita', VARCHAR(255)),
    Column('mes_dia_num', VARCHAR(10)),
    Column('atribuicao', VARCHAR(255)),
    Column('receita_contabil', VARCHAR(255)),
    Column('ano', Integer),
    Column('mes_num', Integer),
    Column('dia', Integer),
    Column('mes', VARCHAR(20)),
    Column('cd_nm_receita', VARCHAR(255))
)

# Cria a tabela no banco (dropa se já existir e recria)
metadata.drop_all(engine, [dam_pago])
metadata.create_all(engine)

# Lê o CSV e trata os dados
COLS = [
    'data',
    'receita',
    'mes_dia_num',
    'atribuicao',
    'receita_contabil',
    'ano',
    'mes_num',
    'dia',
    'mes',
    'cd_nm_receita'
]
file = 'output_processamento/dam_pago.csv'
df = pd.read_csv(file, sep=';', parse_dates=['data'], usecols=COLS, keep_default_na=False)
df.query('ano >= 2023', inplace=True)

# Converte a coluna 'data' para datetime.date
df['data'] = df['data'].dt.date

# Exporta os dados para o PostgreSQL
df.to_sql('dam_pago', con=engine, if_exists='append', index=False)
