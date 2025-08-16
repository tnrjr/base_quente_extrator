from sqlalchemy import create_engine

def create_pg_engine(user, password, host, port, db_name):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    return engine
