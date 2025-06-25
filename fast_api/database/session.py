from sqlalchemy import create_engine, engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from constants import DUCKDB_CONN, DUCKDB_CONFIG

db_engine = create_engine(
    DUCKDB_CONN,
    connect_args={'config': DUCKDB_CONFIG}
)

SessionLocal = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# def get_session():
#     with Session(bind=db_engine) as session:
#         yield session
#
# SessionDep = Annotated[Session, Depends(get_session)]
