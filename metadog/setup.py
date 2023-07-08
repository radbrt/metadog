from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import os

Base = declarative_base()

class Sources(Base):
    __tablename__ = 'sources'

    # id = Column(Integer)
    name = Column(String)
    type = Column(String)
    uri = Column(String, primary_key=True, not_null=True)
    files = relationship("Files", back_populates="source")
    tables = relationship("Tables", back_populates="source")
    # databases = relationship("Databases", back_populates="source")

class Files(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    uri = Column(String, primary_key=True, not_null=True)
    filetype = Column(String)
    file_encoding = Column(String)
    source_id = Column(Integer, ForeignKey('sources.id'))

    source = relationship("Sources", back_populates="files")
    fields = relationship("Fields", back_populates="file")

# class Databases(Base):
#     __tablename__ = 'databases'

#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     type = Column(String)
#     uri = Column(String)
#     source_id = Column(Integer, ForeignKey('sources.id'))

#     source = relationship("Sources", back_populates="databases")
#     tables = relationship("Tables", back_populates="database")


class Tables(Base):
    __tablename__ = 'tables'

    name = Column(String)
    uri = Column(String, primary_key=True, not_null=True)
    db_name = Column(String)
    schema_name = Column(String)

    source_id = Column(Integer, ForeignKey('sources.uri'))
    source = relationship("Sources", back_populates="tables")
    # database_id = Column(Integer, ForeignKey('databases.id'))
    # database = relationship("Databases", back_populates="tables")
    fields = relationship("Fields", back_populates="table")
    table_metrics = relationship("TableMetrics", back_populates="table")


class Fields(Base):
    __tablename__ = 'fields'

    name = Column(String)
    type = Column(String)
    uri = Column(String, primary_key=True, not_null=True)

    file_id = Column(Integer, ForeignKey('files.uri'))
    file = relationship("Files", back_populates="fields")

    table_id = Column(Integer, ForeignKey('tables.uri'))
    table = relationship("Tables", back_populates="fields")

    column_metrics = relationship("ColumnMetrics", back_populates="field")


class TableMetrics(Base):
    __tablename__ = 'table_metrics'

    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('tables.id'))
    table = relationship("Tables", back_populates="table_metrics")
    metric_name = Column(String)
    metric_value = Column(String)


class ColumnMetrics(Base):
    __tablename__ = 'column_metrics'

    id = Column(Integer, primary_key=True)
    field_id = Column(Integer, ForeignKey('fields.id'))
    metric_name = Column(String)
    metric_value = Column(String)

    field = relationship("Fields", back_populates="column_metrics")



# engine = create_engine("sqlite:///metadog.db")
# Base.metadata.create_all(engine)


def run_model_ddls():
    backend_uri = os.getenv("METADOG_BACKEND_URI")

    if backend_uri is None:
        raise ValueError("METADOG_BACKEND_URI environment variable is not set")

    # Create the tables if they don't already exist
    engine = create_engine(backend_uri)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    os.environ["METADOG_BACKEND_URI"] = "sqlite:///metadog.db"
    run_model_ddls()