from airflow import settings
from airflow.models import Connection
from airflow.utils.session import create_session

def create_postgres_connection():
    conn = Connection(
        conn_id='postgres_grillos',
        conn_type='postgres',
        description="Postgres DB for grillos.",
        host='postgres_warehouse',
        schema='warehouse',
        login='postgres',
        password='postgres',
        port=5432
    )
    
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
    session.add(conn)
    session.commit()

if __name__ == "__main__":
    create_postgres_connection()
