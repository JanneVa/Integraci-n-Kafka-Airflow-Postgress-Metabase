from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'grillos_operations',
    default_args=default_args,
    description='DAG with SQL operations on PostgreSQL',
    schedule_interval=timedelta(days=1),
)

create_schemas = PostgresOperator(
    task_id='create_schemas',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
    COMMIT;
    """,
    dag=dag,
)

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS bronze.raw_grillos (
        id VARCHAR PRIMARY KEY,
        counter INTEGER,
        fecha_registro DATE,
        fecha_nacimiento DATE,
        tipo CHAR(1),
        volumen FLOAT
    );
    COMMIT;
    """,
    dag=dag,
)

grant_privileges = PostgresOperator(
    task_id='grant_privileges',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    GRANT USAGE ON SCHEMA bronze TO dataengineer;
    GRANT USAGE ON SCHEMA silver TO dataengineer;
    GRANT USAGE ON SCHEMA gold TO dataengineer;
    GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO dataengineer;
    GRANT SELECT ON ALL TABLES IN SCHEMA silver TO dataengineer;
    GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dataengineer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO postgres;
    ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO postgres;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO postgres;
    COMMIT;
    """,
    dag=dag,
)

copy_data = PostgresOperator(
    task_id='copy_data',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    CREATE TEMP TABLE tmp_grillos AS TABLE bronze.raw_grillos WITH NO DATA;
    
    COPY tmp_grillos (id, counter, fecha_registro, fecha_nacimiento, tipo, volumen)
    FROM '/import/data_grillos.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO bronze.raw_grillos (id, counter, fecha_registro, fecha_nacimiento, tipo, volumen)
    SELECT id, counter, fecha_registro, fecha_nacimiento, tipo, volumen
    FROM tmp_grillos
    ON CONFLICT (id) DO NOTHING;

    DROP TABLE tmp_grillos;
    COMMIT;
    """,
    dag=dag,
)

create_silver_table = PostgresOperator(
    task_id='create_silver_table',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    CREATE TABLE IF NOT EXISTS silver.aggr_data AS
    SELECT 
      id,
      counter,
      fecha_registro,
      fecha_nacimiento,
      tipo,
      volumen,
      CASE
        WHEN tipo = 'R' THEN fecha_nacimiento + INTERVAL '42 days'
        WHEN tipo = 'E' THEN fecha_nacimiento + INTERVAL '45 days'
      END as fecha_reproduccion_esperada,
      CASE
        WHEN tipo = 'R' THEN fecha_nacimiento + INTERVAL '54 days'
        WHEN tipo = 'E' THEN fecha_nacimiento + INTERVAL '35 days'
      END as fecha_sacrificio_esperada
    FROM 
        bronze.raw_grillos;
    COMMIT;
    """,
    dag=dag,
)

create_gold_table = PostgresOperator(
    task_id='create_gold_table',
    postgres_conn_id='postgres_grillos',
    sql="""
    BEGIN;
    DROP TABLE IF EXISTS gold.monthly_production;
    CREATE TABLE gold.monthly_production AS
    WITH PRE AS(
        SELECT 
            count(*) as number_of_boxes,  
            EXTRACT(MONTH FROM fecha_sacrificio_esperada) as month, 
            tipo as type
        FROM silver.aggr_data
        GROUP BY 2, 3
        ORDER BY 2, 3
    ),
    PROD AS(
        SELECT 
            month,
            type,
            number_of_boxes,
            CASE 
                WHEN type = 'E' THEN number_of_boxes * 2.5
                WHEN type = 'R' THEN number_of_boxes * 1.5
            END as flour_produced_kg
        FROM PRE
    ),
    FINAL AS(
        SELECT
            month as mes_numero, 
            sum(number_of_boxes) as numero_de_cajas,
            sum(flour_produced_kg) as total_kg_harina
        FROM PROD
        GROUP BY 1
    )
    SELECT
        numero_de_cajas,
        total_kg_harina,
        CASE
            WHEN mes_numero = 1 THEN 'January'
            WHEN mes_numero = 2 THEN 'February'
            WHEN mes_numero = 3 THEN 'March'
            WHEN mes_numero = 4 THEN 'April'
            WHEN mes_numero = 5 THEN 'May'
            WHEN mes_numero = 6 THEN 'June'
            WHEN mes_numero = 7 THEN 'July'
            WHEN mes_numero = 8 THEN 'August'
            WHEN mes_numero = 9 THEN 'September'
            WHEN mes_numero = 10 THEN 'October'
            WHEN mes_numero = 11 THEN 'November'
            WHEN mes_numero = 12 THEN 'December'
        END AS mes_nombre
    FROM FINAL;
    COMMIT;
    """,
    dag=dag,
)


create_schemas >> create_tables >> grant_privileges >> copy_data >> create_silver_table >> create_gold_table
