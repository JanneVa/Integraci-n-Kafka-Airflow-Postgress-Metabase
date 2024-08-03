DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_database
      WHERE datname = 'warehouse'
   ) THEN
      CREATE DATABASE warehouse;
   END IF;
END
$$;

-- Create a new user
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE rolname = 'dataengineer'
   ) THEN
      CREATE USER dataengineer WITH PASSWORD 'postgres';
   END IF;
END
$$;

-- Grant all privileges to the new user on the datawarehouse database
GRANT ALL PRIVILEGES ON DATABASE warehouse TO postgres;

-- Switch to the datawarehouse database
\c warehouse

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Create tables
CREATE TABLE bronze.raw_grillos (
    id VARCHAR PRIMARY KEY,
    counter INTEGER,
    fecha_registro DATE,
    fecha_nacimiento DATE,
    tipo CHAR(1),
    volumen FLOAT
);

-- Grant usage on schemas to postgres user
GRANT USAGE ON SCHEMA bronze TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

-- Grant select on all tables in schemas to postgres user
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO postgres;

-- Ensure future tables in these schemas are accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO postgres;


-- Copy data from CSV file into bronze.raw_grillos
COPY bronze.raw_grillos
FROM '/import/data_grillos.csv'
DELIMITER ','
CSV HEADER;
