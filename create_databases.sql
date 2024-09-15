-- Create multiple databases if they do not exist
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE airflow');
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'test_postgres') THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE test_postgres');
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'db3') THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE db3');
    END IF;
END $$;

-- Optionally, create users and grant permissions
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'user1') THEN
        CREATE USER user1 WITH ENCRYPTED PASSWORD 'password1';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'user2') THEN
        CREATE USER user2 WITH ENCRYPTED PASSWORD 'password2';
    END IF;
END $$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE db1 TO user1;
GRANT ALL PRIVILEGES ON DATABASE db2 TO user2;
