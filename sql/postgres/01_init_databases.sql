SELECT 'CREATE DATABASE mrr OWNER dwh_user'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mrr')\gexec

SELECT 'CREATE DATABASE stg OWNER dwh_user'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stg')\gexec

SELECT 'CREATE DATABASE dwh OWNER dwh_user'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh')\gexec