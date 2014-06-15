CREATE TABLE mammutdb_migrations (
       name varchar(255) UNIQUE PRIMARY KEY,
       created_at timestamp with timezone DEFAULT now()
);
