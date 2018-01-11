CREATE OR REPLACE FUNCTION @extschema@.fn_issue_notify()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_record   RECORD;
BEGIN
    IF( TG_OP IN( 'UPDATE', 'INSERT' ) ) THEN
        IF( TG_OP = 'UPDATE' AND NEW::TEXT IS NOT DISTINCT FROM OLD::TEXT ) THEN
            RETURN NEW;
        END IF;

        my_record := NEW;
    ELSE
        my_record := OLD;
    END IF;

    IF( current_setting( '@extschema@.enable_logging', TRUE )::BOOLEAN IS NOT FALSE ) THEN
        PERFORM pg_notify( n.nspname::VARCHAR || '.' || c.relname::VARCHAR, TG_OP )
           FROM pg_class c
           JOIN pg_namespace n
             ON n.oid = c.relnamespace
          WHERE c.oid = TG_RELID;
    END IF;

    RETURN my_record;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_get_table_oid
(
    in_schema   VARCHAR,
    in_table    VARCHAR
)
RETURNS OID AS
 $_$
    SELECT c.oid
      FROM pg_class c
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
       AND n.nspname::VARCHAR = in_schema
     WHERE c.relname::VARCHAR = in_table;
 $_$
    LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE TABLE @extschema@.tb_catalog_table
(
    oid         OID NOT NULL,
    schema_name VARCHAR NOT NULL DEFAULT 'public',
    table_name  VARCHAR NOT NULL,
    row_count   BIGINT,
    UNIQUE( schema_name, table_name )
)
WITH
(
    OIDS = FALSE
);

CREATE OR REPLACE FUNCTION @extschema@.fn_install_triggers
(
    in_schema   VARCHAR,
    in_table    VARCHAR
)
RETURNS VOID AS
 $_$
DECLARE
    my_oid   OID;
    my_count BIGINT;
BEGIN
    my_oid := @extschema@.fn_get_table_oid(
        in_schema,
        in_table
    );

    EXECUTE 'SELECT COUNT(*) FROM ' || in_schema || '.' || in_table
       INTO my_count;

    INSERT INTO @extschema@.tb_catalog_table
                (
                    oid,
                    schema_name,
                    table_name,
                    row_count
                )
         VALUES
                (
                    my_oid,
                    in_schema,
                    in_table,
                    my_count
                );

    EXECUTE 'CREATE TRIGGER tr_log_table_modification '
         || '    AFTER INSERT OR DELETE OR UPDATE  ON ' || in_schema || '.' || in_table
         || '    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_issue_notify();';

    RETURN;
END
 $_$
    LANGUAGE plpgsql VOLATILE STRICT;

CREATE UNLOGGED TABLE @extschema@.tb_catalog_table_modification
(
    oid         OID NOT NULL,
    op          CHAR NOT NULL,
    recorded    TIMESTAMP NOT NULL
)
WITH
(
    OIDS = FALSE
);

CREATE OR REPLACE FUNCTION @extschema@.fn_log_action
(
    in_oid      OID,
    in_op       CHAR,
    in_record   TEXT
)
RETURNS VOID AS
 $_$
    INSERT INTO @extschema@.tb_catalog_table_modification
                (
                    oid,
                    op,
                    recorded
                )
         VALUES
                (
                    in_oid,
                    in_op,
                    clock_timestamp()
                );
 $_$
    LANGUAGE SQL VOLATILE PARALLEL UNSAFE;

WITH tt_tables AS
(
    SELECT n.nspname::VARCHAR AS schema_name,
           c.relname::VARCHAR AS table_name
      FROM pg_class c
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
       AND n.nspname::VARCHAR = 'public'
     WHERE c.relkind = 'r'
)
    SELECT @extschema@.fn_install_triggers(
               schema_name,
               table_name
           )
      FROM tt_tables;
