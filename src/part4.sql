CREATE DATABASE school21_test;

\c school21

-- Main part
-----------------ex01-----------------
CREATE OR REPLACE PROCEDURE drop_tables_with_prefix()
    LANGUAGE plpgsql
AS
$$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN (SELECT tablename
                       FROM pg_tables
                       WHERE schemaname = 'public'
                         AND tablename LIKE 'tablename%')
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS public.' || table_name;
        END LOOP;
END;
$$;

-- Пример использования
CREATE OR REPLACE PROCEDURE generate_tables()
    LANGUAGE plpgsql
AS
$$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 5
        LOOP
            EXECUTE 'CREATE TABLE IF NOT EXISTS TableName_' || i || ' (id SERIAL PRIMARY KEY, name VARCHAR(50))';
            i := i + 1;
        END LOOP;

    i := 1;
    WHILE i <= 5
        LOOP
            EXECUTE 'CREATE TABLE IF NOT EXISTS OtherTable_' || i || ' (id SERIAL PRIMARY KEY, name VARCHAR(50))';
            i := i + 1;
        END LOOP;
END;
$$;

CALL generate_tables();
CALL drop_tables_with_prefix();

-----------------ex02-----------------
CREATE OR REPLACE PROCEDURE list_user_scalar_functions(OUT function_count INTEGER)
    LANGUAGE plpgsql
AS
$$
DECLARE
    function_info TEXT;
BEGIN
    function_count = 0;
    FOR function_info IN (SELECT r.routine_name || '(' || string_agg(p.parameter_name, ',') || ')' AS function_name
                          FROM information_schema.routines r
                                   JOIN information_schema.parameters p ON r.specific_name = p.specific_name
                          WHERE r.specific_schema = 'public'
                            AND r.data_type IN ('integer', 'smallint', 'bigint', 'real', 'numeric'
                            , 'int', 'double precision', 'decimal')
                            AND r.routine_type = 'FUNCTION'
                          GROUP BY r.routine_name)
        LOOP
            function_count = function_count + 1;
            RAISE NOTICE '%', function_info;
        END LOOP;
END;
$$;

-- Пример использования
CREATE OR REPLACE PROCEDURE list_functions_with_count()
    LANGUAGE plpgsql
AS
$$
DECLARE
    function_count INTEGER;
BEGIN
    CALL list_user_scalar_functions(function_count);
    RAISE NOTICE 'Найдено % функций', function_count;
END;
$$;

CALL list_functions_with_count();

CREATE OR REPLACE FUNCTION add_numbers(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN a + b;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_numbers_alias1(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN add_numbers(a, b);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_numbers_alias2(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN add_numbers(a, b);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_numbers_alias3(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN add_numbers(a, b);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_numbers_alias4(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN add_numbers(a, b);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_numbers_alias5(a INT, b INT)
    RETURNS INT
AS
$$
BEGIN
    RETURN add_numbers(a, b);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION my_scalar_function42()
    RETURNS INTEGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    result INTEGER;
BEGIN
    result := 42;
    RETURN result;
END;
$$;

-----------------ex03-----------------
CREATE OR REPLACE PROCEDURE drop_all_triggers(OUT trigger_count INT)
    LANGUAGE plpgsql
AS
$$
DECLARE
    trigger_name_local TEXT;
    table_name_local   TEXT;
BEGIN
    trigger_count := 0;
    FOR trigger_name_local, table_name_local IN
        SELECT trigger_name, event_object_table
        FROM information_schema.triggers
        WHERE event_object_schema = 'public'
          AND trigger_schema = 'public'
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(trigger_name_local)
                        || ' ON ' || quote_ident(table_name_local) || ' CASCADE';

            RAISE NOTICE 'Found % ', table_name_local;
            RAISE NOTICE 'Found % ', trigger_name_local;
            trigger_count := trigger_count + 1;
        END LOOP;
END;
$$;

-- Пример использования
CREATE OR REPLACE PROCEDURE drop_all_triggers_with_count()
    LANGUAGE plpgsql
AS
$$
DECLARE
    trigger_count INTEGER;
BEGIN
    CALL drop_all_triggers(trigger_count);
    RAISE NOTICE 'Found % triggers', trigger_count;
END;
$$;

CALL drop_all_triggers_with_count();

CREATE TABLE IF NOT EXISTS peers_test
(
    Nickname VARCHAR PRIMARY KEY NOT NULL,
    Birthday DATE                NOT NULL
);

CREATE OR REPLACE FUNCTION example1_after_insert()
    RETURNS TRIGGER AS
$$
BEGIN
    RAISE NOTICE 'Запись добавлена';
    RETURN NEW;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER example1_after_insert_trigger
    AFTER INSERT
    ON peers_test
    FOR EACH ROW
EXECUTE FUNCTION example1_after_insert();

CREATE TRIGGER example1_after_insert_trigger2
    AFTER INSERT
    ON peers_test
    FOR EACH ROW
EXECUTE FUNCTION example1_after_insert();

-----------------ex04-----------------
CREATE OR REPLACE PROCEDURE search_functions_by_text(IN search_pattern TEXT)
    LANGUAGE plpgsql
AS
$$
DECLARE
    function_info RECORD;
BEGIN
    FOR function_info IN
        SELECT r.routine_name AS function_name, r.routine_type AS object_type
        FROM information_schema.routines r
        WHERE r.specific_schema = 'public'
          AND (r.data_type IS NULL
            OR r.data_type IN ('integer', 'smallint', 'bigint', 'real', 'numeric', 'int', 'double precision', 'decimal'))
          AND r.routine_type IN ('FUNCTION', 'PROCEDURE')
          AND position(search_pattern IN r.routine_definition) > 0
        LOOP
            RAISE NOTICE 'Object Name: %, Object Type: %', function_info.function_name, function_info.object_type;
        END LOOP;
END;
$$;

-- Пример использования
CALL search_functions_by_text('Peers');

