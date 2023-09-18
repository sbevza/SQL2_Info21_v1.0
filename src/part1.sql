-- Создаем базу данных
CREATE DATABASE school21;

-- Подключаемся к базе данных
\c school21

-- Создаем таблицу Peers
CREATE TABLE IF NOT EXISTS Peers
(
    Nickname VARCHAR PRIMARY KEY NOT NULL,
    Birthday DATE                NOT NULL
);

-- Создаем таблицу Tasks
CREATE TABLE IF NOT EXISTS Tasks
(
    Title      VARCHAR PRIMARY KEY NOT NULL,
    ParentTask VARCHAR,
    MaxXP      INT                 NOT NULL CHECK (MaxXP > 0),
--     CONSTRAINT fk_tasks_parent_task FOREIGN KEY (ParentTask) REFERENCES Tasks (Title),
    CONSTRAINT unique_pair CHECK (ParentTask != Title)
);

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM information_schema.table_constraints
                       WHERE constraint_name = 'fk_tasks_parent_task') THEN
            ALTER TABLE Tasks
                ADD CONSTRAINT fk_tasks_parent_task
                    FOREIGN KEY (ParentTask) REFERENCES Tasks (Title);
        END IF;
    END
$$;

CREATE OR REPLACE FUNCTION fun_trg_check_unique_null_parent_task()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.ParentTask IS NULL AND
       EXISTS (SELECT 1 FROM Tasks WHERE ParentTask IS NULL AND Title != NEW.Title) THEN
        RAISE EXCEPTION 'ParentTask with NULL value already exists';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER trg_check_unique_null_parent_task
    BEFORE INSERT OR UPDATE
    ON Tasks
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_check_unique_null_parent_task();

-- Создаем тип enum для статуса проверки
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'check_status') THEN
            CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');
        END IF;
    END
$$;

-- Создаем таблицу Checks
CREATE TABLE IF NOT EXISTS Checks
(
    ID   SERIAL PRIMARY KEY,
    Peer VARCHAR NOT NULL,
    Task VARCHAR NOT NULL,
    Date DATE    NOT NULL,
    CONSTRAINT fk_checks_peer_peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_checks_task FOREIGN KEY (Task) REFERENCES Tasks (Title)
);


-- Создаем таблицу P2P
CREATE TABLE IF NOT EXISTS P2P
(
    ID           SERIAL PRIMARY KEY,
    "Check"      INT          NOT NULL,
    CheckingPeer VARCHAR      NOT NULL,
    State        check_status NOT NULL,
    Time         TIME         NOT NULL,
    CONSTRAINT fk_p2p_check FOREIGN KEY ("Check") REFERENCES Checks (ID),
    CONSTRAINT fk_p2p_checking_peer FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname)
);

CREATE OR REPLACE FUNCTION fnc_trg_p2p_check_state() RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.State = 'Start' THEN
        IF EXISTS (SELECT 1
                   FROM P2P
                   WHERE State = 'Start'
                     AND "Check" = NEW."Check") THEN
            RAISE EXCEPTION 'The check already has started';
        END IF;
    END IF;
    IF NEW.State IN ('Success', 'Failure') THEN
        IF NOT EXISTS (SELECT 1
                       FROM P2P
                       WHERE State = 'Start'
                         AND "Check" = NEW."Check") THEN
            RAISE EXCEPTION 'The check has not started yet';
        END IF;
        IF (SELECT COUNT("Check")
            FROM P2P
            WHERE "Check" = NEW."Check") >= 2
        THEN
            RAISE EXCEPTION 'The check has ended yet';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER trg_p2p_check_state
    BEFORE INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_p2p_check_state();

-- Создаем таблицу Verter
CREATE TABLE IF NOT EXISTS Verter
(
    ID      SERIAL PRIMARY KEY,
    "Check" BIGINT       NOT NULL,
    State   check_status NOT NULL,
    Time    TIME         NOT NULL,
    CONSTRAINT fk_verter_check FOREIGN KEY ("Check") REFERENCES Checks (ID)
);

CREATE OR REPLACE FUNCTION fnc_trg_verter_check_state() RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.State = 'Start' THEN
        IF EXISTS (SELECT 1
                   FROM Verter
                   WHERE State = 'Start'
                     AND "Check" = NEW."Check") THEN
            RAISE EXCEPTION 'The check already has started';
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM P2P
                       WHERE "Check" = NEW."Check"
                         AND State = 'Success')
        THEN
            RAISE EXCEPTION 'The check has not been ended successfully';
        END IF;
    END IF;
    IF NEW.State IN ('Success', 'Failure') THEN
        IF NOT EXISTS (SELECT 1
                       FROM Verter
                       WHERE State = 'Start'
                         AND "Check" = NEW."Check") THEN
            RAISE EXCEPTION 'The check has not started yet';
        END IF;
        IF (SELECT COUNT("Check")
            FROM Verter
            WHERE "Check" = NEW."Check") >= 2
        THEN
            RAISE EXCEPTION 'The check has ended yet';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_verter_check_state
    BEFORE INSERT
    ON Verter
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_verter_check_state();

-- Создаем таблицу TransferredPoints
CREATE TABLE IF NOT EXISTS TransferredPoints
(
    ID           SERIAL PRIMARY KEY,
    CheckingPeer VARCHAR NOT NULL,
    CheckedPeer  VARCHAR NOT NULL,
    PointsAmount INT     NOT NULL DEFAULT 1,
    CONSTRAINT fk_transferred_points_checking_peer
        FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_transferred_points_checked_peer
        FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname),
    CONSTRAINT ch_transferred_points_peers check (CheckingPeer <> CheckedPeer)
);

-- Создаем таблицу Friends
CREATE TABLE IF NOT EXISTS Friends
(
    ID    SERIAL PRIMARY KEY,
    Peer1 VARCHAR NOT NULL,
    Peer2 VARCHAR NOT NULL,
    CONSTRAINT fk_friends_peer1 FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    CONSTRAINT fk_friends_peer2 FOREIGN KEY (Peer2) REFERENCES Peers (Nickname),
    CONSTRAINT ch_friends_peers check (Peer1 <> Peer2)
);

-- Создаем таблицу Recommendations
CREATE TABLE IF NOT EXISTS Recommendations
(
    ID              SERIAL PRIMARY KEY,
    Peer            VARCHAR NOT NULL,
    RecommendedPeer VARCHAR,
    CONSTRAINT fk_recommendations_peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_recommendations_recommended_peer FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname),
    CONSTRAINT ch_recommendations_peers CHECK (Peer <> RecommendedPeer)
);

-- Создаем таблицу XP
CREATE TABLE IF NOT EXISTS XP
(
    ID       SERIAL PRIMARY KEY,
    "Check"  INT NOT NULL,
    XPAmount INT NOT NULL CHECK (XPAmount > 0)
);

CREATE OR REPLACE FUNCTION fun_trg_xp_check_xp_amount()
    RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT t.MaxXP
        FROM Checks ch
                 LEFT JOIN Tasks t on ch.Task = t.Title
        WHERE ch.ID = NEW."Check") < NEW.XPAmount
    THEN
        RAISE EXCEPTION 'XPAmount exceeds the maximum allowed for this check';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE  TRIGGER trg_xp_check_xp_amount
    BEFORE INSERT OR UPDATE
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_xp_check_xp_amount();

-- Создаем таблицу TimeTracking
CREATE TABLE IF NOT EXISTS TimeTracking
(
    ID    SERIAL PRIMARY KEY,
    Peer  VARCHAR NOT NULL,
    Date  DATE    NOT NULL,
    Time  TIME    NOT NULL,
    State INT     NOT NULL CHECK (State IN (1, 2)),
    CONSTRAINT fk_time_tracking FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);

CREATE OR REPLACE FUNCTION fun_trg_xp_tracking_check_state()
    RETURNS TRIGGER AS
$$
DECLARE
    last_state INT;
BEGIN
    SELECT State
    INTO last_state
    FROM TimeTracking
    WHERE Peer = NEW.Peer
      AND Date = NEW.Date
    ORDER BY ID DESC
    LIMIT 1;

    IF NEW.State = 1 THEN
        IF last_state IS NULL OR last_state = 2 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'Invalid State transition: Entry without Exit';
        END IF;
    ELSIF NEW.State = 2 THEN
        IF last_state = 1 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'Invalid State transition: Exit without Entry';
        END IF;
    END IF;
    RAISE EXCEPTION 'Invalid State transition';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_time_tracking_check_state
    BEFORE INSERT
    ON TimeTracking
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_xp_tracking_check_state();
-----------------------------------------------------------

-- Процедура импорта данных в таблицу
CREATE OR REPLACE FUNCTION import_from_csv(
    tablename text,
    filename text,
    delimiter text DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    seq_name text;
    max_id bigint;
BEGIN
    EXECUTE format('COPY %I FROM %L WITH CSV DELIMITER %L', tablename, filename, delimiter);
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = tablename
          AND column_name = 'id'
    ) THEN
        SELECT pg_get_serial_sequence(tablename, 'id') INTO seq_name;
        EXECUTE format('SELECT MAX(id) FROM %I', tablename) INTO max_id;
        IF max_id IS NOT NULL THEN
            EXECUTE format('SELECT setval(%L, %s)', seq_name, max_id);
        END IF;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION export_to_csv(
    tablename text,
    filename text,
    delimiter char DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format(
            'COPY %I TO %L WITH CSV DELIMITER %L',
            tablename,
            filename,
            delimiter
        );
END;
$$;

-- Заполнения таблиц командами импорта из таблиц:
DO
$$
    DECLARE
        path_dir text;
    BEGIN
        path_dir := '/Users/amazomic/SQL2_Info21_v1.0-1/src/';
        --         path_dir := '/tmp/';

        -- Очищаем другие таблицы перед импортом
        TRUNCATE TABLE checks CASCADE;
        TRUNCATE TABLE peers CASCADE;
        TRUNCATE TABLE tasks CASCADE;
        TRUNCATE TABLE p2p CASCADE;
        TRUNCATE TABLE verter CASCADE;
        TRUNCATE TABLE transferredpoints CASCADE;
        TRUNCATE TABLE friends CASCADE;
        TRUNCATE TABLE recommendations CASCADE;
        TRUNCATE TABLE xp CASCADE;
        TRUNCATE TABLE timetracking CASCADE;

        PERFORM import_from_csv(
                'peers',
                path_dir || 'Peers.csv'
            );

        PERFORM import_from_csv(
                'tasks',
                path_dir || 'Tasks.csv'
            );

        PERFORM import_from_csv(
                'checks',
                path_dir || 'Checks.csv'
            );

        PERFORM import_from_csv(
                'p2p',
                path_dir || 'P2P.csv'
            );

        PERFORM import_from_csv(
                'verter',
                path_dir || 'Verter.csv'
            );

        PERFORM import_from_csv(
                'transferredpoints',
                path_dir || 'TransferredPoints.csv'
            );

        PERFORM import_from_csv(
                'friends',
                path_dir || 'Friends.csv'
            );

        PERFORM import_from_csv(
                'recommendations',
                path_dir || 'Recommendations.csv'
            );

        PERFORM import_from_csv(
                'xp',
                path_dir || 'XP.csv'
            );

        PERFORM import_from_csv(
                'timetracking',
                path_dir || 'TimeTracking.csv'
            );

    END
$$;

DO
$$
    DECLARE
        path_dir text;
    BEGIN
        path_dir := '/Users/amazomic/SQL2_Info21_v1.0-1/src/';

        PERFORM export_to_csv(
                'peers',
                path_dir || 'Peers.csv'
            );

        PERFORM export_to_csv(
                'tasks',
                path_dir || 'Tasks.csv'
            );

        PERFORM export_to_csv(
                'checks',
                path_dir || 'Checks.csv'
            );

        PERFORM export_to_csv(
                'p2p',
                path_dir || 'P2P.csv'
            );

        PERFORM export_to_csv(
                'verter',
                path_dir || 'Verter.csv'
            );

        PERFORM export_to_csv(
                'transferredpoints',
                path_dir || 'TransferredPoints.csv'
            );

        PERFORM export_to_csv(
                'friends',
                path_dir || 'Friends.csv'
            );

        PERFORM export_to_csv(
                'recommendations',
                path_dir || 'Recommendations.csv'
            );

        PERFORM export_to_csv(
                'xp',
                path_dir || 'XP.csv'
            );

        PERFORM export_to_csv(
                'timetracking',
                path_dir || 'TimeTracking.csv'
            );

    END
$$;




