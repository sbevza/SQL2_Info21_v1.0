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
        IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'fk_tasks_parent_task') THEN
            ALTER TABLE Tasks
                ADD CONSTRAINT fk_tasks_parent_task
                    FOREIGN KEY (ParentTask) REFERENCES Tasks(Title);
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


CREATE TRIGGER trg_check_unique_null_parent_task
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
    ID SERIAL PRIMARY KEY,
    Peer VARCHAR NOT NULL,
    Task VARCHAR NOT NULL,
    Date DATE NOT NULL,
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

CREATE TRIGGER trg_verter_check_state
    BEFORE INSERT
    ON P2P
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

CREATE OR REPLACE FUNCTION fun_trg_set_points_amount()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.PointsAmount := 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_set_points_amount
    BEFORE INSERT OR UPDATE
    ON TransferredPoints
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_set_points_amount();

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
--     CONSTRAINT uk_recommendations_person_recommended_peer CHECK (
--             Peer NOT IN (SELECT unnest(string_to_array(RecommendedPeer, ', '))))
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
        WHERE ch.ID = NEW."Check") >= NEW.XPAmount
    THEN
        RAISE EXCEPTION 'XPAmount exceeds the maximum allowed for this check';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_xp_check_xp_amount
    BEFORE INSERT OR UPDATE
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_xp_check_xp_amount();

-- Создаем таблицу TimeTracking
CREATE TABLE TimeTracking
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

-- Процедура импорта данных в таблицу Peers
CREATE OR REPLACE FUNCTION import_from_csv(
    tablename text,
    filename text,
    delimiter text DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format('COPY %I FROM %L WITH CSV DELIMITER %L', tablename, filename, delimiter);
END;
$$;









CREATE OR REPLACE FUNCTION export_to_csv(
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
            substring(filename from '[^.]+'),
            filename,
            delimiter
        );
END;
$$;

-- Использование:

SELECT import_from_csv(
               'peers',
               '/Users/amazomic/SQL2_Info21_v1.0-1/src/peers.csv'
           );

SELECT import_from_csv(
               'tasks',
               '/Users/amazomic/SQL2_Info21_v1.0-1/src/Tasks.csv'
           );


SELECT import_from_csv(
               'checks',
               '/Users/amazomic/SQL2_Info21_v1.0-1/src/Checks.csv'
           );

SELECT import_from_csv(
               'p2p',
               '/Users/amazomic/SQL2_Info21_v1.0-1/src/P2P.csv'
           );



-- INSERT INTO Tasks (Title, ParentTask, MaxXP)
-- VALUES ('C2_SimpleBashUtils','Math Homework 1', 15);


COPY Checks (Peer, Task, Date) FROM '/Users/amazomic/SQL2_Info21_v1.0-1/src/Checks.csv' WITH CSV DELIMITER ',';
COPY Checks FROM '/Users/amazomic/SQL2_Info21_v1.0-1/src/Checks.csv' WITH CSV HEADER DELIMITER ',';
