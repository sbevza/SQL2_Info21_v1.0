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
    MaxXP      INT                 NOT NULL,
    CONSTRAINT fk_tasks_parent_task FOREIGN KEY (ParentTask) REFERENCES Tasks (Title),
    CONSTRAINT unique_pair CHECK (ParentTask != Title)
);

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
CREATE TABLE Checks
(
    ID   SERIAL PRIMARY KEY,
    Peer VARCHAR NOT NULL,
    Task VARCHAR NOT NULL,
    Date DATE    NOT NULL,
    CONSTRAINT fk_checks_peer_nickname FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_checks_task_title FOREIGN KEY (Task) REFERENCES Tasks (Title)
);

-- Создаем таблицу P2P
CREATE TABLE IF NOT EXISTS P2P
(
    ID           SERIAL PRIMARY KEY,
    "Check"      INT          NOT NULL,
    CheckingPeer VARCHAR      NOT NULL,
    State        check_status NOT NULL,
    Time         TIME         NOT NULL,
    CONSTRAINT fk_p2p_check_id FOREIGN KEY ("Check") REFERENCES Checks (ID),
    CONSTRAINT fk_p2p_checking_peer_nickname FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname)
);

-- Создаем таблицу Verter 
CREATE TABLE IF NOT EXISTS Verter
(
    ID      SERIAL PRIMARY KEY,
    "Check" BIGINT       NOT NULL,
    State   check_status NOT NULL,
    Time    TIME         NOT NULL,
    CONSTRAINT fk_verter_check_id FOREIGN KEY ("Check") REFERENCES Checks (ID)
);

-- Создаем таблицу TransferredPoints
CREATE TABLE IF NOT EXISTS TransferredPoints
(
    ID           SERIAL PRIMARY KEY,
    CheckingPeer VARCHAR NOT NULL,
    CheckedPeer  VARCHAR NOT NULL,
    PointsAmount INT     NOT NULL,
    CONSTRAINT fk_transferred_points_checking_peer_nickname
        FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_transferred_points_checked_peer_nickname
        FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
);

-- Создаем таблицу Friends
CREATE TABLE Friends
(
    id    SERIAL PRIMARY KEY,
    peer1 VARCHAR     NOT NULL,
    peer2 VARCHAR(50) NOT NULL
);

-- Создаем таблицу Recommendations  
CREATE TABLE Recommendations
(
    id               SERIAL PRIMARY KEY,
    peer             VARCHAR(50) NOT NULL,
    recommended_peer VARCHAR(50) NOT NULL
);

-- Создаем таблицу XP
CREATE TABLE XP
(
    id        SERIAL PRIMARY KEY,
    check_id  INT NOT NULL,
    xp_amount INT NOT NULL
);

-- Создаем таблицу TimeTracking
CREATE TABLE TimeTracking
(
    id    SERIAL PRIMARY KEY,
    peer  VARCHAR(50) NOT NULL,
    date  DATE        NOT NULL,
    time  TIME        NOT NULL,
    state INT         NOT NULL
);

-----------------------------------------------------------

-- Процедура импорта данных в таблицу Peers
CREATE OR REPLACE FUNCTION import_from_csv(
    filename text,
    delimiter char DEFAULT ','
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format('COPY %I FROM %L WITH CSV HEADER DELIMITER %L',
                   substring(filename from '[^.]+'),
                   filename,
                   delimiter);
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
            'COPY %I TO %L WITH CSV HEADER DELIMITER %L',
            substring(filename from '[^.]+'),
            filename,
            delimiter
        );
END;
$$;

-- Использование:

-- SELECT export_to_csv('peers.csv', '|');
-- SELECT import_from_csv('peers.csv', '|');

