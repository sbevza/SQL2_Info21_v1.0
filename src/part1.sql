-- Создаем базу данных
CREATE DATABASE school21;

-- Подключаемся к базе данных
\c school21

-- Создаем таблицу Peers
CREATE TABLE Peers (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  birthday DATE NOT NULL
);

-- Создаем таблицу Tasks
CREATE TABLE Tasks (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  parent_task VARCHAR(100),  
  max_xp INT NOT NULL
);

-- Создаем тип enum для статуса проверки
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

-- Создаем таблицу P2P
CREATE TABLE P2P (
  id SERIAL PRIMARY KEY,
  check_id INT NOT NULL,
  peer_username VARCHAR(50) NOT NULL,
  check_status check_status NOT NULL,
  checked_at TIMESTAMP NOT NULL
);

-- Создаем таблицу Verter 
CREATE TABLE Verter (
  id SERIAL PRIMARY KEY,
  check_id INT NOT NULL,
  check_status check_status NOT NULL,
  checked_at TIMESTAMP NOT NULL
); 

-- Создаем таблицу Checks
CREATE TABLE Checks (
  id SERIAL PRIMARY KEY,
  peer_username VARCHAR(50) NOT NULL,
  task_name VARCHAR(100) NOT NULL,
  checked_at DATE NOT NULL
);

-- Создаем таблицу TransferredPoints
CREATE TABLE TransferredPoints (
  id SERIAL PRIMARY KEY,
  from_peer VARCHAR(50) NOT NULL,
  to_peer VARCHAR(50) NOT NULL, 
  points_amount INT NOT NULL
);

-- Создаем таблицу Friends
CREATE TABLE Friends (
  id SERIAL PRIMARY KEY,
  peer1 VARCHAR(50) NOT NULL,
  peer2 VARCHAR(50) NOT NULL
);

-- Создаем таблицу Recommendations  
CREATE TABLE Recommendations (
  id SERIAL PRIMARY KEY,
  peer VARCHAR(50) NOT NULL,
  recommended_peer VARCHAR(50) NOT NULL
);

-- Создаем таблицу XP
CREATE TABLE XP (
  id SERIAL PRIMARY KEY,
  check_id INT NOT NULL,
  xp_amount INT NOT NULL 
);

-- Создаем таблицу TimeTracking
CREATE TABLE TimeTracking (
  id SERIAL PRIMARY KEY,
  peer VARCHAR(50) NOT NULL,
  date DATE NOT NULL, 
  time TIME NOT NULL,
  state INT NOT NULL 
);

-----------------------------------------------------------

  -- Процедура импорта данных в таблицу Peers
CREATE OR REPLACE FUNCTION import_from_csv(
    filename text, 
    delimiter char DEFAULT ','
)
RETURNS void
LANGUAGE plpgsql
AS $$
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
AS $$
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

