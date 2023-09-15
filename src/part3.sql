-- 1. Вывод TransferredPoints в читаемом виде
CREATE OR REPLACE FUNCTION show_transferred_points()
    RETURNS TABLE
            (
                Peer1        VARCHAR,
                Peer2        VARCHAR,
                PointsAmount BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT CASE
                             WHEN tp1.checkingPeer < tp1.checkedPeer THEN tp1.checkingPeer
                             ELSE tp1.checkedPeer
                             END AS Peer1,
                         CASE
                             WHEN tp1.checkingPeer < tp1.checkedPeer THEN tp1.checkedPeer
                             ELSE tp1.checkingPeer
                             END AS Peer2,
                         SUM(
                                 CASE
                                     WHEN tp1.checkingPeer < tp1.checkedPeer THEN tp1.pointsAmount
                                     ELSE -tp1.pointsAmount
                                     END
                             )   AS PointsAmount
                  FROM transferredPoints tp1
                           LEFT JOIN transferredPoints tp2
                                     ON tp1.checkingPeer = tp2.checkedPeer
                                         AND tp1.checkedPeer = tp2.checkingPeer
                                         AND tp1.id > tp2.id
                  GROUP BY Peer1, Peer2
                  ORDER BY Peer1 -- Добавлена сортировка по Peer1
    );

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT *
FROM show_transferred_points();

-- 2. Получение выполненных заданий и XP
CREATE OR REPLACE FUNCTION show_user_tasks_xp()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task VARCHAR,
                XP   INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT DISTINCT c.Peer,
                                  t.Title     AS Task,
                                  xp.XPAmount AS XP
                  FROM P2P p
                           JOIN Checks c ON p."Check" = c.ID
                           JOIN Tasks t ON c.Task = t.Title
                           JOIN XP xp ON c.ID = xp."Check"
                           LEFT JOIN Verter v ON c.ID = v."Check"
                  WHERE p.State = 'Success'
                    AND (v.State IS NULL OR v.State != 'Failure'));

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT *
FROM show_user_tasks_xp();

-- 3. Поиск пиров, не покидавших кампус
CREATE OR REPLACE FUNCTION find_peers_inside_campus(day DATE)
    RETURNS TABLE
            (
                Peer VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT DISTINCT t.Peer
                  FROM TimeTracking t
                  WHERE t.Date = day
                    AND t.State = 1
                    AND NOT EXISTS (SELECT 1
                                    FROM TimeTracking t2
                                    WHERE t2.Peer = t.Peer
                                      AND t2.Date = day
                                      AND t2.State = 2));

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage: 
SELECT *
FROM find_peers_inside_campus('2023-01-15');

-- 4. Расчет изменения пир-поинтов
CREATE OR REPLACE FUNCTION calculate_peer_points_change()
    RETURNS TABLE
            (
                Peer         VARCHAR,
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT Subquery.Peer,
                         CAST(SUM(Subquery.PointsChange) AS BIGINT) AS PointsChange
                  FROM (SELECT tp.CheckingPeer      AS Peer,
                               SUM(tp.PointsAmount) AS PointsChange
                        FROM TransferredPoints tp
                        GROUP BY tp.CheckingPeer
                        UNION ALL
                        SELECT tp.CheckedPeer        AS Peer,
                               -SUM(tp.PointsAmount) AS PointsChange
                        FROM TransferredPoints tp
                        GROUP BY tp.CheckedPeer) AS Subquery
                  GROUP BY Subquery.Peer)
        ORDER BY PointsChange DESC;

    RETURN;
END;
$$ LANGUAGE plpgsql;


-- Usage:
SELECT *
FROM calculate_peer_points_change();

-- 5. Самые часто проверяемые задания
CREATE OR REPLACE FUNCTION calculate_peer_points_change_from_first_function()
    RETURNS TABLE
            (
                Peer         VARCHAR,
                PointsChange BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT Subquery.Peer,
                         CAST(SUM(Subquery.PointsChange) AS BIGINT) AS PointsChange
                  FROM (SELECT tp.Peer1             AS Peer,
                               SUM(tp.PointsAmount) AS PointsChange
                        FROM show_transferred_points() tp
                        GROUP BY tp.Peer1
                        UNION ALL
                        SELECT tp.Peer2              AS Peer,
                               -SUM(tp.PointsAmount) AS PointsChange
                        FROM show_transferred_points() tp
                        GROUP BY tp.Peer2) AS Subquery
                  GROUP BY Subquery.Peer
                  ORDER BY PointsChange DESC);

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT *
FROM calculate_peer_points_change_from_first_function();


-- 6. Поиск пиров, выполнивших весь блок
CREATE OR REPLACE FUNCTION mostFrequentTasksPerDay()
    RETURNS TABLE
            (
                Day         DATE,
                PopularTask VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY (WITH TaskRanks AS (SELECT DATE(c.Date)                                                   AS "Day",
                                            t.Title                                                        AS "Task",
                                            COUNT(*)                                                       AS TaskCount,
                                            RANK() OVER (PARTITION BY DATE(c.Date) ORDER BY COUNT(*) DESC) AS TaskRank
                                     FROM Checks c
                                              JOIN Tasks t ON c.Task = t.Title
                                     GROUP BY "Day", "Task")
                  SELECT "Day", "Task"
                  FROM TaskRanks
                  WHERE TaskRank = 1);
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT *
FROM mostFrequentTasksPerDay();


-- 7. Нахождение рекомендуемых пиров
-- Правильное имя процедуры
CREATE OR REPLACE PROCEDURE find_peers_completed_block(block_name VARCHAR)
AS
$$
DECLARE
    last_task_name VARCHAR;
BEGIN
    -- Находим последнее задание в блоке
    SELECT get_last_task_in_block(block_name) INTO last_task_name;

    -- Выводим имена пиров, которые успешно выполнили это задание
    PERFORM
    FROM Checks c
    WHERE c.Task = last_task_name
      AND c.id IN (SELECT p2p."Check"
                   FROM P2P
                   WHERE p2p.State = 'Success')
      AND NOT EXISTS (SELECT 1
                      FROM Verter v
                      WHERE v."Check" = c.ID
                        AND v.State = 'Failure')
    ORDER BY c.Date DESC; -- Сортировка по убыванию даты completion_date

END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT *
FROM find_peers_completed_block('C');


---- Доп функция получения посследнего задания блока
CREATE OR REPLACE FUNCTION get_last_task_in_block(block_name VARCHAR)
    RETURNS VARCHAR
AS
$$
DECLARE
    last_task_name VARCHAR;
BEGIN
    SELECT MAX(Title)
    INTO last_task_name
    FROM Tasks
    WHERE Title LIKE (block_name || '_%');

    RETURN last_task_name;
END;
$$ LANGUAGE plpgsql;

SELECT get_last_task_in_block('D'); -- Возвращает название последней задачи в блоке 'C'


-- 8. Расчет процента пиров, приступивших к блокам
CREATE PROCEDURE get_started_block_percents(
    block1 VARCHAR,
    block2 VARCHAR
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    total       INTEGER;
    block1_only INTEGER;
    block2_only INTEGER;
    both        INTEGER;
BEGIN
    SELECT COUNT(*) INTO total FROM peers;

    SELECT COUNT(DISTINCT c.peer)
    INTO block1_only
    FROM checks c
    WHERE c.task LIKE block1 || '%'
      AND c.peer NOT IN (SELECT peer
                         FROM checks
                         WHERE task LIKE block2 || '%');

    -- Вычисление block2_only аналогично

    SELECT COUNT(DISTINCT c.peer)
    INTO both
    FROM checks c
    WHERE c.task LIKE block1 || '%'
      AND c.peer IN (
        SELECT peer
        FROM checks
        WHERE task LIKE block2 || '%'
        );

    -- Вывод процентов
    RAISE INFO 'Started only %: %', block1, ROUND(100.0 * block1_only / total);
    RAISE INFO 'Started only %: %', block2, ROUND(100.0 * block2_only / total);
    RAISE INFO 'Started both: %', ROUND(100.0 * both / total);
    RAISE INFO 'Started none: %', ROUND(100.0 * (total - both - block1_only - block2_only) / total);
END;
$$

-- 9. Процент пиров, прошедших проверки в день рождения
CREATE PROCEDURE get_bday_check_percents()
    LANGUAGE plpgsql
AS
$$
DECLARE
    total        INTEGER;
    successful   INTEGER;
    unsuccessful INTEGER;
BEGIN
    SELECT COUNT(*) INTO total FROM peers;

    SELECT COUNT(DISTINCT c.peer)
    INTO successful
    FROM checks c
             JOIN peers p
                  ON c.peer = p.nickname
    WHERE EXTRACT(MONTH FROM c.date) = EXTRACT(MONTH FROM p.birthday)
      AND EXTRACT(DAY FROM c.date) = EXTRACT(DAY FROM p.birthday)
      AND c.id IN (SELECT
        check
    FROM p2p
    WHERE
    state = 'Success' );

    -- Аналогично для unsuccessful

    RAISE INFO 'Successful: %', ROUND(100.0 * successful / total, 1);
    RAISE INFO 'Unsuccessful: %', ROUND(100.0 * unsuccessful / total, 1);

END;
$$

-- 10. Поиск пиров, выполнивших задания 1 и 2, но не 3
CREATE FUNCTION get_peers_tasks(
    task1 VARCHAR,
    task2 VARCHAR,
    task3 VARCHAR
)
    RETURNS TABLE
            (
                peer VARCHAR
            )
AS
$$
SELECT p.nickname AS peer
FROM peers p
WHERE p.nickname IN (SELECT peer
                     FROM checks
                     WHERE task IN (task1, task2)
                       AND id IN (SELECT check
FROM p2p
WHERE state = 'Success'
    )
    )
  AND p.nickname NOT IN (
SELECT peer
FROM checks
WHERE task = task3
    )
$$ LANGUAGE sql;

-- 11. Подсчет предшествующих заданий
-- через рекурсивный CTE




-- 12. Используя рекурсивное обобщенное табличное выражение,
-- для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE PROCEDURE task_predecessor_count(INOUT ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH RECURSIVE TaskHierarchy AS (SELECT title      AS Task,
                                                parenttask AS ParentTask
                                         FROM tasks
                                         WHERE parenttask IS NOT NULL
                                         UNION ALL
                                         SELECT t.title,
                                                t.parenttask
                                         FROM tasks t
                                                  JOIN TaskHierarchy th ON t.parenttask = th.Task)
        SELECT t.title AS Task, COUNT(th.ParentTask) AS PrevCount
        FROM tasks t
                 LEFT JOIN TaskHierarchy th ON t.title = th.Task
        GROUP BY t.title
        ORDER BY t.title;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL task_predecessor_count('ref');
FETCH ALL FROM ref;
CLOSE ref;
END;

-- 13. Найти "удачные" для проверок дни. День считается "удачным", если в нем
-- есть хотя бы N идущих подряд успешных проверки
CREATE OR REPLACE PROCEDURE find_successful_check_days(
    N INTEGER, INOUT ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH CheckData AS (SELECT ch.date,
                                  p.id,
                                  p.time,
                                  p.state,
                                  SUM(CASE
                                          WHEN p.state = 'Failure' THEN
                                              1
                                          ELSE 0
                                      END)
                                  OVER (PARTITION BY ch.date ORDER BY p.time) AS reset_counter
                           FROM checks ch
                                    JOIN p2p p ON ch.id = p."Check"),

             CheckDataSuccess AS (SELECT date,
                                         id,
                                         time,
                                         state,
                                         ROW_NUMBER() OVER (PARTITION BY date, reset_counter ORDER BY time) AS consecutive_success_count
                                  FROM CheckData
                                  WHERE state = 'Success'
                                  ORDER BY date, id)
        SELECT date
        FROM CheckDataSuccess
        GROUP BY date
        HAVING MAX(consecutive_success_count) >= N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_successful_check_days(2, 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;

-- 14. Определить пира с наибольшим количеством XP find_peer_with_highest_xp
CREATE OR REPLACE PROCEDURE find_peer_with_highest_xp(INOUT ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        WITH XPTable AS (SELECT ch.peer,
                                xp.xpamount AS XP
                         FROM checks ch
                                  JOIN xp ON ch.id = xp."Check")
        SELECT p.Nickname,
               XP_table.XP_amount
        FROM Peers p
                 JOIN (SELECT XPTable.Peer, SUM(XP) AS XP_amount
                       FROM XPTable
                       GROUP BY XPTable.Peer
                       HAVING SUM(XP) = (SELECT MAX(total_xp)
                                         FROM (SELECT SUM(XP) AS total_xp FROM XPTable GROUP BY Peer) AS max_xp)) AS XP_table
                      ON p.Nickname = XP_table.Peer;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_peer_with_highest_xp('ref');
FETCH ALL FROM ref;
CLOSE ref;
END;

-- 15. Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
CREATE OR REPLACE PROCEDURE find_peers_early_arrivals_count(
    IN time_ TIME, IN N INTEGER, INOUT ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer AS Nickname
        FROM TimeTracking
        WHERE EXTRACT(HOUR FROM Time) < EXTRACT(HOUR FROM time_)
          AND state = 1
        GROUP BY Peer
        HAVING COUNT(*) >= N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_peers_early_arrivals_count('14:00:00', 2, 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;

-- 16. Определить пиров, выходивших за последние N дней из кампуса больше M раз
CREATE OR REPLACE PROCEDURE find_peer_activity(IN N INTEGER, IN M INTEGER, INOUT ref REFCURSOR)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT peers.Nickname AS Peers
        FROM peers
                 JOIN TimeTracking ON Peers.Nickname = TimeTracking.Peer
        WHERE TimeTracking.Date >= CURRENT_DATE - N * INTERVAL '1 days'
          AND state = 1
        GROUP BY peers.nickname
        HAVING COUNT(*) > M;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_peer_activity(300, 1, 'ref');
FETCH ALL FROM ref;
COMMIT;
END;

-- 17. Определить для каждого месяца процент ранних входов
CREATE OR REPLACE PROCEDURE calculate_early_entry_percentage(INOUT ref REFCURSOR) AS
$$
BEGIN
    OPEN ref FOR
        SELECT TO_CHAR(Birthday, 'Month')::VARCHAR AS Month,
               ROUND((SUM(CASE
                              WHEN EXTRACT(HOUR FROM Time) < 12 THEN 1
                              ELSE 0
                   END) * 100.0 / COUNT(*)), 2)    AS EarlyEntries
        FROM Peers
                 JOIN TimeTracking ON Peers.Nickname = TimeTracking.Peer
        GROUP BY Month, peers.birthday
        ORDER BY EXTRACT(MONTH FROM Birthday);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL calculate_early_entry_percentage('ref');
FETCH ALL FROM ref;
CLOSE ref;
END;
