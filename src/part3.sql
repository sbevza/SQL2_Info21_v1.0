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
CREATE OR REPLACE PROCEDURE calculate_peer_points_change(INOUT ref REFCURSOR)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT Subquery.Peer,
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
        GROUP BY Subquery.Peer
        ORDER BY PointsChange DESC;

END;
$$ LANGUAGE plpgsql;

-- Usage:
BEGIN;
CALL calculate_peer_points_change( 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;


-- 5. Самые часто проверяемые задания
CREATE OR REPLACE PROCEDURE calculate_peer_points_change_from_first_function(INOUT ref REFCURSOR)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT Subquery.Peer,
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
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;


-- Usage:
BEGIN;
CALL calculate_peer_points_change_from_first_function('ref');
FETCH ALL FROM ref;
CLOSE ref;
END;


-- 6. Поиск пиров, выполнивших весь блок
CREATE OR REPLACE PROCEDURE mostFrequentTasksPerDay(inOUT ref REFCURSOR)
AS
$$
BEGIN
    OPEN ref FOR
        WITH TaskRanks AS (
            SELECT DATE(c.Date) AS "Day",
                   t.Title AS "Task",
                   COUNT(*) AS TaskCount,
                   RANK() OVER (PARTITION BY DATE(c.Date) ORDER BY COUNT(*) DESC) AS TaskRank
            FROM Checks c
                     JOIN Tasks t ON c.Task = t.Title
            GROUP BY "Day", "Task"
        )
        SELECT "Day", "Task"
        FROM TaskRanks
        WHERE TaskRank = 1;
END;
$$ LANGUAGE plpgsql;


-- Usage:
BEGIN;
CALL mostFrequentTasksPerDay('ref');
FETCH ALL FROM ref;
CLOSE ref;
END;


-- 7. Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания

CREATE OR REPLACE PROCEDURE find_peers_completed_block(block_name VARCHAR, INOUT ref REFCURSOR)
AS
$$
DECLARE
    last_task_name VARCHAR;
BEGIN
    -- Находим последнее задание в блоке
    SELECT get_last_task_in_block(block_name) INTO last_task_name;

    -- Выводим имена пиров, которые успешно выполнили это задание
    OPEN ref FOR
        SELECT c.Peer AS peer_name,
               c.Date AS completion_date
        FROM Checks c
        WHERE c.Task = last_task_name
          AND c.id IN (
            SELECT p2p."Check"
            FROM P2P
            WHERE p2p.State = 'Success'
        )
          AND NOT EXISTS (
            SELECT 1
            FROM Verter v
            WHERE v."Check" = c.ID
              AND v.State = 'Failure'
        )
        ORDER BY c.Date DESC, c.Peer; -- Сортировка сначала по Date, затем по Peer

END;
$$ LANGUAGE plpgsql;


-- Usage:
BEGIN;
CALL find_peers_completed_block('C', 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;


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

SELECT get_last_task_in_block('C'); -- Возвращает название последней задачи в блоке 'C'


-- 8 Определить, к какому пиру стоит идти на проверку каждому обучающемуся
CREATE OR REPLACE PROCEDURE find_most_recommended_peers(INOUT ref REFCURSOR)
AS
$$
DECLARE
    popular_peer VARCHAR;
    second_popular_peer VARCHAR;
BEGIN
    SELECT RecommendedPeer
    INTO popular_peer
    FROM (
             SELECT RecommendedPeer, COUNT(*) AS recommendation_count
             FROM Recommendations
             GROUP BY RecommendedPeer
             ORDER BY COUNT(*) DESC
             LIMIT 2
         ) AS subquery
    ORDER BY recommendation_count DESC
    LIMIT 1;

    SELECT RecommendedPeer
    INTO second_popular_peer
    FROM (
             SELECT RecommendedPeer, COUNT(*) AS recommendation_count
             FROM Recommendations
             GROUP BY RecommendedPeer
             ORDER BY COUNT(*) DESC
             LIMIT 2
         ) AS subquery
    ORDER BY recommendation_count DESC
    OFFSET 1
        LIMIT 1;

    OPEN ref FOR
        SELECT
            p.nickname AS Peer,
            CASE
                WHEN p.nickname = popular_peer THEN second_popular_peer
                ELSE popular_peer
                END AS RecommendedPeer
        FROM
            Peers p;
END;
$$ LANGUAGE plpgsql;



BEGIN;
CALL find_most_recommended_peers( 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;


-- 9  Определить процент пиров, которые:
-- - Приступили только к блоку 1
-- - Приступили только к блоку 2
-- - Приступили к обоим
-- - Не приступили ни к одному
CREATE OR REPLACE PROCEDURE calculate_block_participation(
    IN block1_name VARCHAR,
    IN block2_name VARCHAR,
    INOUT ref REFCURSOR
)
AS
$$
DECLARE
    block1_started DECIMAL;
    block2_started DECIMAL;
    both_blocks_started DECIMAL;
    neither_block_started DECIMAL;
BEGIN
    -- Рассчитываем процент пиров, приступивших только к блоку 1
    SELECT (
                   (COUNT(DISTINCT c.Peer) * 100.0) / (SELECT COUNT(DISTINCT Peer) FROM Checks)
               )
    INTO block1_started
    FROM Checks c
    WHERE c.Task LIKE block1_name || '%';

    -- Рассчитываем процент пиров, приступивших только к блоку 2
    SELECT (
                   (COUNT(DISTINCT c.Peer) * 100.0) / (SELECT COUNT(DISTINCT Peer) FROM Checks)
               )
    INTO block2_started
    FROM Checks c
    WHERE c.Task LIKE block2_name || '%';

    -- Рассчитываем процент пиров, приступивших к обоим блокам
    SELECT (
                   (COUNT(DISTINCT c.Peer) * 100.0) / (SELECT COUNT(DISTINCT Peer) FROM Checks)
               )
    INTO both_blocks_started
    FROM Checks c
    WHERE c.Task LIKE block1_name || '%' AND c.Peer IN (
        SELECT DISTINCT Peer
        FROM Checks
        WHERE Task LIKE block2_name || '%'
    );

    -- Рассчитываем процент пиров, не приступивших ни к одному блоку
    SELECT (
                   (COUNT(DISTINCT p.Nickname) * 100.0 - COUNT(DISTINCT c.Peer) * 100.0) / (SELECT COUNT(DISTINCT Peer) FROM Checks)
               )
    INTO neither_block_started
    FROM Peers p
             LEFT JOIN Checks c ON p.Nickname = c.Peer
    WHERE c.Peer IS NULL;

    -- Открываем курсор для вывода результата
    OPEN ref FOR
        SELECT block1_started, block2_started, both_blocks_started, neither_block_started;

END;
$$ LANGUAGE plpgsql;

-- Usage:
BEGIN;
CALL calculate_block_participation( 'C', 'D','ref');
FETCH ALL FROM ref;
CLOSE ref;
END;

-- 10 Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

CREATE OR REPLACE PROCEDURE calculate_birthday_check_stats(
    INOUT ref REFCURSOR
)
AS
$$
DECLARE
    total_people INT;
    total_successful_checks INT;
BEGIN
    -- Получаем сумму людей и сумму успешных проверок в День Рождения
    SELECT
        COUNT(DISTINCT p.Nickname),
        SUM(CASE WHEN p2p.State = 'Success' AND (verter.State IS NULL OR verter.State = 'Success') THEN 1 ELSE 0 END)
    INTO
        total_people,
        total_successful_checks
    FROM Peers p
             JOIN Checks c ON p.Nickname = c.Peer
             LEFT JOIN P2P p2p ON c.ID = p2p."Check"
             LEFT JOIN Verter verter ON c.ID = verter."Check"
    WHERE EXTRACT(MONTH FROM c.Date) = EXTRACT(MONTH FROM p.Birthday)
      AND EXTRACT(DAY FROM c.Date) = EXTRACT(DAY FROM p.Birthday);

    -- Открываем курсор для вывода результатов
    OPEN ref FOR
        SELECT
            ROUND((total_successful_checks * 100.0) / total_people) AS SuccessfulChecks,
            100 - ROUND((total_successful_checks * 100.0) / total_people) AS UnsuccessfulChecks;

END;
$$ LANGUAGE plpgsql;


-- Usage:
BEGIN;
CALL calculate_birthday_check_stats( 'ref');
FETCH ALL FROM ref;
CLOSE ref;
END;




-- 11 пределить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE PROCEDURE find_peers_completed_tasks(
    IN task1_name VARCHAR(255),
    IN task2_name VARCHAR(255),
    IN task3_name VARCHAR(255),
    INOUT ref REFCURSOR
)
AS
$$
BEGIN
    -- Создаем временные таблицы для каждого задания
    CREATE TEMP TABLE IF NOT EXISTS successful_task1_completed (
        Peer VARCHAR(50)
    );
    CREATE TEMP TABLE IF NOT EXISTS successful_task2_completed (
        Peer VARCHAR(50)
    );
    CREATE TEMP TABLE IF NOT EXISTS successful_task3_completed (
        Peer VARCHAR(50)
    );

    -- Очищаем временные таблицы перед вставкой новых данных
    TRUNCATE TABLE successful_task1_completed;
    TRUNCATE TABLE successful_task2_completed;
    TRUNCATE TABLE successful_task3_completed;

    -- Получаем список пиров, успешно выполнивших первое задание
    INSERT INTO successful_task1_completed (Peer)
    SELECT DISTINCT c.Peer
    FROM Checks c
             JOIN Tasks t ON c.Task = t.Title
             LEFT JOIN P2P p2p ON c.ID = p2p."Check" AND p2p.State = 'Success'
             LEFT JOIN Verter verter ON c.ID = verter."Check"
    WHERE t.Title = task1_name
      AND (p2p.State = 'Success' AND (verter.State IS NULL OR verter.State = 'Success'));

    -- Получаем список пиров, успешно выполнивших второе задание
    INSERT INTO successful_task2_completed (Peer)
    SELECT DISTINCT c.Peer
    FROM Checks c
             JOIN Tasks t ON c.Task = t.Title
             LEFT JOIN P2P p2p ON c.ID = p2p."Check" AND p2p.State = 'Success'
             LEFT JOIN Verter verter ON c.ID = verter."Check"
    WHERE t.Title = task2_name
      AND (p2p.State = 'Success' AND (verter.State IS NULL OR verter.State = 'Success'));

    -- Получаем список пиров, успешно выполнивших третье задание
    INSERT INTO successful_task3_completed (Peer)
    SELECT DISTINCT c.Peer
    FROM Checks c
             JOIN Tasks t ON c.Task = t.Title
             LEFT JOIN P2P p2p ON c.ID = p2p."Check" AND p2p.State = 'Success'
             LEFT JOIN Verter verter ON c.ID = verter."Check"
    WHERE t.Title = task3_name
      AND (p2p.State = 'Success' AND (verter.State IS NULL OR verter.State = 'Success'));

    -- Получаем итоговый список пиров без пустых строк
    OPEN ref FOR
        SELECT
            successful_task1_completed.Peer AS "Список пиров"
        FROM successful_task1_completed
                 FULL OUTER JOIN successful_task2_completed
                                 ON successful_task1_completed.Peer = successful_task2_completed.Peer
                 FULL OUTER JOIN successful_task3_completed
                                 ON successful_task1_completed.Peer = successful_task3_completed.Peer
        WHERE successful_task1_completed.Peer IS NOT NULL
          AND successful_task2_completed.Peer IS NOT NULL
          AND successful_task3_completed.Peer IS NULL;
END;
$$ LANGUAGE plpgsql;


-- Usage:
BEGIN;
CALL find_peers_completed_tasks( 'C2_SimpleBashUtils', 'C3_S21_String+', 'C4_S21_Math', 'ref' );
FETCH ALL FROM ref;
CLOSE ref;
END;




-- 11.  Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE PROCEDURE find_peers_completed_tasks(
    task1 VARCHAR,
    task2 VARCHAR,
    task3 VARCHAR,
    INOUT ref REFCURSOR
)
AS
$$
BEGIN
    OPEN ref FOR
WITH task_filtered AS (
    SELECT c.id,
           c.peer,
           c.task,
           p.state AS p_state,
           v.state AS v_state
    FROM Checks c
             LEFT JOIN P2P p ON c.ID = p."Check"
             LEFT JOIN Verter v ON c.ID = v."Check"
    WHERE c.task IN (task1, task2, task3)
      AND p.state <> 'Start'
)

SELECT DISTINCT p.nickname
FROM peers p
WHERE EXISTS (
    SELECT 1
    FROM task_filtered tf1
    WHERE tf1.peer = p.nickname
      AND tf1.task = task1
      AND (tf1.p_state = 'Success' AND (tf1.v_state IS NULL OR tf1.v_state = 'Success'))
)
  AND EXISTS (
    SELECT 1
    FROM task_filtered tf2
    WHERE tf2.peer = p.nickname
      AND tf2.task = task2
      AND (tf2.p_state = 'Success' AND (tf2.v_state IS NULL OR tf2.v_state = 'Success'))
)
  AND NOT EXISTS (
    SELECT 1
    FROM task_filtered tf3
    WHERE tf3.peer = p.nickname
      AND tf3.task = task3
      AND (tf3.p_state = 'Failure' OR tf3.v_state = 'Failure')
);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL find_peers_completed_tasks('C2_SimpleBashUtils', 'D01_Linux', 'D02_LinuxNetwork', 'ref');
FETCH ALL FROM ref;
CLOSE ref;
COMMIT;
END;

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
        SELECT substring(t.title from '^[^_]+') AS Task, COUNT(th.ParentTask) AS PrevCount
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
COMMIT;
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
COMMIT;
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
COMMIT;
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
COMMIT;
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
CLOSE ref;
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
COMMIT;
END;
