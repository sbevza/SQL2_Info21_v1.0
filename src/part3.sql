-- 1. Вывод TransferredPoints в читаемом виде
CREATE OR REPLACE FUNCTION show_transferred_points()
    RETURNS TABLE (
                      Peer1 VARCHAR,
                      Peer2 VARCHAR,
                      PointsAmount BIGINT
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT
            CASE
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
                ) AS PointsAmount
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
SELECT * FROM show_transferred_points();

-- 2. Получение выполненных заданий и XP
CREATE OR REPLACE FUNCTION show_user_tasks_xp()
    RETURNS TABLE (
                      Peer VARCHAR,
                      Task VARCHAR,
                      XP INTEGER
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT DISTINCT
            c.Peer,
            t.Title AS Task,
            xp.XPAmount AS XP
        FROM P2P p
                 JOIN Checks c ON p."Check" = c.ID
                 JOIN Tasks t ON c.Task = t.Title
                 JOIN XP xp ON c.ID = xp."Check"
                 LEFT JOIN Verter v ON c.ID = v."Check"
        WHERE p.State = 'Success'
          AND (v.State IS NULL OR v.State != 'Failure')
    );

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT * FROM show_user_tasks_xp();

-- 3. Поиск пиров, не покидавших кампус
CREATE OR REPLACE FUNCTION find_peers_inside_campus(day DATE)
    RETURNS TABLE (
        Peer VARCHAR
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT
            DISTINCT t.Peer
        FROM TimeTracking t
        WHERE t.Date = day
          AND t.State = 1
          AND NOT EXISTS (
            SELECT 1
            FROM TimeTracking t2
            WHERE t2.Peer = t.Peer
              AND t2.Date = day
              AND t2.State = 2
        )
    );

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage: 
SELECT * FROM find_peers_inside_campus('2023-01-15');

-- 4. Расчет изменения пир-поинтов
CREATE OR REPLACE FUNCTION calculate_peer_points_change()
    RETURNS TABLE (
                      Peer VARCHAR,
                      PointsChange BIGINT
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT
            Subquery.Peer,
            CAST(SUM(Subquery.PointsChange) AS BIGINT) AS PointsChange
        FROM (
                 SELECT
                     tp.CheckingPeer AS Peer,
                     SUM(tp.PointsAmount) AS PointsChange
                 FROM TransferredPoints tp
                 GROUP BY tp.CheckingPeer
                 UNION ALL
                 SELECT
                     tp.CheckedPeer AS Peer,
                     -SUM(tp.PointsAmount) AS PointsChange
                 FROM TransferredPoints tp
                 GROUP BY tp.CheckedPeer
             ) AS Subquery
        GROUP BY Subquery.Peer
    )
        ORDER BY PointsChange DESC;

    RETURN;
END;
$$ LANGUAGE plpgsql;


-- Usage:
SELECT * FROM calculate_peer_points_change();

-- 5. Самые часто проверяемые задания
CREATE OR REPLACE FUNCTION calculate_peer_points_change_from_first_function()
    RETURNS TABLE (
                      Peer VARCHAR,
                      PointsChange BIGINT
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT
            Subquery.Peer,
            CAST(SUM(Subquery.PointsChange) AS BIGINT) AS PointsChange
        FROM (
                 SELECT
                     tp.Peer1 AS Peer,
                     SUM(tp.PointsAmount) AS PointsChange
                 FROM show_transferred_points() tp
                 GROUP BY tp.Peer1
                 UNION ALL
                 SELECT
                     tp.Peer2 AS Peer,
                     -SUM(tp.PointsAmount) AS PointsChange
                 FROM show_transferred_points() tp
                 GROUP BY tp.Peer2
             ) AS Subquery
        GROUP BY Subquery.Peer
        ORDER BY PointsChange DESC
    );

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT * FROM calculate_peer_points_change_from_first_function();


-- 6. Поиск пиров, выполнивших весь блок
CREATE OR REPLACE FUNCTION mostFrequentTasksPerDay()
    RETURNS TABLE (
                      Day DATE,
                      PopularTask VARCHAR
                  ) AS $$
BEGIN
    RETURN QUERY (
        WITH TaskRanks AS (
            SELECT
                DATE(c.Date) AS "Day",
                t.Title AS "Task",
                COUNT(*) AS TaskCount,
                RANK() OVER (PARTITION BY DATE(c.Date) ORDER BY COUNT(*) DESC) AS TaskRank
            FROM Checks c
                     JOIN Tasks t ON c.Task = t.Title
            GROUP BY "Day", "Task"
        )
        SELECT "Day", "Task"
        FROM TaskRanks
        WHERE TaskRank = 1
    );
END;
$$ LANGUAGE plpgsql;

-- Usage:
SELECT * FROM mostFrequentTasksPerDay();


-- 7. Нахождение рекомендуемых пиров
CREATE OR REPLACE FUNCTION peersCompletedBlock(blockName VARCHAR)
    RETURNS TABLE (
                      PeerName VARCHAR,
                      CompletionDate DATE
                  ) AS $$
BEGIN
    RETURN QUERY (
        WITH BlockTasks AS (
            SELECT DISTINCT ON (c.Peer) c.Peer AS PeerName, MAX(c.Date) AS CompletionDate
            FROM Checks c
                     JOIN Tasks t ON c.Task = t.Title
            WHERE t.Title LIKE blockName || '%'
              AND c.ID IN (
                SELECT "Check"
                FROM P2P
                WHERE State = 'Success'
            )
              AND c.ID IN (
                SELECT "Check"
                FROM Verter
                WHERE State = 'Success' OR State IS NULL
            )
            GROUP BY PeerName
        )
        SELECT bt.PeerName, bt.CompletionDate
        FROM BlockTasks bt
    )
        ORDER BY CompletionDate;
END;
$$ LANGUAGE plpgsql;



-- Usage:
SELECT * FROM peersCompletedBlock('D');
-- Это найдет для каждого пира того пира, которого рекомендует
-- наибольшее число друзей текущего пира.

-- 8. Расчет процента пиров, приступивших к блокам
CREATE PROCEDURE get_started_block_percents(
    block1 VARCHAR,
    block2 VARCHAR
)
    LANGUAGE plpgsql
AS $$
DECLARE
    total INTEGER;
    block1_only INTEGER;
    block2_only INTEGER;
    both INTEGER;
BEGIN
    SELECT COUNT(*) INTO total FROM peers;

    SELECT COUNT(DISTINCT c.peer)
    INTO block1_only
    FROM checks c
    WHERE c.task LIKE block1 || '%'
      AND c.peer NOT IN (
        SELECT peer
        FROM checks
        WHERE task LIKE block2 || '%'
    );

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
AS $$
DECLARE
    total INTEGER;
    successful INTEGER;
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
      AND c.id IN (
        SELECT check
    FROM p2p
    WHERE state = 'Success'
        );

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
) RETURNS TABLE (
    peer VARCHAR
                ) AS $$
SELECT p.nickname AS peer
FROM peers p
WHERE p.nickname IN (
    SELECT peer
    FROM checks
    WHERE task IN (task1, task2)
      AND id IN (
        SELECT check
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
CREATE OR REPLACE FUNCTION calculate_early_entry_percentage()
    RETURNS TABLE (
                      Month VARCHAR,
                      EarlyEntries INT,
                      EarlyEntryPercentage DECIMAL(5, 2)
                  ) AS $$
BEGIN
    RETURN QUERY
        SELECT
            TO_CHAR(Birthday, 'Month') AS Month,
            SUM(CASE WHEN EXTRACT(HOUR FROM Time) < 12 THEN 1 ELSE 0 END) AS EarlyEntries,
            (SUM(CASE WHEN EXTRACT(HOUR FROM Time) < 12 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS EarlyEntryPercentage
        FROM Peers
                 JOIN TimeTracking ON Peers.Nickname = TimeTracking.Peer
        GROUP BY Month
        ORDER BY Month;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM calculate_early_entry_percentage();





SELECT * FROM calculate_early_entry_percentage();


