-- 1. Вывод TransferredPoints в читаемом виде
CREATE OR REPLACE FUNCTION show_transferred_points()
    RETURNS TABLE (
                      Peer1 VARCHAR,
                      Peer2 VARCHAR,
                      PointsAmount INTEGER
                  ) AS $$
BEGIN
    RETURN QUERY (
        SELECT
            tp.checkingPeer AS Peer1,
            tp.checkedPeer AS Peer2,
            CASE
                WHEN tp.pointsAmount > 0 THEN tp.pointsAmount
                ELSE -tp.pointsAmount
                END AS PointsAmount
        FROM transferredPoints tp
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
        SELECT
            c.Peer,
            t.Title AS Task,
            xp.XPAmount AS XP
        FROM P2P p
                 JOIN Checks c ON p."Check" = c.ID
                 JOIN Tasks t ON c.Task = t.Title
                 JOIN XP xp ON c.ID = xp."Check"
        WHERE p.State = 'Success'
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
SELECT * FROM find_peers_inside_campus('2023-01-12');

-- 4. Расчет изменения пир-поинтов
CREATE OR REPLACE FUNCTION calculate_peer_points_change()
    RETURNS TABLE (
                      Peer VARCHAR,
                      PointsChange BIGINT -- Изменим тип данных на BIGINT
                  ) AS $$
BEGIN
    RETURN QUERY (
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
    );

    RETURN;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM calculate_peer_points_change();

-- 5. Самые часто проверяемые задания
CREATE FUNCTION get_most_checked_tasks(DATE)
    RETURNS TABLE (
                      date DATE,
                      task VARCHAR
                  ) AS $$
SELECT
    date,
    task
FROM (
         SELECT
             date,
             task,
             COUNT(*) AS checks
         FROM checks
         WHERE date = $1
         GROUP BY date, task
     ) t
WHERE checks = (SELECT MAX(checks) FROM t)
$$ LANGUAGE sql;

-- 6. Поиск пиров, выполнивших весь блок
CREATE PROCEDURE get_peers_completed_block(
    block VARCHAR
)
    LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
        SELECT
            c.peer,
            MAX(c.date) AS completed
        FROM checks c
        WHERE c.task LIKE block || '%'
        GROUP BY c.peer
        HAVING COUNT(DISTINCT c.task) = (
            SELECT COUNT(*)
            FROM tasks
            WHERE title LIKE block || '%'
        )
        ORDER BY completed;
END;
$$

-- 7. Нахождение рекомендуемых пиров
CREATE FUNCTION get_recommended_peers()
    RETURNS TABLE (
                      peer VARCHAR,
                      recommended_peer VARCHAR
                  ) AS $$
SELECT
    p.peer,
    rp.recommendedPeer AS recommended_peer
FROM peers p
         JOIN recommendations r
              ON p.nickname = r.peer
         JOIN (
    SELECT peer, recommendedPeer, COUNT(*) AS recs
    FROM recommendations
    GROUP BY peer, recommendedPeer
) rp
              ON r.recommendedPeer = rp.recommendedPeer
WHERE rp.recs = (
    SELECT MAX(recs)
    FROM recommendations recs
    WHERE recs.peer = p.nickname
)
ORDER BY p.peer;
$$ LANGUAGE sql;

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

-- и т.д.