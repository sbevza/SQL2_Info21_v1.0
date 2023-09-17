-----------------ex01-----------------
CREATE OR REPLACE PROCEDURE add_p2p(
    IN checked_peer VARCHAR,
    IN checking_peer VARCHAR,
    IN task_name VARCHAR,
    IN state check_status,
    IN check_time TIME
)
    LANGUAGE plpgsql
AS $$
DECLARE
    check_id BIGINT;
BEGIN
    IF state = 'Start' THEN
        IF NOT EXISTS (SELECT 1 FROM tasks WHERE title = task_name) THEN
            RAISE EXCEPTION 'Task % not found', task_name;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM peers WHERE nickname = checked_peer) THEN
            RAISE EXCEPTION 'Checked peer % not found', checked_peer;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM peers WHERE nickname = checking_peer) THEN
            RAISE EXCEPTION 'Checking peer % not found', checking_peer;
        END IF;

        INSERT INTO checks (peer, task, date)
        VALUES (checked_peer, task_name, CURRENT_DATE)
        RETURNING id INTO check_id;

        INSERT INTO p2p("Check", CheckingPeer, State, Time)
        VALUES (check_id, checking_peer, state, check_time);

    ELSIF state IN ('Success', 'Failure') THEN

        SELECT c.id INTO check_id
        FROM checks c
                 JOIN p2p p ON p."Check" = c.id
        WHERE p.state = 'Start'
          AND c.task = task_name
          AND p.checkingPeer = checking_peer;

        IF check_id IS NULL THEN
            RAISE EXCEPTION 'No started check found for %, %, %',
                checked_peer, checking_peer, task_name;
        END IF;

        INSERT INTO p2p("Check", CheckingPeer, State, Time)
        VALUES (check_id, checking_peer, state, check_time);

    ELSE
        RAISE EXCEPTION 'Invalid state: %', state;
    END IF;

END;
$$;
-- Пример использования
CALL add_p2p('john', 'alice', 'D01_Linux', 'Start', '10:00');
CALL add_p2p('john', 'alice', 'D01_Linux', 'Success', '11:00');
CALL add_p2p('john', 'alice', 'D01_Linux', 'Success', '11:00');
CALL add_p2p('john1', 'alice1', 'D01_Linux', 'Success', '11:00');

-----------------ex02-----------------
CREATE OR REPLACE PROCEDURE add_verter(
    IN checked_peer VARCHAR,
    IN task_local VARCHAR,
    IN state_local check_status,
    IN time_local TIME
)
    LANGUAGE PLPGSQL
AS
$$
DECLARE
    check_id INTEGER;
BEGIN
    SELECT MAX(p."Check")
    INTO check_id
    FROM P2P p
             JOIN Checks c ON p."Check" = c.ID
    WHERE p.State = 'Success'
      AND c.Task = task_local
      AND c.Peer = checked_peer;

    IF check_id IS NOT NULL THEN
        INSERT INTO Verter ("Check", State, Time)
        VALUES (check_id, state_local, time_local);
    ELSE
        RAISE EXCEPTION 'Invalid state of the check.';
    END IF;
END;
$$;

-- Пример использования
CALL add_verter('john', 'D01_Linux', 'Start', '12:00');
CALL add_verter('john', 'D01_Linux', 'Success', '13:00');
CALL add_verter('kate', 'C6_S21_Matrix', 'Start', '12:00');
CALL add_verter('john', 'C6_S21_Matrix', 'Start', '12:00');

-----------------ex03-----------------
CREATE OR REPLACE FUNCTION fnc_trg_p2p_add_prp()
    RETURNS TRIGGER AS
$$
DECLARE
    checked_peer VARCHAR;
BEGIN
    SELECT c.Peer
    INTO checked_peer
    FROM Checks c
    WHERE c.ID = NEW."Check";

    IF NEW."state" = 'Start' THEN
        IF NOT EXISTS (SELECT 1
                       FROM TransferredPoints tp
                       WHERE NEW.CheckingPeer = tp.CheckingPeer
                         AND checked_peer = tp.CheckedPeer)
        THEN
            INSERT INTO TransferredPoints(CheckingPeer, CheckedPeer)
            VALUES (NEW.CheckingPeer, checked_peer);
        ELSE
            UPDATE TransferredPoints
            SET PointsAmount = PointsAmount + 1
            WHERE NEW.CheckingPeer = TransferredPoints.CheckingPeer
              AND checked_peer = TransferredPoints.CheckedPeer;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_add_prp
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_p2p_add_prp();

-- Пример использования
INSERT INTO P2P ("Check", checkingpeer, state, time)
VALUES (7, 'john', 'Start', '11:00');
SELECT *
FROM transferredpoints
WHERE checkingpeer = 'john' AND checkedpeer = 'lisa';

-----------------ex04-----------------
CREATE OR REPLACE FUNCTION fnc_trg_xp_check_row()
    RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT t.MaxXP
        FROM Checks ch
                 LEFT JOIN Tasks t ON ch.Task = t.Title
        WHERE ch.ID = NEW."Check") <= NEW.XPAmount
    THEN
        RAISE EXCEPTION 'XPAmount exceeds the maximum allowed for this check';
    END IF;

    IF (SELECT COUNT(*)
        FROM Checks ch
                 LEFT JOIN Verter v ON v."Check" = ch.ID
                 LEFT JOIN P2P p ON p."Check" = ch.ID
        WHERE (v.State = 'Success' OR v.State IS NULL) AND p.State = 'Success'
       ) = 0
    THEN
        RAISE EXCEPTION 'No successful checks';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_xp_check_row
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_xp_check_row();

-- Пример использования
INSERT INTO XP ("Check", xpamount) VALUES (11, 1000); -- Error
INSERT INTO XP ("Check", xpamount) VALUES (11, 300);

-- SELECT * FROM xp
-- WHERE xpamount = 300;
