CREATE OR REPLACE PROCEDURE add_p2p(
    IN checked_peer VARCHAR,
    IN checking_peer VARCHAR,
    IN task_local VARCHAR,
    IN state_local check_status,
    IN time_local TIME
)
    LANGUAGE plpgsql
AS
$$
DECLARE
    check_id INTEGER;
BEGIN
    SELECT MAX(p."Check")
    INTO check_id
    FROM P2P p
             JOIN Checks c ON p."Check" = c.ID
    WHERE p.CheckingPeer = checking_peer
      AND p.State = 'Start'
      AND c.Task = task_local;

    IF state_local = 'Start' THEN
        INSERT INTO Checks (Peer, Task, Date)
        VALUES (checked_peer, task_local, CURRENT_DATE);

        SELECT MAX(ID) INTO check_id FROM Checks;

        INSERT INTO P2P("Check", CheckingPeer, State, Time)
        VALUES (check_id, checking_peer, state_local, time_local);
    ELSE
        INSERT INTO P2P("Check", CheckingPeer, State, Time)
        VALUES (check_id, checking_peer, state_local, time_local);
    END IF;
END;
$$;

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
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION fun_trg_p2p_add_prp()
    RETURNS TRIGGER AS
$$
DECLARE
    checked_peer VARCHAR;
BEGIN
    SELECT c.Peer
    INTO checked_peer
    FROM Checks c
    WHERE c.ID = NEW."Check";

    IF NEW."State" = 'Start' THEN
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
EXECUTE FUNCTION fun_trg_p2p_add_prp();

CREATE OR REPLACE FUNCTION fun_trg_xp_check_row()
    RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT t.MaxXP
        FROM Checks ch
                 LEFT JOIN Tasks t ON ch.Task = t.Title
        WHERE ch.ID = NEW."Check") <= NEW.XPAmount
    THEN
        RAISE NOTICE 'XPAmount exceeds the maximum allowed for this check';
        RETURN NULL;
    END IF;

    IF (SELECT COUNT(*)
        FROM Checks ch
                 LEFT JOIN Verter v ON v."Check" = ch.ID
                 LEFT JOIN P2P p ON p."Check" = ch.ID
        WHERE (v.State = 'Success' OR v.State IS NULL) AND p.State = 'Success'
       ) = 0
    THEN
        RAISE NOTICE 'No successful checks';
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_xp_check_row
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE FUNCTION fun_trg_xp_check_row();
