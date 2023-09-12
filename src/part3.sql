CREATE OR REPLACE FUNCTION fnc_tranferred_points_human_view()
    RETURNS TABLE
            (
                Peer1          VARCHAR,
                Peer2          VARCHAR,
                "PointsAmount" INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT CheckingPeer      AS Peer1,
               CheckedPeer       AS Peer2,
               SUM(PointsAmount) AS Sum
        FROM TransferredPoints
        GROUP BY CheckingPeer, CheckedPeer;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_tranferred_points_human_view();

SELECT tp1.CheckingPeer AS Peer1,
       tp1.CheckedPeer  AS Peer2,
       CASE
           WHEN tp2.ID IS NOT NULL THEN (tp1.PointsAmount - tp2.PointsAmount)
           ELSE tp1.PointsAmount
           END          AS PointsAmount
FROM TransferredPoints tp1
         LEFT JOIN TransferredPoints tp2
                   ON tp1.checkingpeer = tp2.checkedpeer
                       AND tp1.checkedpeer = tp2.checkingpeer
                       AND tp1.ID != tp2.ID;



