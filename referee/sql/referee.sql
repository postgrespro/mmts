CREATE EXTENSION referee;

SELECT * FROM referee.decision;

SELECT referee.request_grant(1, 7);
-- node can get its grant reissued
SELECT referee.request_grant(1, 9);
-- but another can't get it while the previous is not cleared
SELECT referee.request_grant(2, 4);
SELECT referee.request_grant(2, 10);
SELECT * FROM referee.decision;

DELETE FROM referee.decision WHERE gen_num < 8 OR (node_id = 1 AND gen_num <= 9);
-- surely 2 node can acquire the grant after removal of the old one
SELECT referee.request_grant(2, 11);
SELECT * FROM referee.decision;
