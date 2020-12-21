-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION referee" to load this file. \quit

CREATE TABLE IF NOT EXISTS referee.decision(
    key text PRIMARY KEY NOT NULL,
    node_id int,
    -- storing gen_num here guarantees we clear (delete) the grant which is
    -- indeed can already be cleared instead of accidently removing newer one
    gen_num bigint
);

-- returns nothing on success, bails out with ERROR on conflict
CREATE OR REPLACE FUNCTION referee.request_grant(applicant_id int, gen_num bigint) RETURNS void AS
$$
DECLARE
    winner_id int;
    winner_gen_num bigint;
BEGIN
    INSERT INTO referee.decision AS d VALUES ('winner', applicant_id, gen_num)
      ON CONFLICT (key) DO UPDATE SET
        node_id=EXCLUDED.node_id, gen_num=EXCLUDED.gen_num
      -- reissue grant iff it was previously given to this node, not another
      WHERE d.node_id = EXCLUDED.node_id AND
      -- this could be assert as well, node never repeats request with the same
      -- gen num
            d.gen_num < EXCLUDED.gen_num
      RETURNING applicant_id INTO winner_id;
    -- if insertion hasn't happened, there must have been conflict with existing
    -- grant
    IF winner_id IS NULL THEN
      SELECT d.node_id, d.gen_num INTO winner_id, winner_gen_num FROM referee.decision d;
      RAISE EXCEPTION 'grant was already issued to node % in generation %', winner_id, winner_gen_num;
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION referee.clean() RETURNS bool AS
$$
BEGIN
    delete from referee.decision where key = 'winner';
    return 'true';
END
$$ LANGUAGE plpgsql;
