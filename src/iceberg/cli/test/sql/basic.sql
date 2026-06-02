-- RUN: %run_cli_sql_case -DSQL_FILE=%s -DEXPECTED_FILE=%S/../expected/basic.out -DCASE_NAME=basic -DWORK_DIR=%t.work -P %S/../run_cli_sql_case.cmake

CREATE TABLE t (
  id INT NOT NULL,
  name TEXT,
  score DOUBLE PRECISION
);

INSERT INTO t VALUES
  (1, 'alice', 10.5),
  (2, 'bob', 20.25);

EXPLAIN SELECT * FROM t;
SELECT id, name, score FROM t;
