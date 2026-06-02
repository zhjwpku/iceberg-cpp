-- RUN: %run_cli_sql_case -DSQL_FILE=%s -DEXPECTED_FILE=%S/../expected/insert_columns_and_nulls.out -DCASE_NAME=insert_columns_and_nulls -DWORK_DIR=%t.work -P %S/../run_cli_sql_case.cmake

CREATE TABLE people (
  id BIGINT NOT NULL,
  active BOOLEAN,
  name TEXT,
  rating DOUBLE
);

INSERT INTO people (id, name) VALUES
  (10, 'carol'),
  (11, NULL);

SELECT id, active, name, rating FROM people;
