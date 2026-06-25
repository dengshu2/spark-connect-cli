from spark_connect_cli.session import is_read_only


def test_reads_allowed():
    for sql in ["SELECT 1", "  select * from t", "SHOW DATABASES",
                "DESCRIBE TABLE t", "WITH x AS (SELECT 1) SELECT * FROM x",
                "(SELECT 1)", "explain select 1"]:
        assert is_read_only(sql), sql


def test_writes_blocked():
    for sql in ["DROP TABLE t", "INSERT INTO t VALUES (1)", "DELETE FROM t",
                "UPDATE t SET x=1", "CREATE TABLE t (a int)", "TRUNCATE TABLE t",
                "ALTER TABLE t ADD COLUMN c int", "MERGE INTO t ..."]:
        assert not is_read_only(sql), sql
