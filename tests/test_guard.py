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


def test_cte_prefixed_write_is_blocked():
    # A CTE can prefix a write; the WITH leader must not whitelist it.
    for sql in ["WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
                "with s as (select * from a) delete from t where id in (select * from s)",
                "WITH s AS (SELECT 1) MERGE INTO t USING s ON t.id=s.id"]:
        assert not is_read_only(sql), sql


def test_leading_comment_does_not_block_a_read():
    for sql in ["-- a note\nSELECT 1",
                "/* block */ SELECT 1",
                "-- one\n-- two\nselect * from t"]:
        assert is_read_only(sql), sql
