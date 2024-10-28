DROP STREAM IF EXISTS ibis_testing.diamonds;
CREATE STREAM ibis_testing.diamonds AS
SELECT *, now64(3, 'UTC') as _tp_time FROM file('ibis/diamonds.parquet', 'Parquet');

DROP STREAM IF EXISTS ibis_testing.batting;
CREATE STREAM ibis_testing.batting AS
SELECT *, now64(3, 'UTC') as _tp_time FROM file('ibis/batting.parquet', 'Parquet');

DROP STREAM IF EXISTS ibis_testing.awards_players;
CREATE STREAM ibis_testing.awards_players AS
SELECT *, now64(3, 'UTC') as _tp_time FROM file('ibis/awards_players.parquet', 'Parquet');

DROP STREAM IF EXISTS ibis_testing.functional_alltypes;
CREATE STREAM ibis_testing.functional_alltypes AS
SELECT *, now64(3, 'UTC') as _tp_time
FROM file('ibis/functional_alltypes.parquet', 'Parquet');

DROP STREAM IF EXISTS ibis_testing.astronauts;
CREATE STREAM ibis_testing.astronauts AS
SELECT *, now64(3, 'UTC') as _tp_time FROM file('ibis/astronauts.parquet', 'Parquet');

DROP STREAM IF EXISTS ibis_testing.tzone;
CREATE STREAM ibis_testing.tzone (
    ts nullable(datetime),
    key nullable(string),
    value nullable(float64)
) ENGINE = Memory;

DROP STREAM IF EXISTS ibis_testing.array_types;
CREATE STREAM ibis_testing.array_types (
    x array(nullable(int64)),
    y array(nullable(string)),
    z array(nullable(float64)),
    grouper nullable(string),
    scalar_column nullable(float64),
    multi_dim array(array(nullable(int64)))
) ENGINE = Memory;

INSERT INTO ibis_testing.array_types VALUES
    ([1, 2, 3], ['a', 'b', 'c'], [1.0, 2.0, 3.0], 'a', 1.0, [[], [1, 2, 3], []]),
    ([4, 5], ['d', 'e'], [4.0, 5.0], 'a', 2.0, []),
    ([6, NULL], ['f', NULL], [6.0, NULL], 'a', 3.0, [[], [], []]),
    ([NULL, 1, NULL], [NULL, 'a', NULL], [], 'b', 4.0, [[1], [2], [], [3, 4, 5]]),
    ([2, NULL, 3], ['b', NULL, 'c'], NULL, 'b', 5.0, []),
    ([4, NULL, NULL, 5], ['d', NULL, NULL, 'e'], [4.0, NULL, NULL, 5.0], 'c', 6.0, [[1, 2, 3]]);

DROP STREAM IF EXISTS ibis_testing.time_df1;
CREATE STREAM ibis_testing.time_df1 (
    time int64,
    value nullable(float64),
    key nullable(string)
) ENGINE = Memory;
INSERT INTO ibis_testing.time_df1 VALUES
    (1, 1.0, 'x'),
    (20, 20.0, 'x'),
    (30, 30.0, 'x'),
    (40, 40.0, 'x'),
    (50, 50.0, 'x');

DROP STREAM IF EXISTS ibis_testing.time_df2;
CREATE STREAM ibis_testing.time_df2 (
    time int64,
    value nullable(float64),
    key nullable(string)
) ENGINE = Memory;
INSERT INTO ibis_testing.time_df2 VALUES
    (19, 19.0, 'x'),
    (21, 21.0, 'x'),
    (39, 39.0, 'x'),
    (49, 49.0, 'x'),
    (1000, 1000.0, 'x');

DROP STREAM IF EXISTS ibis_testing.struct;
CREATE STREAM ibis_testing.struct (
    abc tuple(
        a nullable(float64),
        b nullable(string),
        c nullable(int64)
    )
) ENGINE = Memory;

INSERT INTO ibis_testing.struct VALUES
    ((1.0, 'banana', 2)),
    ((2.0, 'apple', 3)),
    ((3.0, 'orange', 4)),
    ((NULL, 'banana', 2)),
    ((2.0, NULL, 3)),
    ((NULL, NULL, NULL)),
    ((3.0, 'orange', NULL));

DROP STREAM IF EXISTS ibis_testing.map;
CREATE STREAM ibis_testing.map (kv map(string, nullable(int64))) ENGINE = Memory;

INSERT INTO ibis_testing.map VALUES
    ({'a': 1}), ({'b': 2}), ({'c': 3}),
    ({'d': 4}), ({'e': 5}), ({'c': 6});

DROP STREAM IF EXISTS ibis_testing.win;
CREATE STREAM ibis_testing.win (g nullable(string), x int64, y nullable(int64)) ENGINE = Memory;
INSERT INTO ibis_testing.win VALUES
    ('a', 0, 3),
    ('a', 1, 2),
    ('a', 2, 0),
    ('a', 3, 1),
    ('a', 4, 1);

DROP STREAM IF EXISTS ibis_testing.topk;
CREATE STREAM ibis_testing.topk (x nullable(int64)) ENGINE = Memory;
INSERT INTO ibis_testing.topk VALUES (1), (1), (NULL);
SELECT sleep(3);
