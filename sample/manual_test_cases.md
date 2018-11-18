## No table, no header

### Shold fail
```
DROP TABLE IF EXISTS "no_header"
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --input-file sample/no-header.csv
```

### Ok
```
DROP TABLE IF EXISTS "no_header"
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode create --input-file sample/no-header.csv
```
```
csv2db-test=# select * from no_header;
 col1 | col2 | col3 |    col4    | col0
------+------+------+------------+------
 2    | qqq  | {}   | 2017-01-01 | 1
 4    |      |      |            |
(2 rows)

```

## No table, with header

### Shold fail
```
DROP TABLE IF EXISTS "no_header"
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --input-file sample/header.csv --has-header
```

### Ok
```
DROP TABLE IF EXISTS "no_header"
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode create --input-file sample/header.csv --has-header
```
```
csv2db-test=# select * from no_header;
 d  |     e      | a | b |  c
----+------------+---+---+-----
 {} | 2017-01-01 | 1 | 2 | qqq
    |            |   | 4 |
(2 rows)
```

## Typed table exists, no header (insert fields by native column order)

### Fail on not null
```
CREATE TABLE public.no_header
(
  a bigint NOT NULL,
  b integer NOT NULL,
  c character varying,
  d jsonb,
  e date
)
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode as-is --input-file sample/no-header.csv
```

### Append
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer NOT NULL,
  c character varying,
  d jsonb,
  e date
)
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode as-is --input-file sample/no-header.csv
```
```
csv2db-test=# select * from no_header;
 a | b |  c  | d  |     e
---+---+-----+----+------------
 1 | 2 | qqq | {} | 2017-01-01
   | 4 |     |    |
(2 rows)
```

### Re-create
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer NOT NULL,
  c character varying,
  d jsonb,
  e date
)
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode drop-anc-create --input-file sample/no-header.csv
```
```
 col0 | col1 | col2 | col3 |    col4
------+------+------+------+------------
 1    | 2    | qqq  | {}   | 2017-01-01
      | 4    |      |      |
(2 rows)

```

### Truncate
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer NOT NULL,
  c character varying,
  d jsonb,
  e date
)
```
Run twice or more. Repeat the same with ``--table-mode delete-all``
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode truncate --input-file sample/no-header.csv
```
```
csv2db-test=# select * from no_header;
 a | b |  c  | d  |     e
---+---+-----+----+------------
 1 | 2 | qqq | {} | 2017-01-01
   | 4 |     |    |
(2 rows)
```

### Table has more fields then CSV
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer NOT NULL,
  c character varying,
  d jsonb,
  e date,
  f text
)
```
Run twice or more. Repeat the same with ``--table-mode delete-all``
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode truncate --input-file sample/no-header.csv
```
```
csv2db-test=# select * from no_header;
 a | b |  c  | d  |     e      | f
---+---+-----+----+------------+---
 1 | 2 | qqq | {} | 2017-01-01 |
   | 4 |     |    |            |
(2 rows)
```

### Table has less fields then CSV
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer NOT NULL,
  c character varying,
  d jsonb
)
```
Run twice or more. Repeat the same with ``--table-mode delete-all``
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode truncate --input-file sample/no-header.csv
```
```
csv2db-test=# select * from no_header;
 a | b |  c  | d
---+---+-----+----
 1 | 2 | qqq | {}
   | 4 |     |
(2 rows)
```

## Both table and CSV header present
```
CREATE TABLE public.no_header
(
  a bigint,
  b integer,
  c character varying,
  d jsonb
)
```
```
./csv2db --db postgres --conn 'postgres://csv2db:csv2db@localhost:5432/csv2db-test?sslmode=require' \
    --table no_header --table-mode truncate --input-file sample/header-short.csv --has-header
```
```
csv2db-test=# select * from no_header;
 a | b |  c  | d
---+---+-----+----
 1 |   | qqq | {}
   |   |     |
(2 rows)
```
