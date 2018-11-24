# csv2db
Tool for csv to database uploading.
See more details in command help ``csv2db -h``

## Supported databases

* postgres
* mysql

## Installation

### From sources
```
go install github.com/and-hom/csv2db
```

### From Ubuntu PPA
See here https://launchpad.net/~and-hom/+archive/ubuntu/csv2db

## Performance testing
**sample** utility can be used to generate sample CSV file with random text. Parameters are:

`` ./sample [row count] [column count] [column width]``

```
pushd sample
    go build
    ./sample big-sample.csv 200000 10 32
popd
go build
./csv2db --url 'mysql://csv2db:csv2db@localhost/csv2db'  --table big-sample-tab \
    --input-file sample/big-sample.csv --has-header --table-mode create

```