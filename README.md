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

## Authorization
### By url
Use url with login and password like this: mysql://csv2db:csv2db@localhost/csv2db

### By environment variables
Set environment variables ``DB_USERNAME`` and ``DB_PASSWORD``. If db url does not
have username, username from environment will be used. If db url does not contain
password, password from environment will be used.

### By command line prompt
If **csv2db** can not define password previous ways, it will ask for them.