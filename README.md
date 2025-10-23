# Heurist Analyser

This repository contain a prototype to analyse the content of the LostMa Heurist DB : https://heurist.huma-num.fr/heurist/?db=jbcamps_gestes

Background: https://github.com/LostMa-ERC/heurist-api

## Before to start

Install the [Heurist-api](https://github.com/LostMa-ERC/heurist-api) with pip.

```shell
pip install heurist-api
```

Download your records in a database file.

```shell
heurist -d 'YOUR.DATABASE' -l 'YOUR.LOGIN' -p 'YOUR.PASSWORD' download -f 'FILE.DB'
```

Download the schema of the database in csv files

```shell
heurist -d 'YOUR.DATABASE' -l 'YOUR.LOGIN' -p 'YOUR.PASSWORD'  schema -t csv
```

## Goal

This prototype contains some function to check-up the content of the database. We aim to add this as new features
of the Heurist-API
