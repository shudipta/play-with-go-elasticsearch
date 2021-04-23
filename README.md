# play-with-go-elasticsearch

Time pass with official go client of elasticsearch

## Contains

- default crud
  - create
  - update
  - index
  - search
  - delete
- bulk
  - bulk-indexer
- makefile
- docker-compose

## Run

```shell
# install dependency(ies)
$ make dep

$ make clean

$ make run-default
$ ACTIONS_ARGS=create,search,delete \
       make run-default

$ make run-bulk
$ BULK_ARGS="--del-only=true" make run-bulk
$ BULK_ARGS="--cr-only=true" make run-bulk
```

> This is really stupid going through this repo.
