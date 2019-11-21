# Elasticsearch old docs clean-up 

A python script to delete old documents from elasticsearch (2.4.x) indices. Deletes old data from large indices without date format in its name for which elasticsearch-curator doesn't work

Tested on large amount of documents (>100 Mio deleted) and Elasticsearch 2.4.6.

In comparison to standard [bulk_helpers](https://elasticsearch-py.readthedocs.io/en/2.4.0/helpers.html#bulk-helpers) uses `ThreadPoolExecutor` instead of `multiprocessing.dummy Pool`, as done [here](https://github.com/TeamHG-Memex/scrapy-cdr) which avoids unbounded memory consumption on big datasets.

## Prerequisites 

pip install --no-cache-dir better_exceptions "elasticsearch-dsl>=2.0.0,<3.0.0" ipython pyyaml

## Configuration

use `config-cleaner.yml` to configure the script:
  * log-level - logging of the main script
  * thread_count - number of threads to execute deletion (default is 4)
  * cluster - ES cluster to connect to 
  * index - index to delete documents from
  * field - custom timestamp field (use @timestamp as default)
  * years - last years to keep data

## Licence 
MIT 
