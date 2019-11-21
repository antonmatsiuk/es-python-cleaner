#!/usr/bin/env python3

import yaml
import logging
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Range
import elasticsearch.helpers as es_helpers


def delete_actions(hits):
    """generates the delete actions to be distributed in bulk"""
    for h in hits:
        if h.meta.routing is not None:
            yield dict(
                _op_type="delete",
                _index=h.meta.index,
                _type=h.meta.doc_type,
                _id=h.meta.id,
                _routing=h.meta.routing,
            )
        else:
            yield dict(
                _op_type="delete",
                _index=h.meta.index,
                _type=h.meta.doc_type,
                _id=h.meta.id,
            )

#use instead of native to fix OOM exceptions
def parallel_bulk(client, actions, thread_count=4, chunk_size=500,
                  max_chunk_bytes=100 * 1024 * 1024,
                  expand_action_callback=es_helpers.expand_action,
                  **kwargs):
    """ es_helpers.parallel_bulk rewritten with imap_fixed_output_buffer
    instead of Pool.imap, which consumed unbounded memory if the generator
    outruns the upload (which usually happens).
    """
    actions = map(expand_action_callback, actions)
    for result in imap_fixed_output_buffer(
            lambda chunk: list(
                es_helpers._process_bulk_chunk(client, chunk, **kwargs)),
            es_helpers._chunk_actions(actions, chunk_size, max_chunk_bytes,
                                      client.transport.serializer),
            threads=thread_count,
    ):
        for item in result:
            yield item


def imap_fixed_output_buffer(fn, it, threads: int):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        max_futures = threads + 1
        for i, x in enumerate(it):
            while len(futures) >= max_futures:
                future, futures = futures[0], futures[1:]
                yield future.result()
            futures.append(executor.submit(fn, x))
        for future in futures:
            yield future.result()

def main():
    config_file = "config-cleaner.yml"
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                        datefmt='%Y-%m-%d, %H:%M:%S')
    with open(config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            logging.getLogger().setLevel(config['settings']['log-level'])
            thread_count = config['settings']['thread_count']
            cluster = config['settings']['cluster']
            index = config['settings']['index']
            field = config['settings']['field']
            years = config['settings']['years']
            logging.info('Loaded settings started')

        except yaml.YAMLError as exc:
            logging.error(f"Cannot load file: {config_file} - Error: {exc}")
            exit()
    logging.getLogger('elasticsearch').setLevel(logging.WARN)
    logging.info(f"connecting to cluster {cluster} index {index}")
    client = Elasticsearch(list(cluster.split(",")))
    s = Search(using=client, index=index)
    total = s.count()
    old_documents = s.filter(
        # adapt to months if needed
        # https://elasticsearch-dsl.readthedocs.io/en/2.2.0/search_dsl.html#queries
        Range(**{field: {"lt": f"now-{years}y"}})
    )

    matches = old_documents.count()
    if matches < 1:
        logging.warning(f"no documents older than {years} year(s) found ({total} total)")
        return
    items_deleted = 0
    items_failed = 0
    logging.info(f"{matches} of {total} documents older than {years} year(s), deleting...")

    for success, info in parallel_bulk(client, delete_actions(old_documents.scan()), thread_count=thread_count,
                                       raise_on_exception=False, raise_on_error=False):
        if not success:
            logging.warning(f"failed: {info}")
            items_failed += 1
        else:
            items_deleted += 1
            if items_deleted % 10000 == 0:
                logging.info(f"deleted documents: {items_deleted}")
                
    logging.info(f"deleted: {items_deleted} failed: {items_failed} documents from index: {index}")

if __name__ == "__main__":
    main()
