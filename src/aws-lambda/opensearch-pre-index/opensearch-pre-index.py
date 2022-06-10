# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import yaml
import logging
import boto3
from crhelper import CfnResource
from opensearchpy import OpenSearch

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Initialise the helper, all inputs are optional, this example shows the defaults
helper = CfnResource()

s3 = boto3.resource('s3')

INDEX_NAME = 'products'
TYPE_NAME = 'product'
ID_FIELD = 'id'

MAX_BULK_BATCH_SIZE = 100

@helper.delete
@helper.update
def no_op(_, __):
    # no operation is needed, the opensearch domain is just deleted with the resources
    pass

@helper.create
def opensearch_create(event,_):
    # Conditionally creates and loads OpenSearch products index
    # If the products index already exists, this function does nothing.
    # Otherwise, this function will create the products index and add
    # all products from the bundled products.yaml file.

    search_domain_endpoint = event['ResourceProperties']['OpenSearchDomainEndpoint']
    logger.info('OpenSearch endpoint: ' + search_domain_endpoint)

    search_host = {
        'host' : search_domain_endpoint,
        'port' : 443,
        'scheme' : 'https',
    }

    # For testing: specify 'ForceIndex' to force existing index to be deleted and products indexed.
    force_index = event['ResourceProperties'].get('ForceIndex', 'no').lower() in [ 'true', 'yes', '1' ]

    search = OpenSearch(hosts = [search_host], timeout=30, max_retries=10, retry_on_timeout=True)

    create_index_and_bulk_load = True

    if search.indices.exists(INDEX_NAME):
        logger.info(f'{INDEX_NAME} already exists')
        create_index_and_bulk_load = False

        if force_index:
            logger.info(f'Deleting "{INDEX_NAME}"...')
            res = search.indices.delete(index = INDEX_NAME)
            logger.debug(" response: '%s'" % (res))
            create_index_and_bulk_load = True
    else:
        logger.info('Index does not exist')

    if create_index_and_bulk_load:
        request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }
        logger.info(f'Creating "{INDEX_NAME}" index...')
        res = search.indices.create(index = INDEX_NAME, body = request_body)
        logger.debug(" response: '%s'" % (res))

        logger.info('Downloading products.yaml...')
        s3.meta.client.download_file(event['ResourceProperties']['Bucket'], event['ResourceProperties']['File'], '/tmp/products.yaml')
        with open('/tmp/products.yaml') as file:
            logger.info('Loading products.yaml...')
            products_list = yaml.safe_load(file)

            logger.info(f'Bulk indexing {len(products_list)} products in batches...')
            bulk_data = []

            for product in products_list:
                bulk_data.append({
                    "index": {
                        "_index": INDEX_NAME,
                        "_type": TYPE_NAME,
                        "_id": product[ID_FIELD]
                    }
                })
                bulk_data.append(product)

                if len(bulk_data) >= MAX_BULK_BATCH_SIZE:
                    search.bulk(index = INDEX_NAME, body = bulk_data)
                    bulk_data = []

            if len(bulk_data) > 0:
                search.bulk(index = INDEX_NAME, body = bulk_data)

        logger.info('Products successfully indexed!')

        helper.Data['Output'] = 'OpenSearch product index populated'
    else:
        helper.Data['Output'] = 'OpenSearch product index already exists'

    return search_domain_endpoint

def lambda_handler(event, context):
    helper(event, context)