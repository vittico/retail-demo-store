# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Utility script for local development that indexes products in a local
# OpenSearch instance. Typically you would be running OpenSearch
# in a local Docker container for development. See the docker-compose.yml
# file for details.

# When deploying to AWS, products are either indexed by a Lambda function
# (custom resource) or using the Search workshop notebook.

import os
import sys
import requests
import yaml
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

INDEX_NAME = 'products'

# Defaults assume you're running OpenSearch locally on port 9200
search_domain_scheme = os.environ.get('OPENSEARCH_DOMAIN_SCHEME', 'http')
search_domain_host = os.environ.get('OPENSEARCH_DOMAIN_HOST', 'localhost')
search_domain_port = os.environ.get('OPENSEARCH_DOMAIN_PORT', 9200)

logger.info(f'OpenSearch scheme: {search_domain_scheme}')
logger.info(f'OpenSearch endpoint: {search_domain_host}')
logger.info(f'OpenSearch port: {str(search_domain_port)}')

url = f'{search_domain_scheme}://{search_domain_host}:{search_domain_port}/{INDEX_NAME}'


headers = { "Content-Type": "application/json" }

r = requests.get(url, headers = headers)

# If index exists, delete it so we freshly index products.
if r.ok:
    logger.info(f'Deleting index {INDEX_NAME}')
    requests.delete(url)
    r = requests.get(url, headers = headers)

if r.ok:
    logger.info('Index exists! Nothing to do.')
else:
    logger.info('Index does NOT exist!')

    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    logger.info(f"Creating '{INDEX_NAME}' index...")

    r = requests.put(url, headers = headers, json = request_body)
    logger.info('Indexing products...')
    products_indexed = 0
    with open('../products/src/products-service/data/products.yaml') as file:
        products_list = yaml.safe_load(file)

        for product in products_list:
            url = f"{search_domain_scheme}://{search_domain_host}:{search_domain_port}/{INDEX_NAME}/_doc/{product['id']}"

            r = requests.put(url, headers = headers, json = product)
            products_indexed += 1

    logger.info(f'{products_indexed} products successfully indexed!')
