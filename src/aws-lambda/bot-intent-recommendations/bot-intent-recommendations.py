# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import logging
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

users_service_base_url = os.environ.get('users_service_base_url')
recommendations_service_base_url = os.environ.get('recommendations_service_base_url')

def close(fulfillment_state, message):
    return {
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': {'contentType': 'PlainText', 'content': message},
        }
    }

def build_response_card(attachments):
    """
    Build a responseCard with one or more attachments.
    """
    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': attachments
    }

def build_response_card_attachment(title, subtitle, image_url, link_url, options = None):
    """
    Build a responseCard attachment with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    if options is not None:
        buttons = [options[i] for i in range(min(5, len(options)))]
    if len(title) > 80:
        title = f'{title[:77]}...'

    if len(subtitle) > 80:
        subtitle = f'{subtitle[:77]}...'

    return {
        'title': title,
        'subTitle': subtitle,
        'imageUrl': image_url,
        'attachmentLinkUrl': link_url,
        'buttons': buttons
    }

def lookup_user(identity_id):
    logger.debug(f'Looking up user for identityId {identity_id}')

    url = f'{users_service_base_url}/users/identityid/{identity_id}'
    response = requests.get(url)

    user = None

    if response.ok:
        user_check = response.json()
        if user_check.get('id') and len(user_check.get('id')) > 0:
            user = user_check
            logger.debug(f'Found user: {json.dumps(user, indent = 2)}')
        else:
            logger.warn(f'User not found for identityId {identity_id}')

    return user

def get_recommendations(user_id, max_items = 10):
    logger.debug(f'Looking up product recommendations for user {user_id}')

    url = f'{recommendations_service_base_url}/recommendations?userID={user_id}&fullyQualifyImageUrls=1&numResults={max_items}'
    response = requests.get(url)

    recommendations = None

    if response.ok:
        recommendations = response.json()
        logger.debug(recommendations)

    return recommendations

def recommend_products(intent_request):
    # What the chatbot sends as the "userId" is really the identity ID from the auth'd client session.
    identity_id = intent_request['userId']

    if store_user := lookup_user(identity_id):
        recommendations = get_recommendations(store_user['id'], 4)

        user_name = store_user['first_name']
        if not user_name:
            user_name = "there"

        if recommendations and len(recommendations) > 0:
            attachments = []

            for recommendation in recommendations:
                product = recommendation['product']
                attachments.append(build_response_card_attachment(product['name'], product['description'], product['image'], product['url']))

            response = {
                'dialogAction': {
                    'type': 'Close',
                    'fulfillmentState': 'Fulfilled',
                    'message': {
                        'contentType': 'PlainText',
                        'content': f'Hi {user_name}. Based on your shopping trends, I think you may be interested in the following products.',
                    },
                    'responseCard': build_response_card(attachments),
                }
            }

        else:
            response = close('Failed', 'Sorry, I was unable to find any products to recommend.')
    else:
        response = close('Failed', 'Before I can make personalized recommendations, I need to know more about you. Please sign in or create an account and try again.')

    return response

""" --- Intents --- """

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    logger.debug(
        f"dispatch userId={intent_request['userId']}, intentName={intent_request['currentIntent']['name']}"
    )


    intent_name = intent_request['currentIntent']['name']

    # Dispatch to bot's intent handlers
    if intent_name == 'RecommendProduct':
        return recommend_products(intent_request)

    raise Exception(f'Intent with name {intent_name} not supported')

""" --- Main handler --- """

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    logger.debug(os.environ)
    logger.debug(json.dumps(event, indent = 2))
    logger.debug(f"event.bot.name={event['bot']['name']}")

    if not users_service_base_url:
        raise ValueError("Missing required environment value for 'users_service_base_url'")

    if not recommendations_service_base_url:
        raise ValueError("Missing required environment value for 'recommendations_service_base_url'")

    return dispatch(event)