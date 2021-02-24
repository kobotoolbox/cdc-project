# coding: utf-8
import math
import json
from datetime import date, datetime, timedelta

import requests

# Date when this script runs for the first time. Useful for next runs to narrow
# down `related_items`.
FIRST_RUN = date(2020, 3, 8)
# Number of days between each run. Has to match cron job.
NUMBER_OF_DAYS_BETWEEN_RUNS = 7
# Server domain with protocol. e.g. https://kf.kobotoolbox.org
SERVER = ''
# User's token. Can be retrieved at https://[kpi-url]/token
TOKEN = ''
# Number of days after a submitted related data that a submission can be deleted
RETENTION_DAYS = 7
# Asset's unique ID. e.g. https://[kpi-url]/api/v2/assets/{asset_uid}/
ASSET_UID = ''
# Question name used to search for matches with `RELATED_ASSET_UID_FOR_DELETION`
# Values must be found in `RELATED_ASSET_UID_FOR_DELETION` results to allow deletion
QUESTION_FOR_DELETION = ''
# Asset's unique ID. This asset is the one used to search for `RELATED_QUESTION_FOR_DELETION` values
RELATED_ASSET_UID_FOR_DELETION = ''
# Question name
RELATED_QUESTION_FOR_DELETION = ''
# Hook's unique ID. e.g. https://[kpi-url]/api/v2/assets/{asset_uid}/hooks/{hook_uid}
HOOK_UID = ''
# Number of submissions to retrieve/delete at once.
BATCH_SIZE = 500

submission_ids_to_delete = []
success_hook_logs_submission_ids = []
related_items = set()


def add_hook_logs_submission_ids(json_response):
    results = json_response.get('results')
    if results:
        for result in results:
            if result.get('status_code') == 200:
                success_hook_logs_submission_ids.append(result['instance_id'])

    return True


def add_related_items(json_response):
    results = json_response.get('results')
    if results:
        for result in results:
            if result.get(RELATED_QUESTION_FOR_DELETION):
                related_items.add(result.get(RELATED_QUESTION_FOR_DELETION))

    return True


def add_submission_ids(json_response):
    """
    Add submission ids to `submission_ids_to_delete` only if:
    - submission has been posted to external server successfully
    - submission has been submitted `RETENTION_DAYS` ago.
    - `QUESTION_FOR_DELETION` is present in `related_items` (already filtered by
    `get_related_items()`)
    """
    results = json_response.get('results')
    if results:
        for result in results:
            submission_time = datetime.strptime(result['_submission_time'],
                                                '%Y-%m-%dT%H:%M:%S')
            delta = datetime.now() - submission_time
            # Submissions are sorted by `_submission_time`.
            # No need to go further. Related asset's data cannot exist for this
            # submission and next ones.
            if delta.days < RETENTION_DAYS:
                return False

            if result['_id'] in success_hook_logs_submission_ids and \
                    result.get(QUESTION_FOR_DELETION) and \
                    result.get(QUESTION_FOR_DELETION) in related_items:
                submission_ids_to_delete.append(result['_id'])

    return True


def delete_submissions():
    url = f'{SERVER}/api/v2/assets/{ASSET_UID}/data/bulk/'
    iteration = math.ceil(len(submission_ids_to_delete) / BATCH_SIZE)
    print('Deleting {} submissions...'.format(len(submission_ids_to_delete)))
    for i in range(iteration):
        start = i * BATCH_SIZE
        end = (i + 1) * BATCH_SIZE
        data = {
            'payload': json.dumps({
                "submission_ids": submission_ids_to_delete[start:end]
            })
        }
        response = requests.delete(url,
                                   headers={'Authorization': f'Token {TOKEN}'},
                                   data=data)
        response.raise_for_status()
    print('Done!')


def get_success_hook_logs_submission_ids(url=None):
    """
    Get all successful posts to external server.
    Warning: `success_hook_logs_submission_ids` can grow a lot quickly because
    we cannot narrow down results with API.
    """
    url = url or f'{SERVER}/api/v2/assets/{ASSET_UID}/hooks/{HOOK_UID}/logs.json'
    retrieve_data(url, add_hook_logs_submission_ids)


def get_related_items():
    """
    Build a dict (`related_items`) of `RELATED_QUESTION_FOR_DELETION` as keys
    and `_submission_date` as values.
    """
    # Get a subset of data of related asset to narrow down the results
    # If today is younger than `FIRST_RUN`, we take data from the beginning
    today = datetime.today().date()
    if today < FIRST_RUN:
        lower_bound_date = date(1970, 1, 1)
    else:
        # If cron task fails to run, we can miss some data at next run.
        # Must ensure cron task runs several times the same day
        lower_bound_date = today - timedelta(
            days=RETENTION_DAYS + NUMBER_OF_DAYS_BETWEEN_RUNS)

    upper_bound_date = today - timedelta(days=RETENTION_DAYS)

    query_string = f'fields=["_submission_time", "{RELATED_QUESTION_FOR_DELETION}"]&' \
                    'sort={"_submission_time": 1}&' \
                    f'query={{"$and": [{{"_submission_time": {{"$gte": "{lower_bound_date}"}}}},' \
                    f'{{"_submission_time": {{"$lt": "{upper_bound_date}"}}}}]}}&' \
                    f'limit={BATCH_SIZE}'
    url = f'{SERVER}/api/v2/assets/{RELATED_ASSET_UID_FOR_DELETION}/data.json?{query_string}'
    retrieve_data(url, add_related_items)


def get_submitted_data_ids():
    query_string = f'fields=["_id", "_submission_time", "{QUESTION_FOR_DELETION}"]&' \
                   'sort={"_submission_time":1}&' \
                   f'limit={BATCH_SIZE}'
    url = f'{SERVER}/api/v2/assets/{ASSET_UID}/data.json?{query_string}'
    retrieve_data(url, add_submission_ids)


def retrieve_data(url, callback):
    response = requests.get(url, headers={'Authorization': f'Token {TOKEN}'})
    response.raise_for_status()
    json_response = response.json()
    fetch_next = callback(json_response)
    if fetch_next and json_response.get('next'):
        retrieve_data(json_response.get('next'), callback)


get_related_items()
get_success_hook_logs_submission_ids()
get_submitted_data_ids()
delete_submissions()