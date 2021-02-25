# coding: utf-8
import contextlib
import math
import json
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

import requests
import dropbox
from dotenv import load_dotenv

"""
This script retrieves values (`related_items`) from one question (`RELATED_QUESTION_FOR_DELETION) 
in a specific survey (i.e. `RELATED_ASSET_UID_FOR_DELETION`).
Then, it deletes all submissions from another survey (`ASSET_UID`).
Each submission must:
- contain a question where its name must equal `QUESTION_FOR_DELETION`
- have a value of question `QUESTION_FOR_DELETION` present in `related_items`
- have been submitted successfully to external server (check with hook `HOOK_UID)
"""

load_dotenv()

# Server domain with protocol. e.g. https://kf.kobotoolbox.org
SERVER = os.getenv('SERVER')
# User's token. Can be retrieved at https://[kpi-url]/token
KPI_TOKEN = os.getenv('KPI_TOKEN')
# Number of days after a submitted related data that a submission can be deleted
RETENTION_DAYS = os.getenv('RETENTION_DAYS', 14)
# Asset's unique ID. e.g. https://[kpi-url]/api/v2/assets/{asset_uid}/
ASSET_UID = os.getenv('ASSET_UID')
# Values of this question are used to build the backup path in Dropbox
QUESTION_NAME = os.getenv('QUESTION_NAME')
# Hook's unique ID. e.g. https://[kpi-url]/api/v2/assets/{asset_uid}/hooks/{hook_uid}  # noqa
HOOK_UID = os.getenv('HOOK_UID')
# Number of submissions to retrieve/delete at once.
BATCH_SIZE = 500
# Dry run mode. Submissions will be deleted only if it equals `'False'`
DRY_RUN = os.getenv('DRY_FUN')

DROPBOX_TOKEN = os.getenv('DROPBOX_TOKEN')
DROPBOX_ROOT_DIR = os.getenv('DROPBOX_ROOT_DIR')

submission_ids_to_delete = []
success_hook_logs_submission_ids = []
dbx = None


def add_hook_logs_submission_ids(json_response):
    """
    Retrieve all instance ids from RestService logs that have been successfully
    sent
    """
    results = json_response.get('results')
    if results:
        for result in results:
            if result.get('status_code') == 200:
                success_hook_logs_submission_ids.append(result['instance_id'])

    return True


def add_submission_ids(json_response):
    """
    Add submission ids to `submission_ids_to_delete` only if:

    - submission has been posted to external server successfully
    - submission has been submitted `RETENTION_DAYS` ago.
    """
    results = json_response.get('results')
    if results:
        for result in results:
            submission_time = datetime.strptime(result['_submission_time'],
                                                '%Y-%m-%dT%H:%M:%S')
            delta = datetime.now() - submission_time
            # Submissions are sorted by `_submission_time`.
            # No need to go further.
            if delta.days < int(RETENTION_DAYS):
                return False

            if result['_id'] in success_hook_logs_submission_ids:
                download_attachments(result)
                submission_ids_to_delete.append(result['_id'])

    return True


def connect_to_dropbox():
    global dbx
    log('Opening DropBox connection...')
    dbx = dropbox.Dropbox(DROPBOX_TOKEN)


def close_dropbox():
    log('Closing DropBox connection...')
    dbx.close()


def delete_submissions():
    """
    Delete submissions by chunks which their id are present in
    `submission_ids_to_delete`
    """
    url = f'{SERVER}/api/v2/assets/{ASSET_UID}/data/bulk/'
    iteration = math.ceil(len(submission_ids_to_delete) / BATCH_SIZE)
    log('Deleting {} submissions...'.format(len(submission_ids_to_delete)))
    for i in range(iteration):
        start = i * BATCH_SIZE
        end = (i + 1) * BATCH_SIZE
        data = {
            'payload': json.dumps({
                "submission_ids": submission_ids_to_delete[start:end]
            })
        }

        if DRY_RUN == 'False':
            response = requests.delete(
                url,
                headers={'Authorization': f'Token {KPI_TOKEN}'},
                data=data
            )
            response.raise_for_status()


def download_attachments(submission):
    """
    Copy attachments to DropBox.
    """
    try:
        attachments = submission['_attachments']
        submission_value = submission[QUESTION_NAME]
    except KeyError:
        return

    for attachment in attachments:
        try:
            download_url = attachment['download_url']
            filename = attachment['filename']
        except KeyError:
            log('ERROR: Properties `filename` or `download_url` are missing',
                error=True)
            continue

        response = requests.get(download_url,
                                allow_redirects=True,
                                headers={'Authorization': f'Token {KPI_TOKEN}'})
        if response.status_code != 200:
            log(f'ERROR: Could not retrieve {download_url}. '
                f'Status code: ({response.status_code})',
                error=True)
            continue

        _, tmp_file_path = tempfile.mkstemp()
        with open(tmp_file_path, 'wb') as f:
            f.write(response.content)

        filename = os.path.basename(filename)
        sub_folder = f"{submission_value}/{submission['_id']}"
        upload_to_dropbox(tmp_file_path,
                          DROPBOX_ROOT_DIR,
                          sub_folder,
                          filename,
                          overwrite=False)

        os.remove(tmp_file_path)


def get_success_hook_logs_submission_ids(url=None):
    """
    Get all successful posts to external server.
    Warning: `success_hook_logs_submission_ids` can grow a lot quickly because
    we cannot narrow down results with API.
    """
    log('Retrieving all successfully submitted data to external server...')
    url = url or f'{SERVER}/api/v2/assets/{ASSET_UID}/hooks/{HOOK_UID}/logs.json'
    retrieve_data(url, add_hook_logs_submission_ids)


def get_submissions():
    """
    Retrieve all submissions up to `RETENTION_DAYS` ago
    """
    # Get a subset of data of related asset to narrow down the results
    # If today is younger than `FIRST_RUN`, we take data from the beginning
    log(f'Retrieving all submissions for asset `{ASSET_UID}`...')
    today = datetime.today().date()
    max_date = today - timedelta(days=int(RETENTION_DAYS))

    query_string = (
        f'fields=["_id", "_submission_time", "_attachments", "{QUESTION_NAME}"]'
        '&sort={"_submission_time": 1}'
        f'&query={{"_submission_time": {{"$lte": "{max_date}"}}}}'  # noqa
        f'&limit={BATCH_SIZE}'
    )
    url = f'{SERVER}/api/v2/assets/{ASSET_UID}/' \
          f'data.json?{query_string}'
    retrieve_data(url, add_submission_ids)


def log(message, error=False):
    console = sys.stdout if not error else sys.stderr
    now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    console.write(f'[{now}] {message}\n')


def retrieve_data(url, callback):
    response = requests.get(url, headers={'Authorization': f'Token {KPI_TOKEN}'})
    response.raise_for_status()
    json_response = response.json()
    fetch_next = callback(json_response)
    if fetch_next and json_response.get('next'):
        retrieve_data(json_response.get('next'), callback)


@contextlib.contextmanager
def stopwatch(message):
    """
    Context manager to print how long a block of code took.

    Source: https://raw.githubusercontent.com/dropbox/dropbox-sdk-python/master/example/updown.py # noqa

    """
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        log('Total elapsed time for %s: %.3f' % (message, t1 - t0))


def upload_to_dropbox(fullname, folder, subfolder, name, overwrite=False):
    """
    Source https://raw.githubusercontent.com/dropbox/dropbox-sdk-python/master/example/updown.py  # noqa
    :param fullname: localpath
    :param folder: dropbox root dir
    :param subfolder: dropbox subfolder
    :param name: filename
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    mtime = os.path.getmtime(fullname)

    with open(fullname, 'rb') as f:
        data = f.read()
    with stopwatch('upload %d bytes' % len(data)):
        try:
            res = dbx.files_upload(
                data, path, mode,
                client_modified=datetime(*time.gmtime(mtime)[:6]),
                mute=True)
        except dropbox.exceptions.ApiError as err:
            log(f'*** API error {err}', error=True)
            return None
    log(f"uploaded as {res.name}")
    return res


def lambda_handler(event, context):
    if DRY_RUN == 'True':
        log('Starting task...')
    else:
        log('Starting task (Dry Run)...')
    connect_to_dropbox()
    get_success_hook_logs_submission_ids()
    get_submissions()
    delete_submissions()
    close_dropbox()
    log('Task is over!')


if __name__ == '__main__':
    lambda_handler(None, None)
