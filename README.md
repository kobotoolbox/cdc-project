`clean_submission.py` removes all data submission to a project that are older than 14 days.
Each submission is deleted only if it has been successfully copied to the related external server on submit (see RESt Services in project settings)
Moreover, its attachments are pushed to Dropbox as well prior to deletion 

## Requirements

- python 3.8
- pipenv

## Installation

`pipenv install`

## Add `.env` file

Create a file `.env` at the root level containing those variables

- `SERVER` e.g.: https://kf.kobotoolbox.org
- `KPI_TOKEN` see: https://kf.kobotoolbox.org/token
- `ASSET_UID` unique id of the project
- `QUESTION_NAME` question name of the value used to build the backup path to dropbox
- `HOOK_UID` unique id of the RESt service used to validate data has been successfully copied to external server.
- `DROPBOX_TOKEN` OAuth2 Token of DropBox app. see: https://www.dropbox.com/developers
- `DROPBOX_ROOT_DIR` Root path in DropBox, e.g.: `kobo-backups` (no slashes)
- `DRY_RUN` Only delete submissions where it equals `False`
- `RETENTION_DAYS` Number of days before submission can be deleted

## Run

`pipenv run python clean_submissions.py`


## AWS Lambda

A script is provided `create-lambda-package.sh` to create a deployment package 
for a lambda function.

It creates a file called `cdc-deployment-package.zip` that can be uploaded from AWS lambda console (or with AWS CLI).

