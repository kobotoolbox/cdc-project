#!/bin/bash

PACKAGE_NAME="cdc-deployment-package.zip"
CURRENT_FOLDER=$(pwd)
if [ -f "$PACKAGE_NAME" ]; then
  rm $PACKAGE_NAME
fi

if [ -z "$VIRTUAL_ENV" ]; then
  VIRTUAL_ENV=$(pipenv run env | grep VIRTUAL_ENV | cut -d= -f2-)
fi

echo "Creating deployment package..."
cd $VIRTUAL_ENV/lib/python3.8/site-packages/
zip -r "${CURRENT_FOLDER}/${PACKAGE_NAME}" .
cd $CURRENT_FOLDER
cp clean_submissions.py lambda_function.py
zip -g $PACKAGE_NAME lambda_function.py
rm lambda_function.py

echo "Done!"
