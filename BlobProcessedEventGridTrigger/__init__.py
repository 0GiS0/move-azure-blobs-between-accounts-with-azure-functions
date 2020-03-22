import json
import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, AccessPolicy, BlobSasPermissions
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, timedelta


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ.get('ARCHIVE_STORAGE_CONNECTION_STRING'))

    # Get the URL and extract the name of the file and container
    blob_url = event.get_json().get('url')
    logging.info('blob URL: %s', blob_url)
    blob_name = blob_url.split("/")[-1].split("?")[0]
    container_name = blob_url.split("/")[-2].split("?")[0]
    archived_container_name = container_name + '-' + os.environ.get('AZURE_STORAGE_ARCHIVE_CONTAINER')

    blob_service_client_origin = BlobServiceClient.from_connection_string(os.environ.get('ORIGIN_STORAGE_CONNECTION_STRING'))

    blob_to_copy = blob_service_client_origin.get_blob_client(container=container_name, blob=blob_name)

    sas_token = generate_blob_sas(
        blob_to_copy.account_name,
        blob_to_copy.container_name,
        blob_to_copy.blob_name,
        account_key=blob_service_client_origin.credential.account_key,        
        permission=BlobSasPermissions(read=True),
        start=datetime.utcnow() + timedelta(seconds=1),
        expiry=datetime.utcnow() + timedelta(hours=1))

    logging.info('sas token: %s',sas_token)

    archived_container = blob_service_client.get_container_client(archived_container_name)

    # Create new Container
    try:
        archived_container.create_container()
    except ResourceExistsError:
        pass

    copied_blob = blob_service_client.get_blob_client(
        archived_container_name, blob_name)

    blob_to_copy_url = blob_url + '?' + sas_token

    logging.info('blob url: ' + blob_to_copy_url)

    # Start copy
    copied_blob.start_copy_from_url(blob_to_copy_url)