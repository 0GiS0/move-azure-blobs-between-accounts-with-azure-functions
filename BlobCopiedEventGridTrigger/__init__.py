import json
import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient, RetentionPolicy

def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    blob_service_client = BlobServiceClient.from_connection_string(os.environ.get('ORIGIN_STORAGE_CONNECTION_STRING'))

    # Create a retention policy to retain deleted blobs
    delete_retention_policy = RetentionPolicy(enabled=True, days=1)

    # Set the retention policy on the service
    blob_service_client.set_service_properties(delete_retention_policy=delete_retention_policy)

    # Blob info to delete
    blob_url = event.get_json().get('url')
    container_name = blob_url.split("/")[-2].split("?")[0].split("-")[0]
    blob_name = blob_url.split("/")[-1].split("?")[0]

    blob_to_delete = blob_service_client.get_blob_client(container=container_name,blob=blob_name)   
    
    blob_to_delete.delete_blob()