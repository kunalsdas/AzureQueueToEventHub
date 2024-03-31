import json
import requests
import base64
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData

queue_connection_str = ""
queue_name = ""

eventhub_connection_str = ""

def get_storage_account_key_from_keyvault(key_vault_url, storage_account_key_secret_name):
    # Initialize Key Vault client
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Retrieve the storage account key from Key Vault
    storage_account_key = secret_client.get_secret(storage_account_key_secret_name).value

    return storage_account_key

def read_and_process_queue_message():
    try:
        blob_service_client = BlobServiceClient.from_connection_string("<your_blob_connection_string>")
        
        producer_client = EventHubProducerClient.from_connection_string(eventhub_connection_str)

        key_vault_name = "your-key-vault-name"
        key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
        storage_account_key_secret_name = "your-storage-account-key-secret-name"
        storage_account_key = get_storage_account_key_from_keyvault(key_vault_url, storage_account_key_secret_name)

        queue_service_client = QueueServiceClient.from_connection_string(queue_connection_str)
        queue_client = queue_service_client.get_queue_client(queue_name)

        for message in queue_client.receive_messages():
            decoded_content = base64.b64decode(message.content).decode('utf-8')
            message_data = json.loads(decoded_content)
            blob_url = message_data['subject']
            container_name = blob_url.split('/')[4]
            blob_name = '/'.join(blob_url.split('/')[5:])
            print(container_name, blob_name)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            blob_content = blob_client.download_blob().readall()

            event_data_batch = producer_client.create_batch()
            event_data_batch.add(EventData(blob_content))
            producer_client.send_batch(event_data_batch)

    except Exception as e:
        print(f"Error processing queue message: {str(e)}")

if __name__ == "__main__":
    read_and_process_queue_message()
