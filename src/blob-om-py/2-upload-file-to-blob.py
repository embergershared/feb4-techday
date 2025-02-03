import os
import uuid

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient


try:
    print("Azure Blob Storage Python demo: Part 2")

    ###########   AUTHENTICATE TO AZURE   ###########
    # Acquire a credential object.
    credential = DefaultAzureCredential()
    print(f"Acquired an Azure credential")


    ###########   UPLOAD A BLOB   ###########
    # Retrieve the storage blob service URL, which is of the form
    # https://<your-storage-account-name>.blob.core.windows.net/
    storage_url = f"https://{os.environ["STORAGE_ACCOUNT_NAME"]}.blob.core.windows.net/"

    # Create the Blob client object using the storage URL and the credential
    blob_client = BlobClient(
        storage_url,
        container_name=os.environ["CONTAINER_NAME"],
        blob_name=f"sample-blob-{str(uuid.uuid4())[0:5]}.txt",
        credential=credential,
    )

    # Open a local file and upload its contents to Blob Storage
    with open("./sample-source.txt", "rb") as data:
        blob_client.upload_blob(data)
        print(f"Uploaded sample-source.txt to {blob_client.url}")


except Exception as ex:
    print("Exception:")
    print(ex)
