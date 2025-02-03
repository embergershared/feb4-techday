import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient

# Constants, are in .env file


try:
    print("Azure Blob Storage Python demo: Part 1")

    ###########   AUTHENTICATE TO AZURE   ###########
    # Acquire a credential object.
    credential = DefaultAzureCredential()
    # Retrieve the subscription ID as it is the scope of management clients.
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    print(f"Acquired an Azure credential")


    ###########   CREATE A RESOURCE GROUP   ###########
    # Obtain a resource management client.
    resource_client = ResourceManagementClient(credential, subscription_id)

    # Use the client to create the resource group.
    rg_result = resource_client.resource_groups.create_or_update(
        os.environ["RESOURCE_GROUP_NAME"],
        {"location": os.environ["LOCATION"]}
    )
    print(f"Provisioned resource group: {rg_result.name}")


    ###########   CREATE A STORAGE ACCOUNT   ###########
    # Obtain a storage management client.
    storage_client = StorageManagementClient(credential, subscription_id)

    # Check if the account name is available. Storage account names must be globally unique.
    availability_result = storage_client.storage_accounts.check_name_availability(
        {"name": os.environ["STORAGE_ACCOUNT_NAME"]}
    )
    if not availability_result.name_available:
        print(
            f"Storage name {os.environ["STORAGE_ACCOUNT_NAME"]} is already in use. Choose another name."
        )
        exit()
    # The name is available, so provision the account
    poller = storage_client.storage_accounts.begin_create(
        os.environ["RESOURCE_GROUP_NAME"],
        os.environ["STORAGE_ACCOUNT_NAME"],
        {
            "location": os.environ["LOCATION"],
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"},
        },
    )
    # Long-running operations return a poller object; calling poller.result() waits for completion.
    account_result = poller.result()
    print(f"Provisioned storage account: {account_result.name}")

    ###########   CREATE A CONTAINER   ###########
    container = storage_client.blob_containers.create(
        os.environ["RESOURCE_GROUP_NAME"],
        os.environ["STORAGE_ACCOUNT_NAME"],
        os.environ["CONTAINER_NAME"],
        {},
    )
    print(f"Provisioned blob container: {container.name}")


except Exception as ex:
    print("Exception:")
    print(ex)
