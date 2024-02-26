"""Module for fetching connection strings, API-Keys using an Azure Keyvault"""

import os
from typing import Union
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential, InteractiveBrowserCredential


def get_credentials() -> Union[ManagedIdentityCredential, InteractiveBrowserCredential]:
    ''''This function intents to return credentials, which can be used to connect with for example a keyvault. 
    Based on the environment a different credential type is returned.

    When running locally InteractiveBrowserCredential is used. This requires the user
    to login with Azure AD. If the code is running in the cloud and a ManagedIdentity is set up, this
    function returns credentials that are setup via the ManagedIdentityCredential'''

    # Check if the code is running in the cloud (Azure App Service, Azure Functions, etc.)
    pass
    return credential

def get_keyvault_connection(keyvault_url: str) -> SecretClient:
    '''Connects with keyvault and return SecretClient instance. Can be used to interact with KeyVault. Docs:
    https://learn.microsoft.com/en-us/python/api/azure-keyvault-secrets/azure.keyvault.secrets.secretclient?view=azure-python
    '''
    credential = get_credentials()
    return SecretClient(keyvault_url, credential)
