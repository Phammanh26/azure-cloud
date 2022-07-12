from datetime import datetime, timedelta
from azure.storage.blob import generate_blob_sas, BlobSasPermissions

def get_blob_sas(account_name,account_key, container_name, blob_name):
    sas_blob = generate_blob_sas(account_name=account_name, 
                                container_name=container_name,
                                blob_name=blob_name,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
    return sas_blob

def gen_sas(account_name, account_key, container_name, blob_name):
    blob = get_blob_sas(account_name,account_key, container_name, blob_name)
    url = 'https://'+account_name+'.blob.core.windows.net/'+container_name+'/'+blob_name+'?'+blob
    return url