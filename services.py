import logging
# import config
from io import BytesIO
import json
from azure.storage.blob import  ContainerClient
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from cloud.utils.genSas import gen_sas
import pickle
# logger = logging.getLogger(config.format_logger)
class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

class BaseService(ABC):
    def __init__(self):
        self._status = 200
        self._time = 0
        self.setup()
    
    @property
    def status(self):
        return self._status

    @abstractmethod
    def setup(self):
        raise NotImplementedError

    
class AzureService(BaseService):
    def __init__(self, conn_string = None, container_name = None, account_name = None, account_key = None, is_prod = False):
        self.conn_string = conn_string
        self.container_name = container_name
        self.account_name = account_name
        self.account_key = account_key
        self.is_prod = is_prod

        super(AzureService, self).__init__()

    @property
    def info(self):
        info =  {}
        info['status'] = self._status
        info['container'] =  self.container_name
        info['is_prod'] =  self.is_prod  
        return info
    
    def setup(self):
        try:
            self.container_client = ContainerClient.from_connection_string(conn_str= self.conn_string, container_name=self.container_name)
            if self.container_client.exists():
                self._status = 200
            else:
                self._status = 404
        except Exception as e:
            logger.exception(e)
            self._status = 500
    
    def gen_Sas_blob(self, blob_name):
        url = gen_sas(account_name = self.account_name, account_key= self.account_key, container_name= self.container_name, blob_name= blob_name)
        return url
    def read_json_blob(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob_name)
        streamdownloader = blob_client.download_blob()
        output = json.loads(streamdownloader.readall())
        return output

    def read_pickle_blob(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob_name)
        streamdownloader = blob_client.download_blob()
        output = pickle.loads(streamdownloader.readall())
        return output


    def read_parquet_blob(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        stream_downloader = blob_client.download_blob()
        stream = BytesIO()
        stream_downloader.readinto(stream)
        output = pd.read_parquet(stream, engine='pyarrow')
        return output


    def read_csv_blob(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        streamdownloader = blob_client.download_blob()
        output = pd.read_csv(BytesIO(streamdownloader.readall()))
        return output

    def read_blobs(self, sub_path_blob):
        blobs = self.get_files(sub_path_blob, pattern = '.csv')
        df = pd.DataFrame([])
        for blob_name in blobs:
            try:
                df = pd.concat([df, self.read_csv_blob(blob_name)])
            except Exception as e:
                message = "blob_name: {} | error: {}".format(blob_name, e)
                logger.error(message)
        return df
    
    def get_files(self, blob_name, pattern = '.csv'):
        list_file = []
        blob_list = self.container_client.list_blobs(name_starts_with=blob_name)
        for blob in blob_list:
            if pattern in blob.name:
                list_file.append(blob.name)
        return list_file

    def get_sub_folder_name(self, blob_name, delimiter='/'):
        list_folder = []
        
        blob_list = self.container_client.walk_blobs(blob_name, delimiter=delimiter)
        for blob in blob_list:
            list_folder.append(blob.name[:-1].split("/")[-1])
        return list_folder

    def upload_json(self, blob_name,  json_data, overwrite=True):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(json.dumps(json_data, indent=4, ensure_ascii=False, cls=NumpyEncoder),overwrite=overwrite)
       
        return None

    def upload_pickle(self, blob_name, data, overwrite=True):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(pickle.dumps(data),overwrite=overwrite)
       
        return None

    def upload_dataframe(self, blob_name, df_data, index = False, overwrite = True):
        
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(df_data.to_csv(index=index),overwrite=overwrite)
    
        return True