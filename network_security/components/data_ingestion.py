from network_security.exception.exception import CustomExecption
from network_security.logging.logger import logging


## configuration of the Data Ingestion Config
from network_security.entity.config_entity import DataIngestionConfig
from network_security.entity.artifact_entity import DataIngestionArtifact


import os
import sys
import numpy as np
import pandas as pd
import pymongo
from typing import List
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

load_dotenv()

# Get MongoDB URL from environment variable
MongoDB_url = os.getenv("MongoDB_url")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        """
        Initialize with data ingestion config.
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise CustomExecption(e, sys)

    def export_collection_as_dataframe(self):
        """
        Fetch data from MongoDB and return as DataFrame.
        """
        try:
            # Connect to MongoDB and get the data
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MongoDB_url)
            collection = self.mongo_client[database_name][collection_name]

            # Convert MongoDB collection to DataFrame
            df = pd.DataFrame(list(collection.find()))
            
            # Drop MongoDB _id column and replace 'na' with NaN
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)
            df.replace({"na": np.nan}, inplace=True)
            return df
        except Exception as e:
            raise CustomExecption(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        """
        Save DataFrame to a CSV file in the feature store folder.
        """
        try:
            # Get file path and create directory if not exists
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            # Save DataFrame to CSV
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe
        except Exception as e:
            raise CustomExecption(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        """
        Split data into train and test sets and save to CSV files.
        """
        try:
            # Split the data into train and test sets
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Train-test split performed.")

            # Create directories if not exists
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Save train and test sets to CSV
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info("Train and test sets saved.")

        except Exception as e:
            raise CustomExecption(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Run the entire data ingestion pipeline.
        """
        try:
            # Fetch data from MongoDB
            dataframe = self.export_collection_as_dataframe()
            
            # Save data to feature store
            dataframe = self.export_data_into_feature_store(dataframe)
            
            # Split data into train and test sets
            self.split_data_as_train_test(dataframe)
            
            # Return the paths to the saved data
            dataingestionartifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )
            return dataingestionartifact

        except Exception as e:
            raise CustomExecption(e, sys)
