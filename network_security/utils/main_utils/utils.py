import yaml
from network_security.exception.exception import CustomExecption
from network_security.logging.logger import logging
import os, sys
import numpy as np
import pickle

from sklearn.metrics import r2_score
from sklearn.model_selection import GridSearchCV

# Function to read a YAML file and return its contents as a dictionary
def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise CustomExecption(e, sys) 

# Function to write content to a YAML file
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)  
        with open(file_path, "w") as file:
            yaml.dump(content, file)  
    except Exception as e:
        raise CustomExecption(e, sys)

# Function to save a numpy array to a file
def save_numpy_array_data(file_path: str, array: np.array):
    """
    Save numpy array data to file
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)  
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array) 
    except Exception as e:
        raise CustomExecption(e, sys) from e

# Function to save an object (like a trained model) to a file
def save_object(file_path: str, obj: object) -> None:
    try:
        logging.info("Entered the save_object method of MainUtils class")
        os.makedirs(os.path.dirname(file_path), exist_ok=True) 
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj) 
        logging.info("Exited the save_object method of MainUtils class")
    except Exception as e:
        raise CustomExecption(e, sys) 

# Function to load a saved object from a file
def load_object(file_path: str) -> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exists")
        with open(file_path, "rb") as file_obj:
            return pickle.load(file_obj)  
    except Exception as e:
        raise CustomExecption(e, sys)

# Function to load a numpy array from a file
def load_numpy_array_data(file_path: str) -> np.array:
    """
    Load numpy array data from file
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj) 
    except Exception as e:
        raise CustomExecption(e, sys) from e

# Function to evaluate multiple machine learning models
def evaluate_models(X_train, y_train, X_test, y_test, models, param):
    """
    Evaluate multiple models using GridSearchCV for hyperparameter tuning and calculate r2 scores.
    """
    try:
        report = {}  

        for i in range(len(list(models))):
            model = list(models.values())[i]
            para = param[list(models.keys())[i]]

            gs = GridSearchCV(model, para, cv=3)
            gs.fit(X_train, y_train)  

            model.set_params(**gs.best_params_)
            model.fit(X_train, y_train)

            y_train_pred = model.predict(X_train)
            y_test_pred = model.predict(X_test)

            train_model_score = r2_score(y_train, y_train_pred)
            test_model_score = r2_score(y_test, y_test_pred)

            report[list(models.keys())[i]] = test_model_score

        return report 

    except Exception as e:
        raise CustomExecption(e, sys)