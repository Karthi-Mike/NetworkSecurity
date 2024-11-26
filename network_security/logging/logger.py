import logging 
import os
from datetime import datetime


#Log file name

LOG_FILE_NAME = f"{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log"

#Log directory path

LOG_FOLDER = os.path.join(os.getcwd(), "logs_folder")
os.makedirs(LOG_FOLDER, exist_ok=True)

#Log file path
LOG_FILE_PATH = os.path.join(LOG_FOLDER,LOG_FILE_NAME)


# Configure logging to write to the specified log file

logging.basicConfig(
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    filename=LOG_FILE_PATH,
    level=logging.INFO
)

#logging entry to confirm setup
logging.info("Logging has been set up succssefully")

