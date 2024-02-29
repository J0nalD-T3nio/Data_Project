"""    This python script is created for the purpose
       of practing extracting data from the WEB using
       FTP with Python
"""

import sys
import json
import time
from os import getenv, remove
from pathlib import Path
from ftplib import FTP_TLS
import schedule
import pandas as pd



def main() -> FTP_TLS:
    """
    Summary: The main function for Extracting data

    Returns:
        FTP_TLS: Returns an authenticated FTP
    """
    # Get the FTP connection variables
    ftphost = getenv('FTPHOST')
    ftpuser = getenv('FTPUSER')
    ftppass = getenv('FTPPASS')

    # Return authenticated FTP
    ftp = FTP_TLS(ftphost,ftpuser,ftppass)
    ftp.prot_p()
    return ftp

def upload_to_ftp(ftp: FTP_TLS, file_source: Path) -> None:
    """Summary: Function for uploading files into the WSL ftpuser

    Args:
        ftp (FTP_TLS): The FTP environment
        file_source (Path): The file source path
    """
    with open(file_source, "rb") as contents2:
        ftp.storbinary(f"STOR {file_source.name}", contents2)

def delete_file(prevfile_source: str | Path) -> None:
    """
    Summary: Function for deleting csv files after uploading to the wsl

    Args:
        prevfile_source (str | Path): The file source path
    """
    remove(prevfile_source)

def reads_csv(config: dict) -> pd.DataFrame:
    """
    Summary: Function for converting a JSON file into a DataFrame

    Args:
        config (dict): _description_

    Returns:
        pd.DataFrame: _description_
    """
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url,**params)

def pipeline() -> None:
    """
        Summary: Basic Pipeline using FTP
    """
    # Load JSON config file
    with open("config.json","rb") as contents:
        configs = json.load(contents)

    sftp = main()

    # Iterate through each configuration to get the name and configuration of the source.
    for source_name,source_config in configs.items():
        file_name = Path(source_name + ".CSV")
        df = reads_csv(source_config)
        df.to_csv(file_name, index=False)

        print(f"{file_name} is uploaded.")
        upload_to_ftp(sftp,file_name)

        print(f"{file_name} to be removed.")
        delete_file(file_name)

if __name__ == '__main__':
    param = sys.argv[1]

    if param == "manual":
        pipeline()
    elif param == "schedule":
        schedule.every().day.at("18:44").do(pipeline)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print("Invalid Parameter! The Pipeline will not be activated.")
