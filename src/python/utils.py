from datetime import datetime
from typing import Tuple


def log(log_path: str, logmsg: str, printout: bool = False) -> None:
    """Function to add a line to a logfile.

    Args:
        log_path: Full path to the filename with the log.
        logmsg: Message to be appended to the log file.
        printout: If True, `logmsg` is also printed out. Defaults to False.

    Returns:
        None.
    """
    # Add the current timestamp to the log
    logmsg = f'{datetime.now()} - {logmsg}'

    if printout:
        print(logmsg)

    with open(log_path, 'a') as f:
        f.write('\n')
        f.write(logmsg)
        f.close()


def load_credentials(filepath: str) -> Tuple[str, str]:
    """Function to load credentials from a file.

    Args:
        filepath: Full file path to the credentials file.
    
    Returns:
        (user, password): Tuple of user and password strings.
    """
    with open(filepath) as f:
        lines = f.readlines()
        f.close()
    user = lines[0].split(':')[1].strip()
    password = lines[1].split(':')[1].strip()

    return (user, password)