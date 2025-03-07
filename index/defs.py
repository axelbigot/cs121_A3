# The name of the entire A3 application.
from pathlib import Path

import platformdirs


APP_NAME = 'CS121_A3'
# Local data dir for this application.
APP_DATA_DIR = Path(platformdirs.user_data_dir(APP_NAME))
