import os
from pipeline import config


if not os.path.exists(config.base_dir):
    print("config.base_dir is invalid; should point to repo base directory;"
          " instead points to: %s" % config.base_dir)
else:
    print("pipeline/config.py appears valid")
