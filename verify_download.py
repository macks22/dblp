import os
from pipeline import aminer


aminer_requirements = aminer.AminerNetworkData()
paths = [target.output().path for target in aminer_requirements.output()]

all_good = True
for path in paths:
    if not os.path.exists(path):
        all_good = False
        print("Download was unsuccessful or config is invalid;"
              " necessary required file does not exist: %s" % path)

if all_good:
    print("Download and configuration successful; ready to run!")
