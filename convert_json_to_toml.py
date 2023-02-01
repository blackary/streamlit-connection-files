import json
from pathlib import Path

import tomli_w

data = json.loads(Path("gcs_token.json").read_text())

print(tomli_w.dumps(data))
