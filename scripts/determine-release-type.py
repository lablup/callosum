import os
import re
from pathlib import Path

import callosum


def main():
    gh_env_file = Path(os.environ.get("GITHUB_ENV", "/dev/null"))
    version = callosum.__version__
    m = re.search(r"(rc\d+|a\d+|b\d+|dev\d+)$", version)
    is_prerelease = "true" if m is not None else "false"
    with open(gh_env_file, "a") as f:
        f.write(f"IS_PRERELEASE={is_prerelease}\n")


if __name__ == "__main__":
    main()
