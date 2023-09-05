import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


def get_tag():
    gh_ref_name = os.environ.get("GITHUB_REF_NAME")
    if gh_ref_name:
        return gh_ref_name
    p = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True
    )
    revision = p.stdout.decode().strip()
    return revision


def get_prev_tag():
    p = subprocess.run(
        ["git", "rev-list", "--tags", "--skip=1", "--max-count=1"],
        capture_output=True,
    )
    rev = p.stdout.decode().strip()

    p = subprocess.run(
        ["git", "describe", "--abbrev=0", "--tags", f"{rev}"], capture_output=True
    )
    tag = p.stdout.decode().strip()
    return tag


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--draft",
        action="store_true",
        default=False,
        help="Do not write to file but only print the expected output",
    )
    args = parser.parse_args()

    prev_tag, tag = get_prev_tag(), get_tag()
    commitlog_url = f"https://github.com/lablup/callosum/compare/{prev_tag}...{tag}"
    changelog_url = f"https://github.com/lablup/callosum/blob/{tag}/CHANGELOG.md"

    print(f"Making release notes for {tag} ...", file=sys.stderr)

    input_path = Path("./CHANGES.md")
    output_path = Path("./CHANGELOG_RELEASE.md")
    try:
        version = Path("./VERSION").read_text().strip()
        input_text = input_path.read_text()
        m = re.search(
            rf"(?:^|\n)## {re.escape(version)}(?:[^\n]*)?\n(.*?)(?:\n## |$)",
            input_text,
            re.S,
        )
        if m is not None:
            content = m.group(1).strip()
            content += (
                f"\n\n### Full Changelog"
                f"\n\nCheck out [the full changelog]({changelog_url}) "
                f"until this release ({tag}).\n"
            )
            content += (
                f"\n\n### Full Commit Logs"
                f"\n\nCheck out [the full commit logs]({commitlog_url}) "
                f"between release ({prev_tag}) and ({tag}).\n"
            )
            if not args.draft:
                output_path.write_text(content)
            print("--------")
            print(content)
            print("--------")
            print(
                "Successfully extracted the latest changelog to CHANGELOG_RELEASE.md",
                file=sys.stderr,
            )
        else:
            print(
                "::error ::Could not extract the latest changelog from CHANGELOG.md",
                file=sys.stderr,
            )
            sys.exit(1)
    except IOError as e:
        print(f"::error ::Could read or write from file: {e!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
