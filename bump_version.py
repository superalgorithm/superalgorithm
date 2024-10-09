import tomli
import tomli_w
from pathlib import Path
import sys

def bump_version(bump_type):
    content = tomli.loads(Path("pyproject.toml").read_text())
    current_version = content['project']['version']
    major, minor, patch = map(int, current_version.split('.'))
    
    if bump_type == 'major':
        major += 1
        minor = patch = 0
    elif bump_type == 'minor':
        minor += 1
        patch = 0
    else:  # patch
        patch += 1
    
    new_version = f"{major}.{minor}.{patch}"
    content['project']['version'] = new_version
    Path("pyproject.toml").write_text(tomli_w.dumps(content))
    return new_version

if __name__ == "__main__":
    new_version = bump_version(sys.argv[1])
    print(f"::set-output name=new_version::{new_version}")
