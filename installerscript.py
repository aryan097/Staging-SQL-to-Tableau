# install_requirements.py
import subprocess
import sys
import platform

def install_requirements():
    requirements = [
        "pandas",
        "pyodbc",
        "pantab",
        "tableauhyperapi",
        "tableauserverclient"
        ]

    print(f"Detected OS: {platform.system()}")
    for pkg in requirements:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

if __name__ == "__main__":
    install_requirements()
