# install_dependencies.py
import subprocess
import sys

required_packages = [
    # Core Dependencies
    "numpy",
    "pandas",
    "requests",
    "matplotlib",
    "seaborn",
    "scikit-learn",
    "tqdm",
    "spacy",
    "shap",
    "optuna",
    "beautifulsoup4",
    "pydub",
    "werkzeug",
    "Pillow",
    "flask",
    "librosa",
    "langdetect",
    "transformers",
    "text-cleaner",  

    # PySpark and Kafka
    "pyspark",
    "kafka-python",

    # MongoDB and Neo4j
    "pymongo",
    "neo4j",

    # Audio and Speech
    "speechrecognition",
    "torchaudio",
    "ffmpeg-python",

    # Torch & Reinforcement Learning
    "torch",
    "stable-baselines3[extra]",
    "sb3-contrib",
    "gymnasium",

    # Optional Utilities
    "psutil"
]

def install_packages(packages):
    for pkg in packages:
        print(f"\nInstalling: {pkg}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
        except subprocess.CalledProcessError as e:
            print(f"Failed to install {pkg}: {e}")

if __name__ == "__main__":
    install_packages(required_packages)
    print("\nAll dependencies attempted for installation.")
