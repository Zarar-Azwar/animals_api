import os
from dotenv import load_dotenv

class EnvConfig:
    def __init__(self, env_file: str = ".env"):
        # Load environment variables from file
        load_dotenv(env_file)

    def get(self, key: str, default=None):
        """Return the value of the environment variable or default if not found."""
        return os.getenv(key, default)