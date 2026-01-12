from pydantic_settings import BaseSettings
from dotenv import load_dotenv


load_dotenv()

class SecretKeys(BaseSettings):
    AWS_SQS_VIDEO_QUEUE_URL: str = ""