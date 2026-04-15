# src/config.py

from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_KEY", "")
    base_url: str = os.getenv("BASE_URL", "https://www.josephperrier.com/")
    user_agent: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (compatible; JosephPerrierIngestion/1.0)"
    )
    timeout: int = int(os.getenv("TIMEOUT", "20"))
    max_workers: int = int(os.getenv("MAX_WORKERS", "8"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "25"))


settings = Settings()