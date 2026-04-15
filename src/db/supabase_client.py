# src/db/supabase_client.py

from supabase import create_client, Client
from src.config import settings


def get_client() -> Client:
    if not settings.supabase_url or not settings.supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")
    return create_client(settings.supabase_url, settings.supabase_key)