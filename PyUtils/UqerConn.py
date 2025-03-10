from dataclasses import dataclass
from uqer import Client


@dataclass(frozen=True)
class UQERConfig:
    UQER_TOKEN = "a29c02476135eea720388c6e073be03fa449c5ac345f4d248763cb90e50f-test"

    @classmethod
    def init_uqer(cls):
        try:
            Client(token=cls.UQER_TOKEN)
        except Exception as e:
            raise e
