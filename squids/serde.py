import json
from typing import Any, Dict


class Serde:
    @classmethod
    def serialize(cls, body: Dict[str, Any]) -> str:
        raise NotImplementedError("Subclass must override this method")

    @classmethod
    def deserialize(cls, body: str) -> Dict[str, Any]:
        raise NotImplementedError("Subclass must override this method")


class JSONSerde(Serde):
    @classmethod
    def serialize(cls, body: Dict[str, Any]) -> str:
        return json.dumps(body)

    @classmethod
    def deserialize(cls, body: str) -> Dict[str, Any]:
        return json.loads(body)
