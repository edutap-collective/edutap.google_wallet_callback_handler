from pydantic import BaseModel
from typing import Literal


class SignedMessage(BaseModel):
    classId: str
    objectId: str
    expTimeMillis: int
    eventType: Literal[
        "DELETE",
        "SAVE",
        "delete",
        "save",
        "UPDATE",
        "update",
        "DEL",
        "del",
    ]
    nonce: str


class SignedKey(BaseModel):
    keyValue: str
    keyExpiration: int


class IntermediateSigningKey(BaseModel):
    signedKey: SignedKey | str
    signatures: list[str]


class CallbackMessage(BaseModel):
    signature: str
    intermediateSigningKey: IntermediateSigningKey
    protocolVersion: str
    signedMessage: SignedMessage | str  # google sends this as a string, but we want to parse it as a SignedMessage

