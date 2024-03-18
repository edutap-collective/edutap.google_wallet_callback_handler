from pydantic import BaseModel
from typing import Literal


class SignedMessage(BaseModel):
    classId: str
    objectId: str
    expTimeMillis: int
    eventType: Literal[
        "DELETE", "SAVE", "delete", "save", "UPDATE", "update", "DEL", "del"
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

    # def repair(self):
    #     """
    #     repairs the message so that signedMessage is a real object rather than a string
    #     """
    #     if isinstance(self.signedMessage, str):
    #         self.signedMessage = SignedMessage.parse_raw(self.signedMessage)

    #     if isinstance(self.intermediateSigningKey.signedKey, str):
    #         self.intermediateSigningKey.signedKey = SignedKey.parse_raw(self.intermediateSigningKey.signedKey)
