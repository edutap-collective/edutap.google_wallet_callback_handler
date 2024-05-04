from .model import CallbackMessage
from .model import SignedMessage
from pydantic import ValidationError


def decrypt_message(message: str) -> str:
    """dummy decryption of message
    TODO: implement decryption correctly using the google_pay_token_decryption library
    this library has to be extendend for using the "ECv2SigningOnly" protocol
    """
    try:
        callback_message: CallbackMessage = CallbackMessage.model_validate_json(message)
        signedMessage = SignedMessage.model_validate_json(
            callback_message.signedMessage
        )
        print(f"------------dummy decrypted message: {signedMessage} ")
        decrypted_message = signedMessage
        return decrypted_message.json(indent=4)

    except ValidationError as e:
        print("Error parsing message: ", e)
        raise
