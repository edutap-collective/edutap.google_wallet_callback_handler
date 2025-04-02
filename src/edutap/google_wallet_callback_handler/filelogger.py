from edutap.google_wallet_callback_handler.log import logger


class CallbackHandler:
    """
    Implementation of edutap.wallet_google.protocols.CallbackHandler
    """

    async def handle(
        self,
        class_id: str,
        object_id: str,
        event_type: str,
        exp_time_millis: int,
        count: int,
        nonce: str,
    ) -> None:
        logger.debug("File-Logger")
        line = f'"{class_id}", "{object_id}", "{event_type}", "{exp_time_millis}", "{count}", "{nonce}"\n'
        logger.debug(line)
        try:
            with open("/logs/callback.log", "a") as file:
                file.write(line)
        except Exception as e:
            logger.error(e)
