from dataclasses import dataclass
import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from molly.messengers.messenger import Messenger

logger = logging.getLogger(__name__)


@dataclass
class SlackWeb(Messenger):
    @property
    def messenger_name(self) -> str:
        return "Slack Web API"

    def __post_init__(self):
        self.__client = WebClient(token=self.credentials["slack_bot_token"])

    @staticmethod
    def __format_message(message: str) -> str:
        SLACK_MAX_MESSAGE_LENGTH = 40000
        if not len(message) > SLACK_MAX_MESSAGE_LENGTH:
            return message

        # TODO: Add a more sophisticated way to truncate the message
        logger.warning(
            f"Message length exceeds {SLACK_MAX_MESSAGE_LENGTH} characters. Truncating message."
        )
        return f"Truncated message: {message[:SLACK_MAX_MESSAGE_LENGTH-20]}..."

    def send(self, message: str, destination: str):
        try:
            message = self.__format_message(message)
            self.__client.chat_postMessage(channel=destination, text=message)
            logger.info(f"Message sent to {destination}")
        except SlackApiError as e:
            logger.error(f"Error sending message: {e.response['error']}")
            assert e.response["error"]
            raise e
