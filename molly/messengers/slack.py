from dataclasses import dataclass

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from molly.messengers.messenger import Messenger


@dataclass
class SlackWeb(Messenger):
    @property
    def messenger_name(self) -> str:
        return "Slack Web API"

    def __post_init__(self):
        self.__client = WebClient(token=self.credentials["slack_bot_token"])

    def send(self, message: str, destination: str):
        try:
            self.__client.chat_postMessage(channel=destination, text=message)
        except SlackApiError as e:
            assert e.response["error"]
