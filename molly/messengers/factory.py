from molly.messengers.messenger import Messenger
from molly.messengers.slack import SlackWeb


def messenger_factory(messenger_name: str, credentials: dict) -> Messenger:
    if messenger_name == "Slack Web API":
        return SlackWeb(credentials)
    else:
        raise ValueError(f"Unsupported messenger: {messenger_name}")
