from time import sleep
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
from requests import Session
from entities import CRYPTO_TICKER_URL, CRYPTO_TICKER_CONFIG
import os

class PublishToCryptoTopic:
    def __init__(self):
        self.project_id = 'egendemo'
        self.topic_id = 'EgenCrypto'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def get_crypto_ticker_data(self) -> str:
        params = {
            "key": "3dedf7ae8b5b6741d36e3511ac5c67b00520de00",
            "convert": CRYPTO_TICKER_CONFIG["currency"],
            "interval": "Id",
            "per-page": "100",
            "page": "1",
        }
        ses = Session()
        res = ses.get(CRYPTO_TICKER_URL, params=params, stream=True)

        if 200 <= res.status_code < 400:
            print(f"Response - {res.status_code}: {res.text}")
            return res.text
        else:
            raise Exception(f"Failed to fetch API data - {res.status_code}: {res.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")
        return callback

    def publish_message_to_topic(self, message: str) -> None:
        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        print(f"Published messages with error handler to {self.topic_path}.")


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'egendemo-89a335d23fd5.json'

    svc = PublishToCryptoTopic()
    for i in range(21):
        message = svc.get_crypto_ticker_data()
        print(message)
        svc.publish_message_to_topic(message)
        sleep(90)