import pandas as pd
from datetime import datetime
from meteostat import Point, Daily
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
import os


class PublishToWeatherTopic:

    def __init__(self):
        self.project_id = 'egendemo'
        self.topic_id = 'EgenWeather'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def get_weather_data(self, row) -> str:
        # using now() to get current time
        current_time = datetime.now()

        start = datetime(current_time.year, current_time.month, current_time.day)
        end = datetime(current_time.year, current_time.month, current_time.day)

        location = Point(row['Latitude'], row['Longitude'], 70)
        data = Daily(location, start, end)
        data = data.fetch()
        data['latitude'] = row['Latitude']
        data['longitude'] = row['Longitude']
        data['zipcode'] = row['ZipCode']

        data=data.reset_index()
        final_record = data.to_json(orient='records')
        return final_record

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")
        return callback

    def publish_message_to_topic(self, message: str) -> None:
        publish_future = self.publisher_client.publish(self.topic_path, message.encode('utf-8'))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        print(f"Published messages with error handler to {self.topic_path}.")


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'egendemo-89a335d23fd5.json'
    weather_api = PublishToWeatherTopic()

    zipcode_data = pd.read_csv('zip-codes-database-FREE.csv', nrows=500)

    zipcode_data.drop_duplicates(keep='first', inplace=True, subset=['ZipCode'])

    for index, row in zipcode_data.iterrows():
        get_final_record = weather_api.get_weather_data(row)
        print(len(get_final_record))
        weather_api.publish_message_to_topic(get_final_record)

