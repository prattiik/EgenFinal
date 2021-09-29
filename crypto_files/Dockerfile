FROM python:3.7

WORKDIR /

ADD ["publish_to_crypto_topic.py","requirement.txt","entities.py","egendemo-89a335d23fd5.json", "./"]

RUN pip install -r requirement.txt

CMD ["python","./publish_to_crypto_topic.py"]