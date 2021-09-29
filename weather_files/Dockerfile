FROM python:3.7

WORKDIR /

ADD ["weather.py","requirement.txt","egendemo-89a335d23fd5.json", "zip-codes-database-FREE.csv" , "./"]

RUN pip install -r requirement.txt

CMD ["python","./weather.py"]