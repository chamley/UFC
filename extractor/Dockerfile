FROM python:3.10.4-buster

WORKDIR /src

COPY requirements.txt requirements.txt

RUN apt-get update
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "extractor.py"]


