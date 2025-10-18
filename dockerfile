FROM python:3.13.9-trixie

WORKDIR /app

COPY ./requirements.txt /app
RUN pip install -r requirements.txt
RUN rm /app/requirements.txt

COPY ./src /app

CMD ["python", "-u", "crawler.py"]