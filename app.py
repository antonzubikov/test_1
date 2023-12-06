import json

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import pika
import sqlite3
import asyncio
from datetime import datetime

app = FastAPI()


class Data(BaseModel):
    datetime: str
    title: str
    text: str


def create_table():  # функция для создания таблицы
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS data
                 (datetime text, title text, text text, x_avg_count_in_line real)''')
    conn.commit()
    conn.close()


def insert_data():  # функция для записи данных в базу данных
    conn = sqlite3.connect('data.db')
    c = conn.cursor()

    with open("text.txt", "r") as file:
        lines = file.readlines()
        for line in lines[1:]:
            x_count = line.count('X')
            x_avg_count_in_line = len(line) / x_count if x_count != 0 else 0

            data = {
                "datetime": str(datetime.now()),
                "title": lines[0],
                "text": line,
                "x_avg_count_in_line": x_avg_count_in_line
            }

    c.execute("INSERT INTO data VALUES (?, ?, ?, ?)",
              (data["datetime"], data["title"], data["text"], data["x_avg_count_in_line"]))
    conn.commit()
    conn.close()


def send_to_rabbitmq():  # функция для отправки данных в брокер сообщений
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='data_queue')
    with open("text.txt", "r") as file:
        lines = file.readlines()
        for line in lines[1:]:
            x_count = line.count('X')
            x_avg_count_in_line = len(line) / x_count if x_count != 0 else 0

            data = {
                "datetime": str(datetime.now()),
                "title": lines[0],
                "text": line,
                "x_avg_count_in_line": x_avg_count_in_line
            }
            channel.basic_publish(exchange='', routing_key='data_queue', body=str(data))
    connection.close()


@app.post("/send_data")  # эндпоинт для отправки данных в топик брокера сообщений
async def send_data(data: Data):
    create_table()
    insert_data()
    send_to_rabbitmq()
    return {"message": "Data sent successfully"}


def get_from_rabbitmq():  # функция для получения данных из брокера сообщений
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='data_queue')

    def callback(ch, method, properties, body):
        data = Data.parse_raw(body)
        insert_data(data)

    channel.basic_consume(queue='data_queue', on_message_callback=callback, auto_ack=True)

    channel.start_consuming()


@app.get("/receive_data")  # эндпоинт для получения данных из топика брокера сообщений
async def receive_data():
    get_from_rabbitmq()
    return {"message": "Data received successfully"}


async def get_data_from_db():  # функция для получения данных из базы данных
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute("SELECT * FROM data")
    data = c.fetchall()
    conn.close()
    return data


@app.get("/get_data")  # эндпоинт для асинхронного получения данных из базы данных
async def get_data():
    data = asyncio.run(get_data_from_db())
    return {"data": data}


if __name__ == "__main__":
    uvicorn.run(app, host='localhost', port=8888)

