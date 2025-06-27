import asyncio
import websockets
import json
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import time


with open('config.json', 'r', encoding='utf-8') as file:
    config = json.load(file)

apikey = "2d4011bf46906686b133e697c15bfa1c3c19d6a4"
first_corn = [43.0, 131.7]
second_corn = [43.16, 132.12]
selected_aquatory = "Порт Владивосток"

running = False
task = None

button_style = """
    QPushButton {
        font-size:25px;
        background: lightgrey;
        border-radius: 10px;
        text-align:left;
        padding: 5px 10px;
        height: 40px;
        border: none;
        color:black;
        margin: 20px;
    }
    QPushButton:hover {
        background-color: #007AFF;
        color: white;
    }
    QPushButton:pressed {
        background-color: #007AFF;
        color: white;
    }
"""


async def connect_ais_stream(selected_aquatory, selected_items, current_value, update_signal, button):
    global running
    running = True

    start_time = time.time()
    end_time = start_time + current_value*60*60
    print(start_time, end_time)

    # if end_time > start_time:
    #     running = True
    # else:
    #     running = False
    #     update_signal.emit("Функция завершена.")

    while running:
        if int(start_time) == int(end_time):
            update_signal.emit("Время выполнения истекло.")
            button.setStyleSheet(button_style)
            break
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                connection = mysql.connector.connect(**config)
                cursor = connection.cursor()
                connection.start_transaction()
                query = f'SELECT * FROM Aquatories WHERE name="{selected_aquatory}"'
                cursor.execute(query)
                aquatory = cursor.fetchall()
                for row in aquatory:
                    first_corn = [row[3], row[5]]
                    second_corn = [row[2], row[4]]
                    aquatory_id = row[0]

                subscribe_message = {"APIKey": apikey, "BoundingBoxes": [[first_corn, second_corn]]}
                subscribe_message_json = json.dumps(subscribe_message)
                await websocket.send(subscribe_message_json)

                while running:
                    try:
                        message_json = await asyncio.wait_for(websocket.recv(), timeout=5)

                        message = json.loads(message_json)
                        message_type = message["MessageType"]

                        try:
                            if message_type == "PositionReport":
                                ais_message = message['Message']['PositionReport']
                                now_time = datetime.now()
                                Datetime = now_time.strftime('%Y-%m-%d %H:%M:%S')

                                if "Скорость судна" in selected_items:
                                    if "Курс судна" in selected_items:

                                        update_signal.emit(f"ShipId: {ais_message['UserID']} Cog: {ais_message['Cog'] } Sog: {ais_message['Sog'] } Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}; Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO DynamicData (UserID, Cog, Sog, Latitude, Longitude, Datetime)
                                        VALUES ({ais_message['UserID']}, {ais_message['Cog']}, {ais_message['Sog']}, {ais_message['Latitude']}, {ais_message['Longitude']}, "{Datetime}")
                                        """

                                    else:
                                        update_signal.emit(f"ShipId: {ais_message['UserID']} Sog: {ais_message['Sog'] } Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}; Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO DynamicData (UserID, Sog, Latitude, Longitude, Datetime)
                                        VALUES ({ais_message['UserID']}, {ais_message['Sog']}, {ais_message['Latitude']}, {ais_message['Longitude']}, "{Datetime}")
                                        """

                                else:
                                    if "Курс судна" in selected_items:
                                        update_signal.emit(f"ShipId: {ais_message['UserID']} Cog: {ais_message['Cog'] } Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}; Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO DynamicData (UserID, Cog, Latitude, Longitude, Datetime)
                                        VALUES ({ais_message['UserID']}, {ais_message['Cog']}, {ais_message['Latitude']}, {ais_message['Longitude']}, "{Datetime}")
                                        """
                                    else:
                                        update_signal.emit(f"ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}; Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO DynamicData (UserID, Latitude, Longitude, Datetime)
                                        VALUES ({ais_message['UserID']}, {ais_message['Latitude']}, {ais_message['Longitude']}, "{Datetime}")
                                        """

                                cursor.execute(insert_query)
                                connection.commit()

                                insert_query = f"""
                                INSERT INTO Aquatories_vessels (aquatory_id, vessel_id, Datetime)
                                VALUES ({aquatory_id}, {ais_message['UserID']}, "{Datetime}")
                                """

                                cursor.execute(insert_query)
                                connection.commit()

                            if message_type == "ShipStaticData":
                                ais_message_static = message['Message']['ShipStaticData']
                                now_time = datetime.now()
                                Datetime = now_time.strftime('%Y-%m-%d %H:%M:%S')
                                size = ais_message_static['Dimension']

                                if "Тип судна" in selected_items:
                                    if "Размер судна" in selected_items:

                                        update_signal.emit(f"ShipId: {ais_message_static['UserID']} 'A': {size['A']} 'B': {size['B']} 'C': {size['C']} 'D': {size['D']} ImoNumber: {ais_message_static['ImoNumber']} Type: {ais_message_static['Type']} Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO StaticData (UserID, A, B, C, D, ImoNumber, Type, Datetime)
                                        VALUES ({ais_message_static['UserID']}, {size['A']}, {size['B']}, {size['C']}, {size['D']}, {ais_message_static['ImoNumber']}, {ais_message_static['Type']}, "{Datetime}")
                                        """

                                    else:
                                        update_signal.emit(f"ShipId: {ais_message_static['UserID']} ImoNumber: {ais_message_static['ImoNumber']} Type: {ais_message_static['Type']} Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO StaticData (UserID, ImoNumber, Type, Datetime)
                                        VALUES ({ais_message_static['UserID']}, {ais_message_static['ImoNumber']}, {ais_message_static['Type']}, "{Datetime}")
                                        """

                                else:
                                    if "Размер судна" in selected_items:
                                        update_signal.emit(f"ShipId: {ais_message_static['UserID']} 'A': {size['A']} 'B': {size['B']} 'C': {size['C']} 'D': {size['D']} ImoNumber: {ais_message_static['ImoNumber']} Datetime: {Datetime}")

                                        insert_query = f"""
                                        INSERT INTO StaticData (UserID, A, B, C, D, ImoNumber, Datetime)
                                        VALUES ({ais_message_static['UserID']}, {size['A']}, {size['B']}, {size['C']}, {size['D']}, {ais_message_static['ImoNumber']}, "{Datetime}")
                                        """
                                    else:
                                        # print(f"ShipId: {ais_message_static['UserID']} ImoNumber: {ais_message_static['ImoNumber']} Type: {ais_message_static['Type']} Datetime: {Datetime}")

                                        # insert_query = f"""
                                        # INSERT INTO StaticData (UserID, ImoNumber, Datetime)
                                        # VALUES ({ais_message_static['UserID']}, {ais_message_static['ImoNumber']}, "{Datetime}")
                                        # """
                                        continue

                                cursor.execute(insert_query)
                                connection.commit()

                        except Error as e:
                            update_signal.emit(f"Ошибка при вставке данных: {e}")
                            # Откатываем транзакцию в случае ошибки
                            if connection.is_connected():
                                connection.rollback()
                                update_signal.emit("Транзакция откатена.")

                    except asyncio.TimeoutError:
                        # Если сообщение не получено в течение времени ожидания, проверяем состояние running
                        if not running:
                            update_signal.emit("Остановка функции по запросу.")
                            break
                        if time.time() > end_time:
                            update_signal.emit("Время выполнения истекло.")
                            button.setStyleSheet(button_style)
                            break

        except Exception as e:
            update_signal.emit(f"Ошибка: {e}")
        finally:
            running = False
            update_signal.emit("Функция завершена.")


def stop_long_running_function():
    global running
    running = False



# if __name__ == "__main__":
#     asyncio.run(connect_ais_stream(selected_aquatory))
