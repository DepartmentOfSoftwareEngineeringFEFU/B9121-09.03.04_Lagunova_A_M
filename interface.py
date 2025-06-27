import sys
from datetime import datetime
import csv
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow,
                             QTabWidget, QWidget,
                             QVBoxLayout, QPushButton, QHBoxLayout,
                             QLabel, QListWidget, QListWidgetItem,
                             QLineEdit, QComboBox, QMessageBox,
                             QDoubleSpinBox, QTextEdit, QDateEdit,
                             QFileDialog)
import mysql.connector
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread, QDate
import asyncio
from collect_data import connect_ais_stream, stop_long_running_function
from staticmap import StaticMap, CircleMarker
from PyQt5.QtGui import QColor, QPixmap, QPainter
import shutil
import numpy as np
from collections import defaultdict
import json


CURRENT_DIRECTORY = Path(__file__).resolve().parent
R = 6371
MIN_SAFE_DISTANCE = 1.85


class Worker(QObject):
    update_signal = pyqtSignal(str) 

    def __init__(self, param1, param2, param3, button):
        super().__init__()
        self.param1 = param1
        self.param2 = param2
        self.param3 = param3
        self.button = button

    def run(self):
        asyncio.run(connect_ais_stream(self.param1, self.param2, self.param3, self.update_signal, self.button))


class MainWindow(QMainWindow):
    def __init__(self, config):
        super().__init__()
        self.config = config

        self.setWindowTitle("Сбор и анализ АИС-данных")

        self.setGeometry(100, 100, 800, 600)
        self.setStyleSheet("background-color: lightgrey;")

        # Создание вкладок
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.tabs.setStyleSheet("""
            QTabBar::tab {
                height: 40px;
                padding: 10px;
                font-size: 20px;
                width: 300px;
                border-radius: 10px;
                color:black;
            }
            QTabBar::tab:selected {
                background-color: white;
                color:black;
            }
            QTabBar::tab:hover {
                background-color: #007AFF;
                color: white;
            }
        """)

        self.combo_style = """
            QComboBox {
                background-color: white;
                color: black;
                border: 1px solid gray;
                border-radius: 5px;
                padding: 5px;
                font-size: 20px;
                margin:20px;
                width: 350px;
            }
            QComboBox:hover {
                background-color: #007AFF;
                color: white;
            }
            QComboBox::drop-down {
                border-left: 1px solid gray; /* Граница выпадающего списка */
            }
            QComboBox::down-arrow {
                image: url(arrow.png); /* Замените на путь к вашей стрелке */
            }
            QComboBox QAbstractItemView {
                background-color: white; /* Цвет фона выпадающего списка */
                color: black; /* Цвет текста выпадающего списка */
            }
            QComboBox QAbstractItemView::item {
                background-color: white; /* Цвет фона элемента по умолчанию */
                color: black; /* Цвет текста элемента по умолчанию */
            }
            QComboBox QAbstractItemView::item:hover {
                background-color: #007AFF; /* Цвет фона элемента при наведении */
                color: white; /* Цвет текста элемента при наведении */
            }
        """

        self.date_style = """
            QDateEdit {
                background-color: #f0f0f0;
                border: 2px solid #555;
                border-radius: 5px;
                padding: 5px;
                font-size: 14px;
                color: black;
            }

            QDateEdit::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 20px;
                border-left: 1px solid #555;
            }

            QDateEdit::down-arrow {
                image: url(down_arrow.png); /* Можно задать изображение стрелки */
            }
            """
        self.button_style = """
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

        self.button_style_clicked = """
            QPushButton {
                font-size:25px;
                border-radius: 10px;
                text-align:left;
                padding: 5px 10px;
                height: 40px;
                border: none;
                background-color: #007AFF;
                color: white;
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

        self.tab1 = QWidget()
        self.tab1.setStyleSheet("background-color: white; color:black;")
        self.tabs.addTab(self.tab1, "Сбор данных")

        self.tab2 = QWidget()
        self.tab2.setStyleSheet("background-color: white;")
        self.tabs.addTab(self.tab2, "Анализ данных")

        self.worker = None
        self.thread = None

        self.db_connection = mysql.connector.connect(**config)
        self.cursor = self.db_connection.cursor()

        self.setup_tab1()
        self.setup_tab2()

    def clean_layout(self, layout):
        if layout is not None:
            if layout.count() > 0:
                while layout.count():
                    item = layout.takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()
                    else:
                        self.clean_layout(item.layout())

    def get_ship_positions(self, start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct):

        if selected_season !=0:
            if is_season_date_correct:
                query = f"""
                SELECT Latitude, Longitude FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season};
                """
            else:
                query = f"""
                SELECT Latitude, Longitude FROM DynamicData
                WHERE Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season};
                """
        else:
            query = f"""
                SELECT Latitude, Longitude FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max};
                """
        try:
            self.cursor.execute(query)
            positions = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")

        return positions

    def get_ship_speed(self, start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct):
        if selected_season !=0:
            if is_season_date_correct:
                query = f"""
                SELECT Latitude, Longitude, Sog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Sog IS NOT NULL
                AND Sog <> 0.0
                """
            else:
                query = f"""
                SELECT Latitude, Longitude, Sog FROM DynamicData
                WHERE Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Sog IS NOT NULL
                AND Sog <> 0.0;
                """
        else:
            query = f"""
                SELECT Latitude, Longitude, Sog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND Sog IS NOT NULL
                AND Sog <> 0.0;
                """
        try:
            self.cursor.execute(query)
            positions = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")
        return positions

    def get_ship_size(self, start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct):
        if selected_season !=0:
            if is_season_date_correct:
                query = f"""
                SELECT DISTINCT Latitude, Longitude, (A + B) as size
                FROM DynamicData
                INNER JOIN StaticData ON DynamicData.UserID = StaticData.UserID
                WHERE DynamicData.Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(DynamicData.Datetime) IN {selected_season}
                AND A IS NOT NULL
                AND B IS NOT NULL;
                """
            else:
                query = f"""
                SELECT DISTINCT Latitude, Longitude, (A + B) as size
                FROM DynamicData
                INNER JOIN StaticData ON DynamicData.UserID = StaticData.UserID
                WHERE Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(DynamicData.Datetime) IN {selected_season}
                AND A IS NOT NULL
                AND B IS NOT NULL;
                """
        else:
            query = f"""
                SELECT DISTINCT Latitude, Longitude, (A + B) as size
                FROM DynamicData
                INNER JOIN StaticData ON DynamicData.UserID = StaticData.UserID
                WHERE DynamicData.Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND A IS NOT NULL
                AND B IS NOT NULL;
                """
        try:
            self.cursor.execute(query)
            positions = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")
        return positions

    def get_ship_course(self, start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct):
        if selected_season != 0:
            if is_season_date_correct:
                query = f"""
                SELECT Latitude, Longitude, Cog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Cog IS NOT NULL
                """
            else:
                query = f"""
                SELECT Latitude, Longitude, Cog FROM DynamicData
                WHERE Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Cog IS NOT NULL
                """
        else:
            query = f"""
                SELECT Latitude, Longitude, Cog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND Cog IS NOT NULL
                """
        try:
            self.cursor.execute(query)
            positions = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")
        return positions

    def get_ships_cog_sog(self, start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct):
        if selected_season != 0:
            if is_season_date_correct:
                query = f"""
                SELECT Latitude, Longitude, Cog, Sog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Cog IS NOT NULL
                AND Sog IS not NULL
                AND Sog <> 0
                """
            else:
                query = f"""
                SELECT Latitude, Longitude, Cog, Sog FROM DynamicData
                WHERE Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND MONTH(Datetime) IN {selected_season}
                AND Cog IS NOT NULL
                AND Sog IS not NULL
                AND Sog <> 0
                """
        else:
            query = f"""
                SELECT Latitude, Longitude, Cog, Sog FROM DynamicData
                WHERE Datetime BETWEEN '{start_date}' AND '{end_date}'
                AND Latitude BETWEEN {lat_min} AND {lat_max}
                AND Longitude BETWEEN {lon_min} AND {lon_max}
                AND Cog IS NOT NULL
                AND Sog IS not NULL
                AND Sog <> 0
                """
        try:
            self.cursor.execute(query)
            positions = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")
        return positions

    def get_date(self, date):
        selected_date = date.date()
        day = selected_date.day()
        month = selected_date.month()
        year = selected_date.year()
        return f"{year}-{month}-{day} 00:00:00", f"{year}-{month}-{day}"

    def set_legend(self, legend_array):
        self.clean_layout(self.legend_layout)

        for item in legend_array:

            color = QColor(item[1])
            size = 20
            pixmap = QPixmap(size, size)
            pixmap.fill(QColor(0, 0, 0, 0))

            painter = QPainter(pixmap)
            painter.fillRect(0, 0, size, size, color)
            painter.end()

            color_label = QLabel()
            color_label.setPixmap(pixmap)

            text_label = QLabel(item[0])
            text_label.setStyleSheet("color:black;")
            layout = QHBoxLayout()
            layout.setContentsMargins(25, 25, 0, 0)
            layout.addWidget(color_label)
            layout.addWidget(text_label)
            layout.addStretch()

            self.legend_layout.addLayout(layout)

        color = QColor('purple')
        size_x = 20
        size_y = 2
        pixmap = QPixmap(size_x, size_y)
        pixmap.fill(QColor(0, 0, 0, 0))

        painter = QPainter(pixmap)
        painter.fillRect(0, 0, size_x, size_y, color)
        painter.end()

        color_label = QLabel()
        color_label.setPixmap(pixmap)

        text_label = QLabel("границы государства")
        text_label.setStyleSheet("color:black;")
        layout = QHBoxLayout()
        layout.setContentsMargins(25, 25, 0, 0)
        layout.addWidget(color_label)
        layout.addWidget(text_label)
        layout.addStretch()
        self.legend_layout.addLayout(layout)

    def geo_to_xy(self, lat, lon, LAT0, LON0):
        lat_rad = np.radians(lat)
        lon_rad = np.radians(lon)
        lat0_rad = np.radians(LAT0)
        lon0_rad = np.radians(LON0)
        x = R * np.cos(lat0_rad) * np.sin(lon_rad - lon0_rad)
        y = R * np.sin(lat_rad - lat0_rad)
        return x, y

    def xy_to_geo(self, x, y, LAT0, LON0):
        lat0_rad = np.radians(LAT0)
        lon0_rad = np.radians(LON0)

        lat_rad = np.arcsin(y / R) + lat0_rad

        lon_rad = lon0_rad + np.arctan2(x / (R * np.cos(lat0_rad)), 1)

        lat = np.degrees(lat_rad)
        lon = np.degrees(lon_rad)
        return lat, lon

    def get_color_stab(self, deviation):
        if deviation < 10:
            return 'green'
        elif deviation < 20:
            return 'yellow'
        elif deviation < 30:
            return 'orange'
        elif deviation < 40:
            return 'red'
        else:
            return 'darkred'

    def show_ships(self):
        self.pixmap_label.clear()

        selected_aquatory = self.aquatories_combo2.currentText()
        print("выбранная акватория", selected_aquatory)

        query = f'SELECT right_top_lat, left_bottom_lat, right_top_lon, left_bottom_lon FROM Aquatories WHERE name="{selected_aquatory}"'

        try:
            self.cursor.execute(query)
            aquatory = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")

        if aquatory:
            for (right_top_lat, left_bottom_lat, right_top_lon, left_bottom_lon,) in aquatory:
                lat_min = left_bottom_lat
                lat_max = right_top_lat
                lon_min = left_bottom_lon
                lon_max = right_top_lon

            selected_metric = self.metric_combo.currentText()
            print("выбранная метрика", selected_metric)

            start_date, start_obj = self.get_date(self.date_from)
            end_date, end_obj = self.get_date(self.date_to)
            print("выбранный промежуток времени:", start_date, end_date)

            selected_season = self.season_combo.currentText()
            print("выбранное время года", selected_season)

            seasons = {"-не задано-": 0, "Лето": (6, 7, 8), "Весна": (3, 4, 5), "Осень": (9, 10, 11), "Зима": (1, 2, 12)}

            is_season_date_correct = False

            date_obj_from = datetime.strptime(start_obj, '%Y-%m-%d')
            date_obj_to = datetime.strptime(end_obj, '%Y-%m-%d')
            hours = (date_obj_to - date_obj_from).days * 24

            selected_season = seasons[selected_season]
            if selected_season != 0:
                month_to = date_obj_from.month
                month_from = date_obj_to.month
                if all(month_from <= num for num in selected_season) and all(month_to >= num for num in selected_season):
                    is_season_date_correct = True

            LAT0 = lat_min
            LON0 = lon_min

            m = StaticMap(800, 600)
            marker = CircleMarker((lon_min, lat_min), 'lightblue', 1)
            m.add_marker(marker)
            marker = CircleMarker((lon_max, lat_max), 'lightblue', 1)
            m.add_marker(marker)

            try:

                if selected_metric == 'Интенсивность движения':

                    positions = self.get_ship_positions(start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct)
                    legend_array = [("меньше 1 судна в час", "green"), ("1 и больше судов в час", "yellow")]
                    if positions:

                        GRID_CELL_SIZE_KM = round((lat_max - lat_min), 1) + 0.1

                        grid_cells = defaultdict(list)

                        for lat, lon in positions:
                            x, y = self.geo_to_xy(lat, lon, LAT0, LON0)
                            i = int(x // GRID_CELL_SIZE_KM)
                            j = int(y // GRID_CELL_SIZE_KM)
                            grid_cells[(i, j)].append(1)

                        cell_deviations = {}

                        for cell, num in grid_cells.items():
                            if len(num) > 1:
                                mean = np.sum(num) / hours
                                cell_deviations[cell] = mean
                            else:
                                cell_deviations[cell] = 0

                        for (i, j), num in cell_deviations.items():
                            if num < 1:
                                color = 'green'
                            else:
                                color = 'yellow'

                            x_center = (i + 0.5) * GRID_CELL_SIZE_KM
                            y_center = (j + 0.5) * GRID_CELL_SIZE_KM

                            lat_center, lon_center = self.xy_to_geo(x_center, y_center, LAT0, LON0)

                            marker = CircleMarker((lon_center, lat_center), color, 4)
                            m.add_marker(marker)

                        # for lat, lon in positions:
                        #     marker = CircleMarker((lon, lat), 'red', 1)
                        #     m.add_marker(marker)

                        image = m.render()

                        image.save('static_map.png')
                        pixmap = QPixmap('static_map.png')
                        self.pixmap_label.setPixmap(pixmap)

                        self.set_legend(legend_array)
                    else:
                        self.pixmap_label.setText("Для выбранной акватории недостаточно данных")
                elif selected_metric == 'Скорость судов':
                    positions = self.get_ship_speed(start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct)
                    legend_array = [("скорость < 10 узлов", "green"), ("скорость 10-20 узлов", "yellow"), ("скорость 20-30 узлов", "moccasin"), ("скорость 30-40 узлов", "orange"), ("скорость 40-50 узлов", "salmon"), ("скорость 50-60 узлов", "red"), ("скорость > 60 узлов", "darkred")]
                    if positions:

                        GRID_CELL_SIZE_KM = round((lat_max - lat_min), 1) + 0.2

                        grid_cells = defaultdict(list)

                        for lat, lon, sog in positions:
                            x, y = self.geo_to_xy(lat, lon, LAT0, LON0)
                            i = int(x // GRID_CELL_SIZE_KM)
                            j = int(y // GRID_CELL_SIZE_KM)
                            grid_cells[(i, j)].append(sog)

                        cell_deviations = {}

                        for cell, sogs in grid_cells.items():
                            if len(sogs) > 1:
                                mean_sog = np.mean(sogs)
                                cell_deviations[cell] = mean_sog
                            else:
                                cell_deviations[cell] = 0

                        for (i, j), sog in cell_deviations.items():
                            if sog < 10.0:
                                color = 'green'
                            elif sog <= 20.0:
                                color = 'yellow'
                            elif sog <= 30.0:
                                color = 'moccasin'
                            elif sog <= 40.0:
                                color = 'orange'
                            elif sog <= 50.0:
                                color = 'salmon'
                            elif sog <= 60.0:
                                color = 'red'
                            else:
                                color = 'darkred'

                            x_center = (i + 0.5) * GRID_CELL_SIZE_KM
                            y_center = (j + 0.5) * GRID_CELL_SIZE_KM

                            lat_center, lon_center = self.xy_to_geo(x_center, y_center, LAT0, LON0)

                            marker = CircleMarker((lon_center, lat_center), color, 4)
                            m.add_marker(marker)

                        image = m.render()

                        image.save('static_map.png')
                        pixmap = QPixmap('static_map.png')
                        self.pixmap_label.setPixmap(pixmap)

                        self.set_legend(legend_array)
                    else:
                        self.pixmap_label.setText("Для выбранной акватории недостаточно данных")
                elif selected_metric == 'Размер судов':
                    positions = self.get_ship_size(start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct)
                    legend_array = [("длина < 100 метров", "green"), ("длина 100-200 метров", "yellow"), ("длина 200-300 метров", "orange"), ("длина 300-400 метров", "red"), ("длина > 400 метров", "darkred")]
                    if positions:

                        GRID_CELL_SIZE_KM = round((lat_max - lat_min), 1) + 0.2

                        grid_cells = defaultdict(list)

                        for lat, lon, size in positions:
                            x, y = self.geo_to_xy(lat, lon, LAT0, LON0)
                            i = int(x // GRID_CELL_SIZE_KM)
                            j = int(y // GRID_CELL_SIZE_KM)
                            grid_cells[(i, j)].append(size)

                        cell_deviations = {}

                        for cell, size in grid_cells.items():
                            if len(size) > 1:
                                mean_size = np.mean(size)
                                cell_deviations[cell] = mean_size
                            else:
                                cell_deviations[cell] = 0

                        for (i, j), size in cell_deviations.items():
                            if size < 100.0:
                                color = 'green'
                            elif size <= 200.0:
                                color = 'yellow'
                            elif size <= 300.0:
                                color = 'orange'
                            elif size <= 400.0:
                                color = 'red'
                            else:
                                color = 'darkred'

                            x_center = (i + 0.5) * GRID_CELL_SIZE_KM
                            y_center = (j + 0.5) * GRID_CELL_SIZE_KM

                            lat_center, lon_center = self.xy_to_geo(x_center, y_center, LAT0, LON0)

                            marker = CircleMarker((lon_center, lat_center), color, 4)
                            m.add_marker(marker)

                        image = m.render()

                        image.save('static_map.png')
                        pixmap = QPixmap('static_map.png')
                        self.pixmap_label.setPixmap(pixmap)

                        self.set_legend(legend_array)
                    else:
                        self.pixmap_label.setText("Для выбранной акватории недостаточно данных")
                elif selected_metric == "Стабильность параметров движения":
                    positions = self.get_ship_course(start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct)
                    legend_array = [("отклонение курсов < 10 градусов", "green"), ("отклонение курсов 10-20 градусов", "yellow"), ("отклонение курсов 20-30 градусов", "orange"), ("отклонение курсов 30-40 градусов", "red"), ("отклонение курсов > 40 градусов", "darkred")]
                    if positions:
                        GRID_CELL_SIZE_KM = round((lat_max - lat_min), 1) + 0.2

                        grid_cells = defaultdict(list)
                        for lat, lon, cog in positions:
                            x, y = self.geo_to_xy(lat, lon, LAT0, LON0)
                            i = int(x // GRID_CELL_SIZE_KM)
                            j = int(y // GRID_CELL_SIZE_KM)
                            grid_cells[(i, j)].append(cog)

                        cell_deviations = {}

                        for cell, cogs in grid_cells.items():
                            if len(cogs) > 1:
                                mean_cog = np.mean(cogs)
                                std_cog = np.sqrt(np.mean((np.array(cogs) - mean_cog)**2))
                                cell_deviations[cell] = std_cog
                            else:
                                cell_deviations[cell] = 0

                        for (i, j), deviation in cell_deviations.items():
                            color = self.get_color_stab(deviation)

                            x_center = (i + 0.5) * GRID_CELL_SIZE_KM
                            y_center = (j + 0.5) * GRID_CELL_SIZE_KM

                            lat_center, lon_center = self.xy_to_geo(x_center, y_center, LAT0, LON0)

                            marker = CircleMarker((lon_center, lat_center), color, 5)
                            m.add_marker(marker)

                        image = m.render()

                        image.save('static_map.png')
                        pixmap = QPixmap('static_map.png')
                        self.pixmap_label.setPixmap(pixmap)

                        self.set_legend(legend_array)
                    else:
                        self.pixmap_label.setText("Для выбранной акватории недостаточно данных")
                elif selected_metric == "Насыщенность трафика":
                    positions = self.get_ships_cog_sog(start_date, end_date, lat_min, lat_max, lon_min, lon_max, selected_season, is_season_date_correct)
                    legend_array = [("доля опасных скоростей и курсов < 50%", "green"), ("доля опасных скоростей и курсов 50-80%", "yellow"), ("доля опасных скоростей и курсов 80-90%", "orange"), ("доля опасных скоростей и курсов 90-100%", "red")]
                    if positions:
                        GRID_CELL_SIZE_KM = round((lat_max - lat_min), 1) + 0.2

                        ships_data = []

                        for lat, lon, cog_deg, speed_knots in positions:
                            x, y = self.geo_to_xy(lat, lon, LAT0, LON0)

                            cog_rad = np.radians(cog_deg)

                            speed_km_per_h = float(speed_knots) * 1.852
                            vx = speed_km_per_h * np.cos(cog_rad) / 3600
                            vy = speed_km_per_h * np.sin(cog_rad) / 3600

                            ships_data.append({'x': x, 'y': y, 'vx': vx, 'vy': vy})

                        def compute_cpa(ship1, ship2):
                            dx0 = ship2['x'] - ship1['x']
                            dy0 = ship2['y'] - ship1['y']

                            dvx = ship2['vx'] - ship1['vx']
                            dvy = ship2['vy'] - ship1['vy']

                            denom = dvx**2 + dvy**2

                            if denom == 0:
                                t_cpa = 0
                                r_cpa = np.sqrt(dx0**2 + dy0**2)
                                return t_cpa, r_cpa

                            t_cpa = -(dx0 * dvx + dy0 * dvy) / denom

                            if t_cpa < 0:
                                t_cpa = 0
                            dx_t = dx0 + dvx * t_cpa
                            dy_t = dy0 + dvy * t_cpa
                            r_t_cpa = np.sqrt(dx_t**2 + dy_t**2)

                            return t_cpa, r_t_cpa

                        min_distance_overall = float('inf')
                        dangerous_pairs_info = []

                        for i in range(len(ships_data)):
                            for j in range(i+1, len(ships_data)):
                                ship1 = ships_data[i]
                                ship2 = ships_data[j]

                                t_cpa, r_cpa = compute_cpa(ship1, ship2)

                                if r_cpa < min_distance_overall:
                                    min_distance_overall = r_cpa

                                if r_cpa < MIN_SAFE_DISTANCE:
                                    dangerous_pairs_info.append({
                                        'ship1_index': i,
                                        'ship2_index': j,
                                        'dangerous': True
                                    })
                                else:
                                    dangerous_pairs_info.append({
                                        'ship1_index': i,
                                        'ship2_index': j,
                                        'dangerous': False
                                    })

                        grid_cells = defaultdict(list)

                        for info in dangerous_pairs_info:
                            coordinates = ships_data[info['ship1_index']]
                            x, y = coordinates['x'], coordinates['y']
                            i = int(x // GRID_CELL_SIZE_KM)
                            j = int(y // GRID_CELL_SIZE_KM)
                            grid_cells[(i, j)].append(info['dangerous'])

                        cell_deviations = {}

                        for cell, dangs in grid_cells.items():
                            if len(dangs) > 1:
                                fraction_true = sum(dangs) / len(dangs)
                                cell_deviations[cell] = fraction_true
                            else:
                                cell_deviations[cell] = 0

                        for (i, j), deviation in cell_deviations.items():
                            if deviation < 0.5:
                                color = 'green'
                            elif deviation < 0.8:
                                color = 'yellow'
                            elif deviation < 0.8:
                                color = 'orange'
                            elif deviation > 0.9:
                                color = 'red'

                            x_center = (i + 0.5) * GRID_CELL_SIZE_KM
                            y_center = (j + 0.5) * GRID_CELL_SIZE_KM

                            lat_center, lon_center = self.xy_to_geo(x_center, y_center, LAT0, LON0)

                            marker = CircleMarker((lon_center, lat_center), color, 4)
                            m.add_marker(marker)

                        image = m.render()

                        image.save('static_map.png')
                        pixmap = QPixmap('static_map.png')
                        self.pixmap_label.setPixmap(pixmap)

                        self.set_legend(legend_array)
                    else:
                        self.pixmap_label.setText("Для выбранной акватории недостаточно данных")

            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Произошла ошибка:\n{e}")

    def save_image(self):
        try:
            source_path = f'{CURRENT_DIRECTORY}/static_map.png'

            options = QFileDialog.Options()
            save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Сохранить изображение как",
                "Метрика",
                "PNG Files (*.png);;All Files (*)",
                options=options
            )
            if save_path:
                shutil.copyfile(source_path, save_path)
                QMessageBox.information(self, "Успех", "Данные успешно сохранены!")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Произошла ошибка:\n{e}")

    def save_csv(self):
        try:
            selected_aquatory = self.aquatories_combo.currentText()
            print("выбранная акватория", selected_aquatory)
            query = f'SELECT * FROM Aquatories WHERE name="{selected_aquatory}"'
            self.cursor.execute(query)
            aquatory = self.cursor.fetchall()
            for row in aquatory:
                min_lat = row[3]
                max_lat = row[2]
                min_lon = row[5]
                max_lon = row[4]
            query = f'''SELECT DISTINCT DynamicData.Datetime, DynamicData.UserID, Cog, Sog, Latitude, Longitude, A, B, C, D, ImoNumber, Type
                    FROM DynamicData
                    left JOIN StaticData ON DynamicData.UserID = StaticData.UserID
                    WHERE Latitude BETWEEN {min_lat} AND {max_lat}
                    AND Longitude BETWEEN {min_lon} AND {max_lon}'''
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            columns = [desc[0] for desc in self.cursor.description]

            options = QFileDialog.Options()
            filename, _ = QFileDialog.getSaveFileName(self, "Сохранить CSV", selected_aquatory, "CSV Files (*.csv)", options=options)
            if filename:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)

                QMessageBox.information(self, "Успех", "Данные успешно сохранены!")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Произошла ошибка:\n{e}")

    def setup_tab2(self):
        mainlayout = QHBoxLayout()
        self.analyz = QLabel("Параметры анализа данных")
        self.aquatories_lable = QLabel("Акватория")
        self.aquatories_lable.setStyleSheet("font-size:20px; margin:20px; color:black;")
        aquatories_layout = QHBoxLayout()
        aquatories_layout.addWidget(self.aquatories_lable)
        self.aquatories_combo2 = QComboBox()
        self.aquatories_combo2.setStyleSheet(self.combo_style)

        query = 'SELECT name FROM Aquatories'
        try:
            self.cursor.execute(query)
            aquatories = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")

        if aquatories:
            for (aquatory_name,) in aquatories:
                self.aquatories_combo2.addItem(aquatory_name)

            aquatories_layout.addWidget(self.aquatories_combo2)

        self.metric_lable = QLabel("Метрика")
        self.metric_lable.setStyleSheet("font-size:20px; margin:20px; color:black;")
        metric_layout = QHBoxLayout()
        metric_layout.addWidget(self.metric_lable)
        self.metric_combo = QComboBox()
        self.metric_combo.setStyleSheet(self.combo_style)

        metrics = ["Стабильность параметров движения", "Насыщенность трафика", "Скорость судов", "Размер судов", "Интенсивность движения"]

        for metric in metrics:
            self.metric_combo.addItem(metric)

        metric_layout.addWidget(self.metric_combo)

        self.time_lable = QLabel("Период времени")
        self.time_lable.setStyleSheet("font-size:20px; margin:20px; color:black;")
        time_layout = QHBoxLayout()
        time_layout.addWidget(self.time_lable)

        time_lable_from = QLabel("От")
        time_lable_from.setStyleSheet("font-size:20px; margin:20px; color:black;")
        self.date_from = QDateEdit()
        self.date_from.setStyleSheet(self.date_style)
        self.date_from.setDate(QDate.currentDate())
        self.date_from.setDisplayFormat('dd.MM.yyyy')

        time_lable_to = QLabel("До")
        time_lable_to.setStyleSheet("font-size:20px; margin:20px; color:black;")
        self.date_to = QDateEdit()
        self.date_to.setStyleSheet(self.date_style)
        self.date_to.setDate(QDate.currentDate())
        self.date_to.setDisplayFormat('dd.MM.yyyy')

        time_layout.addStretch()
        time_layout.addWidget(time_lable_from)
        time_layout.addWidget(self.date_from)
        time_layout.addWidget(time_lable_to)
        time_layout.addWidget(self.date_to)

        self.season_lable = QLabel("Время года")
        self.season_lable.setStyleSheet("font-size:20px; margin:20px; color:black;")
        season_layout = QHBoxLayout()
        season_layout.addWidget(self.season_lable)
        self.season_combo = QComboBox()
        self.season_combo.setStyleSheet(self.combo_style)

        seasons = ["-не задано-", "Лето", "Весна", "Осень", "Зима"]

        for season in seasons:
            self.season_combo.addItem(season)

        season_layout.addWidget(self.season_combo)

        self.legend_layout = QVBoxLayout()

        analyz_layout = QVBoxLayout()
        analyz_layout.addLayout(aquatories_layout)
        analyz_layout.addLayout(metric_layout)
        analyz_layout.addLayout(time_layout)
        analyz_layout.addLayout(season_layout)

        analyz_layout.addLayout(self.legend_layout)
        analyz_layout.addStretch()

        self.result_layout = QVBoxLayout()
        self.result_button = QPushButton("Посчитать метрику")
        self.result_button.setStyleSheet(self.button_style)
        self.download_metric_button = QPushButton("Скачать метрику в формате .png")
        self.download_metric_button.setStyleSheet(self.button_style)
        self.result_button.clicked.connect(self.show_ships)
        self.download_metric_button.clicked.connect(self.save_image)

        self.result_area = QVBoxLayout()
        self.pixmap_label = QLabel()
        self.pixmap_label.setStyleSheet("font-size:20px; margin:20px; color:black;")
        self.result_area.addWidget(self.pixmap_label)

        self.result_layout.addLayout(self.result_area)
        self.result_layout.addStretch()
        self.result_layout.addWidget(self.result_button)
        self.result_layout.addWidget(self.download_metric_button)
        mainlayout.addLayout(analyz_layout)
        mainlayout.addLayout(self.result_layout)
        mainlayout.setStretch(0, 1)
        mainlayout.setStretch(1, 2)
        # mainlayout.addStretch()
        self.tab2.setLayout(mainlayout)

    def start_collect(self):
        self.text_edit.setPlainText("Собранные данные:")
        selected_aquatory = self.aquatories_combo.currentText()
        print("выбранная акватория", selected_aquatory)

        selected_items = []

        for index in range(self.list_widget.count()):
            item = self.list_widget.item(index)
            if item.checkState() == Qt.Checked:
                selected_items.append(item.text())

        print("Выбранные элементы:", selected_items)

        current_value = self.double_spin_box.value()
        print("Текущее значение:", current_value)

        if self.thread is not None and self.thread.isRunning():
            self.stop_worker()

        self.button_start.setStyleSheet(self.button_style_clicked)
        print("start")
        self.worker = Worker(param1=selected_aquatory, param2=selected_items, param3=current_value, button=self.button_start)
        self.thread = QThread()

        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.thread.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.update_signal.connect(self.update_text)
        self.thread.start()

    def stop_worker(self):
        if self.thread is not None and self.thread.isRunning():
            self.thread.quit()
            self.thread.wait()

    def update_text(self, text):
        self.text_edit.append(text)

    def stop_collect(self):
        print("stop")
        self.button_start.setStyleSheet(self.button_style)
        stop_long_running_function()

    def setup_tab1(self):
        # выбор акватории

        self.aquatories_lable = QLabel("Акватория")
        self.aquatories_lable.setStyleSheet("font-size:20px; margin:20px;")
        aquatories_layout = QVBoxLayout()
        aquatories_layout.addWidget(self.aquatories_lable)

        self.aquatories_combo = QComboBox()
        self.aquatories_combo.setStyleSheet("""
            QComboBox {
                background-color: white;
                color: black;
                border: 1px solid gray;
                border-radius: 5px;
                padding: 5px;
                font-size: 20px;
                margin:20px;
                width: 250px;
            }
            QComboBox:hover {
                background-color: #007AFF;
                color: white;
            }
            QComboBox::drop-down {
                border-left: 1px solid gray; /* Граница выпадающего списка */
            }
            QComboBox::down-arrow {
                image: url(arrow.png); /* Замените на путь к вашей стрелке */
            }
            QComboBox QAbstractItemView {
                background-color: white; /* Цвет фона выпадающего списка */
                color: black; /* Цвет текста выпадающего списка */
            }
            QComboBox QAbstractItemView::item {
                background-color: white; /* Цвет фона элемента по умолчанию */
                color: black; /* Цвет текста элемента по умолчанию */
            }
            QComboBox QAbstractItemView::item:hover {
                background-color: #007AFF; /* Цвет фона элемента при наведении */
                color: white; /* Цвет текста элемента при наведении */
            }
        """)

        query = 'SELECT name FROM Aquatories'

        try:
            self.cursor.execute(query)
            aquatories = self.cursor.fetchall()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка загрузки данных: {str(e)}")

        if aquatories:
            for (aquatory_name,) in aquatories:
                self.aquatories_combo.addItem(aquatory_name)

            aquatories_layout.addWidget(self.aquatories_combo)
        aquatories_layout.addStretch()

        # выбор данных

        self.collected_data_lable = QLabel("Данные")
        self.collected_data_lable.setStyleSheet("font-size:20px; margin:20px;")
        check_daya_layout = QVBoxLayout()
        check_daya_layout.addWidget(self.collected_data_lable)

        self.list_widget = QListWidget(self)
        self.list_widget.setStyleSheet("border:none; font-size: 20px; margin-left: 10px; width: 100px")
        check_daya_layout.addWidget(self.list_widget)

        items = ["Скорость судна", "Курс судна", "Тип судна", "Размер судна"]
        for item in items:
            list_item = QListWidgetItem(item)
            list_item.setFlags(list_item.flags() | Qt.ItemIsUserCheckable)
            list_item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(list_item)

        # выбор времени
        self.time_lable = QLabel("Период сбора (в часах)")
        self.time_lable.setStyleSheet("font-size:20px; margin:20px 0px;")
        time_layout = QVBoxLayout()
        time_layout.addWidget(self.time_lable)

        self.double_spin_box = QDoubleSpinBox(self)

        self.double_spin_box.setFixedWidth(250)
        self.double_spin_box.setStyleSheet("""
            QDoubleSpinBox {
                background-color: #f0f0f0;
                border: 1px solid black;
                border-radius: 5px;
                padding: 5px;
                }
            """)
        self.double_spin_box.setRange(0.01, 48.0)
        self.double_spin_box.setDecimals(2)
        self.double_spin_box.setValue(0.1)

        time_layout.addWidget(self.double_spin_box)

        self.time_help_label = QLabel("Период сбора не должен \nпревышать 48 часов.")
        self.time_help_label.setStyleSheet("font-size:20px; color:grey;width: 250px;")

        time_layout.addWidget(self.time_help_label)
        time_layout.addStretch()

        # кнопки
        button_layout = QVBoxLayout()

        self.button_start = QPushButton("Начать сбор")

        self.button_start.setStyleSheet(self.button_style)
        self.button_stop = QPushButton("Остановить сбор")
        self.button_stop.setStyleSheet(self.button_style)
        self.button_download_table = QPushButton("Скачать данные в формате .csv")
        self.button_download_table.setStyleSheet(self.button_style)

        self.button_start.clicked.connect(self.start_collect)
        self.button_stop.clicked.connect(self.stop_collect)
        self.button_download_table.clicked.connect(self.save_csv)
        button_layout.addWidget(self.button_start)
        button_layout.addWidget(self.button_stop)
        button_layout.addWidget(self.button_download_table)

        button_layout.addStretch()

        # top_layout
        top_layout = QHBoxLayout()

        top_layout.addLayout(aquatories_layout)
        top_layout.addLayout(check_daya_layout)
        top_layout.addLayout(time_layout)
        top_layout.addLayout(button_layout)

        top_layout.setStretch(0, 1)
        top_layout.setStretch(1, 1)
        top_layout.setStretch(2, 1)
        top_layout.setStretch(3, 1)

        # bottom_layout
        bottom_layout = QHBoxLayout()

        self.text_edit = QTextEdit(self)
        self.text_edit.setReadOnly(True)
        self.text_edit.setPlainText("Собранные данные:")

        bottom_layout.addWidget(self.text_edit)

        # main_layout
        main_layout = QVBoxLayout()

        main_layout.addLayout(top_layout)
        main_layout.addLayout(bottom_layout)

        self.tab1.setLayout(main_layout)


if __name__ == "__main__":
    app = QApplication(sys.argv)

    with open('config.json', 'r', encoding='utf-8') as file:
        config = json.load(file)

    window = MainWindow(config)
    window.show()

    sys.exit(app.exec_())
