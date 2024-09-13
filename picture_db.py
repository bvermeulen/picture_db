from enum import Enum
import shutil
import datetime
import io
import json
import os
import re
import psutil
from dataclasses import dataclass
from decouple import config
from functools import wraps
from shapely.geometry import Point
import psycopg2
from geopy.geocoders import Nominatim
from picture_exif import Exif
from Utils.plogger import Logger

logformat = "%(asctime)s:%(levelname)s:%(message)s"
Logger.set_logger(".\\picture.log", logformat, "INFO")
logger = Logger.getlogger()


class DbFilter(Enum):
    NOGPS = 1
    CHECKED = 2
    NOT_CHECKED = 3
    ALL = 4


geolocator = Nominatim(user_agent="picture_db")
EPSG_WGS84 = 4326
exif = Exif()


@dataclass
class PicturesTable:
    id: int
    date_picture: datetime.datetime
    md5_signature: str
    camera_make: str
    camera_model: str
    gps_latitude: dict
    gps_longitude: dict
    gps_altitude: dict
    gps_img_direction: dict
    thumbnail: str
    exif: dict
    rotate: int
    rotate_checked: bool


@dataclass
class FilesTable:
    id: int
    picture_id: int
    file_path: str
    file_name: str
    file_modified: datetime.datetime
    file_created: datetime.datetime
    file_size: int
    file_checked: bool


@dataclass
class InfoTable:
    country: str
    state: str
    city: str
    suburb: str
    road: str


def progress_message_generator(message):
    loop_dash = ["\u2014", "\\", "|", "/"]
    i = 1
    print_interval = 1
    while True:
        print(f"\r{loop_dash[int(i/print_interval) % 4]} {i} {message}", end="")
        i += 1
        yield


class DbUtils:
    """utility methods for database"""

    host = config("DB_HOST")
    port = config("PORT")
    db_user = config("DB_USERNAME")
    db_user_pw = config("DB_PASSWORD")
    database = config("DATABASE")

    @classmethod
    def connect(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            connect_string = (
                f"host='{cls.host}' dbname='{cls.database}'"
                f"user='{cls.db_user}' password='{cls.db_user_pw}'"
            )
            result = None
            try:
                # add ggsencmode='disable' to resolve unsupported frontend protocol
                # 1234.5679: server supports 2.0 to 3.0
                # should be fixed on postgresql 12.3
                connection = psycopg2.connect(connect_string, gssencmode="disable")
                cursor = connection.cursor()
                result = func(*args, cursor, **kwargs)
                connection.commit()

            except psycopg2.Error as error:
                print(f"error while connect to PostgreSQL {cls.database}: " f"{error}")

            finally:
                if connection:
                    cursor.close()
                    connection.close()

            return result

        return wrapper

    @staticmethod
    def get_answer(choices):
        """arguments:
        choices: list of picture numbers to be able to delete

        returns a list, either:
        [1, 2, ..., n] : choices of pictures to delete
        [0]  : exit function
        [-1] : skip item
        """
        answer_delete = []
        while not (
            any(val in [-1, 0] for val in answer_delete) and len(answer_delete) == 1
        ) and (not any(val in choices for val in answer_delete)):
            _answer = input(
                "Delete picture numbers [separated by spaces] "
                "(press 0 to quit, space to skip): "
            )

            if _answer == " ":
                _answer = "-1"

            answer_delete = _answer.replace(",", " ").split()
            try:
                answer_delete = [int(val) for val in _answer.replace(",", " ").split()]

            except ValueError:
                pass

        return answer_delete

    @staticmethod
    def get_name():
        valid = False
        while not valid:
            name = input("Please give your name: ")
            if 5 < len(name) < 20:
                valid = True

        return name

    @staticmethod
    def remove_display():
        """remove thumbnail picture by killing the display process"""
        for proc in psutil.process_iter():
            if proc.name() == exif.get_display_process():
                proc.kill()


class PictureDb:
    table_pictures = "pictures"
    table_files = "files"
    table_reviews = "reviews"
    table_locations = "locations"

    @classmethod
    @DbUtils.connect
    def delete_table(cls, table_name: str, cursor):
        sql_string = f"DROP TABLE {table_name};"
        cursor.execute(sql_string)
        print(f"delete table {table_name}")

    @classmethod
    @DbUtils.connect
    def create_pictures_table(cls, cursor):
        sql_string = (
            f"CREATE TABLE {cls.table_pictures} ("
            f"id SERIAL PRIMARY KEY, "
            f"date_picture TIMESTAMP, "
            f"md5_signature VARCHAR(32), "
            f"camera_make VARCHAR(50), "
            f"camera_model VARCHAR(50), "
            f"gps_latitude JSON, "
            f"gps_longitude JSON, "
            f"gps_altitude JSON, "
            f"gps_img_dir JSON, "
            f"thumbnail JSON, "
            f"exif JSON, "
            f"rotate INTEGER DEFAULT 0, "
            f"rotate_checked BOOLEAN DEFAULT FALSE"
            f");"
        )
        print(f"create table {cls.table_pictures}")
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_files_table(cls, cursor):
        sql_string = (
            f"CREATE TABLE {cls.table_files} ("
            f"id SERIAL PRIMARY KEY, "
            f"picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE UNIQUE NOT NULL, "
            f"file_path VARCHAR(250), "
            f"file_name VARCHAR(250), "
            f"file_modified TIMESTAMP, "
            f"file_created TIMESTAMP, "
            f"file_size INTEGER, "
            f"file_checked BOOLEAN"
            f");"
        )
        print(f"create table {cls.table_files}")
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_reviews_table(cls, cursor):
        sql_string = (
            f"CREATE TABLE {cls.table_reviews} ("
            f"id SERIAL PRIMARY KEY, "
            f"picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE NOT NULL, "
            f"reviewer_name VARCHAR(20), "
            f"review_date TIMESTAMP"
            f");"
        )
        print(f"create table {cls.table_reviews}")
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_locations_table(cls, cursor):
        sql_string = (
            f"CREATE TABLE {cls.table_locations} ("
            f"id SERIAL PRIMARY KEY, "
            f"picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE UNIQUE, "
            f"latitude DOUBLE PRECISION NOT NULL, "
            f"longitude DOUBLE PRECISION NOT NULL, "
            f"altitude REAL NOT NULL, "
            f"geolocation_info JSON, "
            f"geom geometry(Point, {EPSG_WGS84})"
            f");"
        )
        print(f"create table {cls.table_locations}")
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def store_pictures_base_folder(cls, base_folder, cursor):
        """re-initialises the database all previous data will be lost"""
        progress_message = progress_message_generator(
            f"loading picture meta data from {base_folder}"
        )

        sql_pictures = (
            f"INSERT INTO {cls.table_pictures} ("
            f"date_picture, md5_signature, camera_make, camera_model, "
            f"gps_latitude, gps_longitude, gps_altitude, gps_img_dir, "
            f"thumbnail, exif, rotate, rotate_checked) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"RETURNING id;"
        )

        sql_files = (
            f"INSERT INTO {cls.table_files} ("
            f"picture_id, file_path, file_name, file_modified, file_created, "
            f"file_size, file_checked)"
            f"VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:
                pic_meta, file_meta = exif.distill_serialized_picfile_meta_data(
                    os.path.join(foldername, filename)
                )
                if not file_meta.file_name:
                    continue

                cursor.execute(
                    sql_pictures,
                    (
                        pic_meta.date_picture,
                        pic_meta.md5_signature,
                        pic_meta.camera_make,
                        pic_meta.camera_model,
                        pic_meta.gps_latitude,
                        pic_meta.gps_longitude,
                        pic_meta.gps_altitude,
                        pic_meta.gps_img_direction,
                        pic_meta.thumbnail,
                        pic_meta.exif,
                        0,
                        False,
                    ),
                )
                picture_id = cursor.fetchone()[0]

                cursor.execute(
                    sql_files,
                    (
                        picture_id,
                        file_meta.file_path,
                        file_meta.file_name,
                        file_meta.file_modified,
                        file_meta.file_created,
                        file_meta.file_size,
                        True,
                    ),
                )
                lat_lon_str, lat_lon_val = exif.convert_gps(
                    pic_meta.gps_latitude, pic_meta.gps_longitude, pic_meta.gps_altitude
                )
                if lat_lon_str:
                    cls.add_to_locations_table(
                        picture_id, pic_meta.date_picture, lat_lon_val
                    )

                next(progress_message)
        print()

    @classmethod
    @DbUtils.connect
    def check_and_add_files(cls, base_folder, cursor):
        """check if files are in database, if they are not then add"""
        progress_message = progress_message_generator(
            f"update picture meta data from {base_folder}"
        )
        sql_string = f"UPDATE {cls.table_files} SET file_checked = FALSE;"
        cursor.execute(sql_string)

        sql_pictures = (
            f"INSERT INTO {cls.table_pictures} ("
            f"date_picture, md5_signature, camera_make, camera_model, "
            f"gps_latitude, gps_longitude, gps_altitude, gps_img_dir, "
            f"thumbnail, exif, rotate, rotate_checked) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            f"RETURNING id;"
        )

        sql_files = (
            f"INSERT INTO {cls.table_files} ("
            f"picture_id, file_path, file_name, file_modified, file_created, "
            f"file_size, file_checked) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s);"
        )

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:
                valid_name = filename[-4:].lower() in [".jpg", ".png"] or filename[
                    -5:
                ].lower() in [".jpeg", ".heic"]
                if valid_name:
                    sql_filename = filename.replace("'", "''")
                    sql_parent_folder = os.path.basename(
                        os.path.dirname("/".join([foldername, sql_filename]))
                    )
                    sql_parent_folder = sql_parent_folder.replace("'", "''")

                    sql_string = (
                        f"SELECT picture_id FROM {cls.table_files} WHERE "
                        f"file_path like '%{sql_parent_folder}%' AND "
                        f"file_name='{sql_filename}';"
                    )
                    cursor.execute(sql_string)
                    try:
                        picture_id = cursor.fetchone()[0]

                    except TypeError:
                        picture_id = None

                    # file exists but not in DB -> add to DB
                    if not picture_id:
                        pic_meta, file_meta = exif.distill_serialized_picfile_meta_data(
                            os.path.join(foldername, filename)
                        )
                        if not file_meta.file_name:
                            continue

                        cursor.execute(
                            sql_pictures,
                            (
                                pic_meta.date_picture,
                                pic_meta.md5_signature,
                                pic_meta.camera_make,
                                pic_meta.camera_model,
                                pic_meta.gps_latitude,
                                pic_meta.gps_longitude,
                                pic_meta.gps_altitude,
                                pic_meta.gps_img_direction,
                                pic_meta.thumbnail,
                                pic_meta.exif,
                                0,
                                False,
                            ),
                        )
                        picture_id = cursor.fetchone()[0]

                        cursor.execute(
                            sql_files,
                            (
                                picture_id,
                                file_meta.file_path,
                                file_meta.file_name,
                                file_meta.file_modified,
                                file_meta.file_created,
                                file_meta.file_size,
                                True,
                            ),
                        )
                        lat_lon_str, lat_lon_val = exif.convert_gps(
                            pic_meta.gps_latitude,
                            pic_meta.gps_longitude,
                            pic_meta.gps_altitude,
                        )
                        if lat_lon_str:
                            cls.add_to_locations_table(
                                picture_id, pic_meta.date_picture, lat_lon_val
                            )
                    else:
                        sql_string = (
                            f"UPDATE {cls.table_files} "
                            f"SET file_checked = TRUE "
                            f"WHERE picture_id={picture_id};"
                        )
                        cursor.execute(sql_string)

                    next(progress_message)

        print()

    @classmethod
    @DbUtils.connect
    def load_picture_meta(cls, _id: int, cursor):
        """load picture meta data from the database
        :arguments:
            _id: picture id number in database: integer
        :returns:
            im: PIL image
            pic_meta: PicturesTable
            file_meta: FilesTable
            info_meta: InfoTable
            lat_lon_str: string
            lat_lon_val: tuple(float, float, float)
        """
        empty_return = None, None, None, None, None, (None, None, None)
        sql_string = f"SELECT * FROM {cls.table_pictures} WHERE id=%s;"
        cursor.execute(sql_string, (_id,))
        data_from_table_pictures = cursor.fetchone()

        if not data_from_table_pictures:
            return empty_return

        sql_string = f"SELECT * FROM {cls.table_files} WHERE picture_id=%s;"
        cursor.execute(sql_string, (_id,))
        data_from_table_files = cursor.fetchone()
        if not data_from_table_files:
            return empty_return

        pic_meta = PicturesTable(
            id=data_from_table_pictures[0],
            date_picture=data_from_table_pictures[1],
            md5_signature=data_from_table_pictures[2],
            camera_make=data_from_table_pictures[3],
            camera_model=data_from_table_pictures[4],
            gps_latitude=data_from_table_pictures[5],
            gps_longitude=data_from_table_pictures[6],
            gps_altitude=data_from_table_pictures[7],
            gps_img_direction=data_from_table_pictures[8],
            thumbnail=data_from_table_pictures[9],
            exif=data_from_table_pictures[10],
            rotate=data_from_table_pictures[11],
            rotate_checked=data_from_table_pictures[12],
        )
        file_meta = FilesTable(
            id=data_from_table_files[0],
            picture_id=data_from_table_files[1],
            file_path=data_from_table_files[2],
            file_name=data_from_table_files[3],
            file_modified=data_from_table_files[4],
            file_created=data_from_table_files[5],
            file_size=data_from_table_files[6],
            file_checked=data_from_table_files[7],
        )
        assert (
            pic_meta.id == file_meta.picture_id
        ), "load_picture_meta: database integrity error"

        sql_string = (
            f"SELECT geolocation_info FROM {cls.table_locations} WHERE picture_id=%s"
        )
        cursor.execute(sql_string, (_id,))
        if geolocation_info := cursor.fetchone():
            geolocation_info = geolocation_info[0]
            info_meta = InfoTable(
                country=geolocation_info.get("country", ""),
                state=", ".join(
                    v
                    for v in [
                        geolocation_info.get(k, "") for k in ["state", "province"]
                    ]
                    if v
                ),
                city=", ".join(
                    v
                    for v in [
                        geolocation_info.get(k, "")
                        for k in ["city", "municipality", "town", "village"]
                    ]
                    if v
                ),
                suburb=geolocation_info.get("suburb", ""),
                road=geolocation_info.get("road", ""),
            )
        else:
            info_meta = InfoTable(country="", state="", city="", suburb="", road="")

        im = None
        if pic_meta.thumbnail:
            img_bytes = io.BytesIO(pic_meta.thumbnail.encode(exif.codec))
            im = exif.get_pil_image(img_bytes)

        lat_lon_str, lat_lon_val = exif.convert_gps(
            pic_meta.gps_latitude, pic_meta.gps_longitude, pic_meta.gps_altitude
        )
        return im, pic_meta, file_meta, info_meta, lat_lon_str, lat_lon_val

    @classmethod
    @DbUtils.connect
    def select_pics_for_merge(cls, source_folder, destination_folder, cursor):
        """method that checks if picture is in the database. If it is
        not moves picture from source folder to the destination folder
        """
        progress_message = progress_message_generator(
            f"merging pictures from {source_folder}"
        )
        log_file = os.path.join(source_folder, "_select_pictures_to_merge.log")
        with open(log_file, "at") as f:
            c_time = datetime.datetime.now()
            f.write(f"===> Select pictures to merge: {c_time}\n")

        log_lines = []
        for foldername, _, filenames in os.walk(source_folder):
            for filename in filenames:
                full_file_name = os.path.join(foldername, filename)
                pic_meta, file_meta = exif.distill_serialized_picfile_meta_data(full_file_name)
                if not file_meta.file_name:
                    continue

                next(progress_message)

                # check on md5_signature
                sql_string = (
                    f"SELECT id FROM {cls.table_pictures} WHERE "
                    f"md5_signature = '{pic_meta.md5_signature}';"
                )
                cursor.execute(sql_string)
                if cursor.fetchone():
                    log_lines.append(
                        f"{full_file_name} already in database: "
                        f"match md5_signature, {pic_meta.md5_signature}"
                    )
                    continue

                # check on picture dates
                if pic_meta.date_picture:
                    sql_string = (
                        f"SELECT id FROM {cls.table_pictures} WHERE "
                        f"date_picture = '{pic_meta.date_picture}';"
                    )
                    cursor.execute(sql_string)
                    if cursor.fetchone():
                        log_lines.append(
                            f"{full_file_name} seems already in database: "
                            f"match date_picture {pic_meta.date_picture}..."
                        )
                        continue

                else:
                    sql_string = (
                        f"SELECT id FROM {cls.table_files} WHERE "
                        f"file_modified = '{file_meta.file_modified}' AND "
                        f"file_name = '{file_meta.file_name}';"
                    )
                    cursor.execute(sql_string)
                    if cursor.fetchone():
                        log_lines.append(
                            f"{full_file_name} seems already in database: "
                            f"match file modified {file_meta.file_modified} and "
                            f"{file_meta.file_name}..."
                        )
                        continue

                log_lines.append(
                    f"{full_file_name} not found in database "
                    f"and moved to {destination_folder}"
                )
                shutil.move(
                    os.path.join(foldername, filename),
                    os.path.join(destination_folder, filename),
                )

        with open(log_file, "at") as f:
            for line in log_lines:
                f.write(line + "\n")

        print()

    @staticmethod
    def get_geolocation_info(longitude: float, latitude: float) -> dict | None:
        lat_lon = ", ".join([str(latitude), str(longitude)])
        try:
            location = geolocator.reverse(lat_lon, language="en")
            return json.dumps(location.raw["address"])

        except Exception as error:
            print(f"Error getting geolocation info: {error=}")
            return

    @classmethod
    @DbUtils.connect
    def add_to_locations_table(cls, picture_id, location, cursor):
        """add record to locations map. It first checks if picture_id is already
        in database.
        :arguments:
            picture_id: integer
            location: tuple(latitude, longitude, altitude)
        :return:
            True: if record is added
            False: if record already in database
        """
        sql_string = (
            f"select picture_id from {cls.table_locations} "
            f"where picture_id = {picture_id} "
        )
        cursor.execute(sql_string)
        if cursor.fetchone():
            return False

        # Point and get_geolocation_info have format (Longitude, Latitude) like (x, y)
        point = Point(location[1], location[0])
        geolocation_info = cls.get_geolocation_info(location[1], location[0])
        sql_string_locations = (
            f"INSERT INTO {cls.table_locations} "
            f"(picture_id, latitude, longitude, altitude, geolocation_info, geom) "
            f"VALUES (%s, %s, %s, %s, %, ST_SetSRID(%s::geometry, %s)) "
        )

        # TODO fix patch elevation is Null
        altitude = 0.0 if location[2] is None else location[2]
        cursor.execute(
            sql_string_locations,
            (
                picture_id,
                location[0],
                location[1],
                altitude,
                geolocation_info,
                point.wkb_hex,
                EPSG_WGS84,
            ),
        )
        return True

    @classmethod
    @DbUtils.connect
    def populate_locations_table(cls, cursor, json_filename=None, picture_ids=None):
        if json_filename is not None:
            with open(json_filename) as json_file:
                # ensure the tuple has always 2 values
                picture_ids = json.load(json_file)

            sql_string_pictures = (
                f"select id, gps_latitude, gps_longitude, gps_altitude "
                f"from {cls.table_pictures} where id=any(array{picture_ids})"
            )
        elif picture_ids:
            sql_string_pictures = (
                f"select id, gps_latitude, gps_longitude, gps_altitude "
                f"from {cls.table_pictures} where id=any(array{picture_ids})"
            )
        else:
            sql_string_pictures = (
                f"select id, gps_latitude, gps_longitude, gps_altitude "
                f"from {cls.table_pictures} where not rotate_checked"
            )
        cursor.execute(sql_string_pictures)

        counter = 0
        for i, pic in enumerate(cursor.fetchall()):
            lat_lon_str, lat_lon_val = exif.convert_gps(pic[1], pic[2], pic[3])
            if lat_lon_str:
                if cls.add_to_locations_table(pic[0], lat_lon_val):
                    counter += 1
                    print(f"{i:5}: {counter:4} pic id: {pic[0]}, {lat_lon_str}")


    @classmethod
    @DbUtils.connect
    def update_image(cls, picture_id, image, rotate, cursor):
        """Assumes thumbnail is changed by a rotation. This method replaces
        the thumbnail and rotation in the database.
        Note the original md5 signature based on the original thumbnail
        at rotation 0 remains unchanged.
        """
        img_bytes = io.BytesIO()
        image.save(img_bytes, format="JPEG")
        picture_bytes = img_bytes.getvalue()

        thumbnail = json.dumps(picture_bytes.decode(exif.codec))

        sql_str = (
            f"UPDATE {cls.table_pictures} "
            f"SET thumbnail = (%s), "
            f"rotate = (%s) "
            f"WHERE id= (%s) "
        )
        cursor.execute(sql_str, (thumbnail, rotate, picture_id))

    @classmethod
    @DbUtils.connect
    def store_attributes(cls, picture_id, image, pic_meta, cursor):
        img_bytes = io.BytesIO()
        image.save(img_bytes, format="JPEG")
        picture_bytes = img_bytes.getvalue()

        thumbnail = json.dumps(picture_bytes.decode(exif.codec))
        pic_meta = exif.serialize_gps_data_fields(pic_meta)

        sql_str = (
            f"UPDATE {cls.table_pictures} "
            f"SET thumbnail = (%s), "
            f"date_picture = (%s), "
            f"camera_make = (%s), "
            f"camera_model = (%s), "
            f"gps_latitude = (%s), "
            f"gps_longitude = (%s), "
            f"gps_altitude = (%s), "
            f"gps_img_dir = (%s), "
            f"rotate = (%s) "
            f"WHERE id= (%s) "
        )
        cursor.execute(
            sql_str,
            (
                thumbnail,
                pic_meta.date_picture,
                pic_meta.camera_make,
                pic_meta.camera_model,
                pic_meta.gps_latitude,
                pic_meta.gps_longitude,
                pic_meta.gps_altitude,
                pic_meta.gps_img_direction,
                pic_meta.rotate,
                picture_id,
            ),
        )

    @classmethod
    @DbUtils.connect
    def get_file_paths(cls, cursor):
        """get a sorted list of unique file_paths with base folder removed"""
        pattern = r"^[a-zA-Z]:\\(?:pictures\\){1,2}(.*)\\$"
        sql_str = (
            f"SELECT DISTINCT lower(file_path) fp from {cls.table_files} ORDER BY fp"
        )
        cursor.execute(sql_str)
        return sorted(
            list(set(re.search(pattern, val[0]).group(1) for val in cursor.fetchall()))
        )

    @classmethod
    @DbUtils.connect
    def get_ids_by_folder(cls, folder, cursor):
        """get the ids of pictures where folder matches."""
        folder = folder.replace("'", "''")
        sql_str = (
            f"SELECT p.id from {cls.table_pictures} as p "
            f"JOIN {cls.table_files} as f on f.picture_id = p.id "
            f"WHERE lower(f.file_path) LIKE '%{folder}\\\\'"
        )
        cursor.execute(sql_str)
        return [val[0] for val in cursor.fetchall()]

    @classmethod
    @DbUtils.connect
    def get_ids_by_date(cls, date_select, cursor):
        """get the ids of pictures where the file created date is greater
        than date_select
        """
        sql_str = (
            f"SELECT p.id from {cls.table_pictures} as p "
            f"JOIN {cls.table_files} as f on f.picture_id = p.id "
            f'WHERE f.file_created > \'{date_select.strftime("%Y-%m-%d")}\' '
        )
        cursor.execute(sql_str)
        return [val[0] for val in cursor.fetchall()]

    @classmethod
    @DbUtils.connect
    def filter_ids(cls, ids, cursor, db_filter=DbFilter.ALL):
        """filter ids on value of db_filter."""
        match db_filter:
            case DbFilter.NOGPS:
                sql_str = (
                    f"SELECT id from {cls.table_pictures} "
                    f"WHERE id=any(array{ids}) AND length(gps_latitude::text) < 3"
                )
            case DbFilter.CHECKED:
                sql_str = (
                    f"SELECT id from {cls.table_pictures} "
                    f"WHERE id=any(array{ids}) AND rotate_checked"
                )
            case DbFilter.NOT_CHECKED:
                sql_str = (
                    f"SELECT id from {cls.table_pictures} "
                    f"WHERE id=any(array{ids}) AND not rotate_checked"
                )
            case other:
                sql_str = (
                    f"SELECT id from {cls.table_pictures} " f"WHERE id=any(array{ids})"
                )
        cursor.execute(sql_str)
        return [val[0] for val in cursor.fetchall()]

    @classmethod
    @DbUtils.connect
    def set_rotate_check(cls, pic_ids, cursor, set_value=True):
        """set rotate_check to True or False"""
        sql_str = (
            f"UPDATE {cls.table_pictures} set rotate_checked = {set_value} "
            f"where id=any(array{pic_ids}) "
        )
        cursor.execute(sql_str)
