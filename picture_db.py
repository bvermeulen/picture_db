from dataclasses import dataclass
import shutil
import datetime
import hashlib
import io
import json
import os
from functools import wraps
import numpy as np
from shapely.geometry import Point
import psycopg2
from PIL import Image, ImageShow
from decouple import config
import psutil
from picture_exif import Exif
from Utils.plogger import Logger

logformat = '%(asctime)s:%(levelname)s:%(message)s'
Logger.set_logger('.\\picture.log', logformat, 'INFO')
logger = Logger.getlogger()

if os.name == 'nt':
    ImageShow.WindowsViewer.format = 'PNG'
    display_process = 'Microsoft.Photos.exe'

elif os.name == 'posix':
    ImageShow.UnixViewer = 'PNG'
    display_process = 'eog'

else:
    assert False, f'operating system: {os.name} is not implemented'


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


EPSG_WGS84 = 4326
# note: the MD5 signature is based on the thumbnail made with the
# size below. So changing the size will make check on signature invalid
DATABASE_PICTURE_SIZE = (600, 600)
exif = Exif()


def progress_message_generator(message):
    loop_dash = ['\u2014', '\\', '|', '/']
    i = 1
    print_interval = 1
    while True:
        print(
            f'\r{loop_dash[int(i/print_interval) % 4]} {i} {message}', end='')
        i += 1
        yield


class DbUtils:
    '''  utility methods for database
    '''
    host = config('DB_HOST')
    db_user = config('DB_USERNAME')
    db_user_pw = config('DB_PASSWORD')
    database = config('DATABASE')

    @classmethod
    def connect(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            connect_string = f'host=\'{cls.host}\' dbname=\'{cls.database}\''\
                             f'user=\'{cls.db_user}\' password=\'{cls.db_user_pw}\''
            result = None
            try:
                # add ggsencmode='disable' to resolve unsupported frontend protocol
                # 1234.5679: server supports 2.0 to 3.0
                # should be fixed on postgresql 12.3
                connection = psycopg2.connect(connect_string, gssencmode='disable')
                cursor = connection.cursor()
                result = func(*args, cursor, **kwargs)
                connection.commit()

            except psycopg2.Error as error:
                print(f'error while connect to PostgreSQL {cls.database}: '
                      f'{error}')

            finally:
                if connection:
                    cursor.close()
                    connection.close()

            return result

        return wrapper

    @staticmethod
    def get_answer(choices):
        ''' arguments:
            choices: list of picture numbers to be able to delete

            returns a list, either:
            [1, 2, ..., n] : choices of pictures to delete
            [0]  : exit function
            [-1] : skip item
        '''
        answer_delete = []
        while (not (any(val in [-1, 0] for val in answer_delete) and
                    len(answer_delete) == 1) and
               (not any(val in choices for val in answer_delete))):

            _answer = input('Delete picture numbers [separated by spaces] '
                            '(press 0 to quit, space to skip): ')

            if _answer == ' ':
                _answer = '-1'

            answer_delete = _answer.replace(',', ' ').split()
            try:
                answer_delete = [int(val) for val in _answer.replace(',', ' ').split()]

            except ValueError:
                pass

        return answer_delete

    @staticmethod
    def get_name():
        valid = False
        while not valid:
            name = input('Please give your name: ')
            if 5 < len(name) < 20:
                valid = True

        return name

    @staticmethod
    def remove_display():
        ''' remove thumbnail picture by killing the display process
        '''
        for proc in psutil.process_iter():
            if proc.name() == display_process:
                proc.kill()


class PictureDb:
    table_pictures = 'pictures'
    table_files = 'files'
    table_reviews = 'reviews'
    table_locations = 'locations'


    @classmethod
    @DbUtils.connect
    def delete_table(cls, table_name: str, cursor):
        sql_string = f'DROP TABLE {table_name};'
        cursor.execute(sql_string)
        print(f'delete table {table_name}')

    @classmethod
    @DbUtils.connect
    def create_pictures_table(cls, cursor):
        sql_string = (
            f'CREATE TABLE {cls.table_pictures} ('
            f'id SERIAL PRIMARY KEY, '
            f'date_picture TIMESTAMP, '
            f'md5_signature VARCHAR(32), '
            f'camera_make VARCHAR(50), '
            f'camera_model VARCHAR(50), '
            f'gps_latitude JSON, '
            f'gps_longitude JSON, '
            f'gps_altitude JSON, '
            f'gps_img_dir JSON, '
            f'thumbnail JSON, '
            f'exif JSON, '
            f'rotate INTEGER DEFAULT 0, '
            f'rotate_checked BOOLEAN DEFAULT FALSE'
            f');'
        )
        print(f'create table {cls.table_pictures}')
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_files_table(cls, cursor):
        sql_string = (
            f'CREATE TABLE {cls.table_files} ('
            f'id SERIAL PRIMARY KEY, '
            f'picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE UNIQUE NOT NULL, '
            f'file_path VARCHAR(250), '
            f'file_name VARCHAR(250), '
            f'file_modified TIMESTAMP, '
            f'file_created TIMESTAMP, '
            f'file_size INTEGER, '
            f'file_checked BOOLEAN'
            f');'
        )
        print(f'create table {cls.table_files}')
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_reviews_table(cls, cursor):
        sql_string = (
            f'CREATE TABLE {cls.table_reviews} ('
            f'id SERIAL PRIMARY KEY, '
            f'picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE NOT NULL, '
            f'reviewer_name VARCHAR(20), '
            f'review_date TIMESTAMP'
            f');'
        )
        print(f'create table {cls.table_reviews}')
        cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def create_locations_table(cls, cursor):
        sql_string = (
            f'CREATE TABLE {cls.table_locations} ('
            f'id SERIAL PRIMARY KEY, '
            f'picture_id INTEGER REFERENCES {cls.table_pictures}(id) ON DELETE CASCADE UNIQUE, '
            f'date_picture TIMESTAMP NOT NULL, '
            f'latitude DOUBLE PRECISION NOT NULL, '
            f'longitude DOUBLE PRECISION NOT NULL, '
            f'altitude REAL NOT NULL, '
            f'geom geometry(Point, {EPSG_WGS84})'
            f');'
        )
        print(f'create table {cls.table_locations}')
        cursor.execute(sql_string)

    @classmethod
    def get_pic_meta(cls, filename):
        pic_meta = PicturesTable(*[None]*13)
        file_meta = FilesTable(*[None]*8)

        # file attributes
        file_stat = os.stat(filename)
        file_meta.file_name = os.path.basename(filename)
        file_meta.file_path = os.path.abspath(filename).replace(file_meta.file_name, '')
        file_meta.file_modified = datetime.datetime.fromtimestamp(file_stat.st_mtime)
        file_meta.file_created = datetime.datetime.fromtimestamp(file_stat.st_ctime)
        file_meta.file_size = file_stat.st_size

        # exif attributes
        try:
            im = Image.open(filename)
        except OSError:
            return PicturesTable(*[None]*13), FilesTable(*[None]*8)

        exif_dict = exif.get_exif_dict(im)

        if exif_dict:
            pic_meta.camera_make = exif_dict.get('0th').get('Make')
            if pic_meta.camera_make:
                pic_meta.camera_make = pic_meta.camera_make.\
                    replace('\x00', '')

            pic_meta.camera_model = exif_dict.get('0th').get('Model')
            if pic_meta.camera_model:
                pic_meta.camera_model = pic_meta.camera_model.\
                    replace('\x00', '')

            try:
                pic_meta.date_picture = datetime.datetime.strptime(exif_dict.get('0th').\
                    get('DateTime'), '%Y:%m:%d %H:%M:%S')

            except (TypeError, ValueError):
                pic_meta.date_picture = None

            (
                pic_meta.gps_latitude, pic_meta.gps_longitude,
                pic_meta.gps_altitude, pic_meta.gps_img_direction
            ) = exif.exifgps_to_json(exif_dict.get('GPS'))

        else:
            pic_meta.camera_make, pic_meta.camera_model, \
                pic_meta.date_picture = None, None, None
            pic_meta.gps_latitude, pic_meta.gps_longitude, \
                pic_meta.gps_altitude, pic_meta.gps_img_direction = [json.dumps({})]*4

        im.thumbnail(DATABASE_PICTURE_SIZE, Image.ANTIALIAS)
        img_bytes = io.BytesIO()
        im.save(img_bytes, format='JPEG')
        picture_bytes = img_bytes.getvalue()

        pic_meta.thumbnail = json.dumps(picture_bytes.decode(exif.codec))
        pic_meta.md5_signature = hashlib.md5(picture_bytes).hexdigest()
        pic_meta.exif = exif.exif_to_json(exif_dict)
        pic_meta.rotate = 0
        pic_meta.rotate_checked = False

        return pic_meta, file_meta

    @classmethod
    @DbUtils.connect
    def store_pictures_base_folder(cls, base_folder, cursor):
        ''' re-initialises the database all previous data will be lost
        '''
        progress_message = progress_message_generator(
            f'loading picture meta data from {base_folder}')

        sql_pictures = (f'INSERT INTO {cls.table_pictures} ('
                        f'date_picture, md5_signature, camera_make, camera_model, '
                        f'gps_latitude, gps_longitude, gps_altitude, gps_img_dir, '
                        f'thumbnail, exif, rotate, rotate_checked) '
                        f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
                        f'RETURNING id;')

        sql_files = (f'INSERT INTO {cls.table_files} ('
                     f'picture_id, file_path, file_name, file_modified, file_created, '
                     f'file_size, file_checked)'
                     f'VALUES (%s, %s, %s, %s, %s, %s, %s);')

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:

                if filename[-4:] in ['.jpg', '.JPG']:
                    pic_meta, file_meta = cls.get_pic_meta(
                        os.path.join(foldername, filename))
                    if not file_meta.file_name:
                        continue

                    cursor.execute(sql_pictures, (
                        pic_meta.date_picture, pic_meta.md5_signature,
                        pic_meta.camera_make, pic_meta.camera_model,
                        pic_meta.gps_latitude, pic_meta.gps_longitude,
                        pic_meta.gps_altitude, pic_meta.gps_img_direction,
                        pic_meta.thumbnail, pic_meta.exif,
                        pic_meta.rotate, pic_meta.rotate_checked
                    ))
                    picture_id = cursor.fetchone()[0]

                    cursor.execute(sql_files, (
                        picture_id, file_meta.file_path, file_meta.file_name,
                        file_meta.file_modified, file_meta.file_created,
                        file_meta.file_size, True
                    ))
                    lat_lon_str, lat_lon_val = exif.convert_gps(
                        pic_meta.gps_latitude, pic_meta.gps_longitude,
                        pic_meta.gps_altitude
                    )
                    if lat_lon_str:
                        cls.add_to_locations_table(
                            picture_id, pic_meta.date_picture, lat_lon_val)

                    next(progress_message)

        print()

    @classmethod
    @DbUtils.connect
    def check_and_add_files(cls, base_folder, cursor):
        ''' check if files are in database, if they are not then add
        '''
        progress_message = progress_message_generator(
            f'update picture meta data from {base_folder}')
        sql_string = (f'UPDATE {cls.table_files} SET file_checked = FALSE;')
        cursor.execute(sql_string)

        sql_pictures = (f'INSERT INTO {cls.table_pictures} ('
                        f'date_picture, md5_signature, camera_make, camera_model, '
                        f'gps_latitude, gps_longitude, gps_altitude, gps_img_dir, '
                        f'thumbnail, exif, rotate, rotate_checked) '
                        f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
                        f'RETURNING id;')

        sql_files = (f'INSERT INTO {cls.table_files} ('
                     f'picture_id, file_path, file_name, file_modified, file_created, '
                     f'file_size, file_checked) '
                     f'VALUES (%s, %s, %s, %s, %s, %s, %s);')

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:

                if filename[-4:] in ['.jpg', '.JPG']:
                    sql_foldername = foldername.replace("'", "''")
                    sql_filename = filename.replace("'", "''")

                    sql_string = (f'SELECT picture_id FROM {cls.table_files} WHERE '
                                  f'file_path=\'{sql_foldername}\\\' AND '
                                  f'file_name=\'{sql_filename}\';')
                    cursor.execute(sql_string)
                    try:
                        picture_id = cursor.fetchone()[0]

                    except TypeError:
                        picture_id = None

                    # file exists but not in DB -> add to DB
                    if not picture_id:
                        pic_meta, file_meta = cls.get_pic_meta(
                            os.path.join(foldername, filename))
                        if not file_meta.file_name:
                            continue

                        cursor.execute(sql_pictures, (
                            pic_meta.date_picture, pic_meta.md5_signature,
                            pic_meta.camera_make, pic_meta.camera_model,
                            pic_meta.gps_latitude, pic_meta.gps_longitude,
                            pic_meta.gps_altitude, pic_meta.gps_img_direction,
                            pic_meta.thumbnail, pic_meta.exif,
                            pic_meta.rotate, pic_meta.rotate_checked
                        ))

                        picture_id = cursor.fetchone()[0]

                        cursor.execute(sql_files, (
                            picture_id, file_meta.file_path, file_meta.file_name,
                            file_meta.file_modified, file_meta.file_created,
                            file_meta.file_size, True
                        ))

                        lat_lon_str, lat_lon_val = exif.convert_gps(
                            pic_meta.gps_latitude, pic_meta.gps_longitude,
                            pic_meta.gps_altitude)
                        if lat_lon_str:
                            cls.add_to_locations_table(
                                picture_id, pic_meta.date_picture, lat_lon_val)

                    else:
                        sql_string = (f'UPDATE {cls.table_files} '
                                      f'SET file_checked = TRUE '
                                      f'WHERE picture_id={picture_id};')
                        cursor.execute(sql_string)

                    next(progress_message)

        print()

    @classmethod
    @DbUtils.connect
    def check_and_remove_non_existing_files(cls, cursor):
        ''' check if files are in the database, but not on file, in that case remove
            from the database
        '''
        sql_string = (
            f'SELECT picture_id FROM {cls.table_files} WHERE NOT file_checked;')
        cursor.execute(sql_string)
        deleted_ids = [id[0] for id in cursor.fetchall()]
        cls.delete_ids(deleted_ids)

    @classmethod
    @DbUtils.connect
    def load_picture_meta(cls, _id: int, cursor):
        ''' load picture meta data from the database
            :arguments:
                _id: picture id number in database: integer
            :returns:
                im: PIL image
                pic_meta: PicturesTable
                file_meta: FilesTable
                lat_lon_str: string
        '''
        sql_string = f'SELECT * FROM {cls.table_pictures} WHERE id={_id};'
        cursor.execute(sql_string)
        data_from_table_pictures = cursor.fetchone()

        if not data_from_table_pictures:
            return None, None, None, None, (None, None, None)

        else:
            sql_string = f'SELECT * FROM {cls.table_files} WHERE picture_id={_id};'
            cursor.execute(sql_string)
            data_from_table_files = cursor.fetchone()
            if not data_from_table_files:
                return None, None, None, None

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

        assert pic_meta.id == file_meta.picture_id, \
            'load_picture_meta: database integrity error'

        if pic_meta.thumbnail:
            img_bytes = io.BytesIO(
                pic_meta.thumbnail.encode(exif.codec))
            im = Image.open(img_bytes)

        lat_lon_str, lat_lon_val = exif.convert_gps(
            pic_meta.gps_latitude, pic_meta.gps_longitude, pic_meta.gps_altitude)

        return im, pic_meta, file_meta, lat_lon_str, lat_lon_val

    @classmethod
    @DbUtils.connect
    def select_pics_for_merge(cls, source_folder, destination_folder, cursor):
        '''  method that checks if picture is in the database. If it is
             not moves picture from source folder to the destination folder
        '''
        progress_message = progress_message_generator(
            f'merging pictures from {source_folder}')

        log_file = os.path.join(source_folder, '_select_pictures_to_merge.log')
        with open(log_file, 'at') as f:
            c_time = datetime.datetime.now()
            f.write(f'===> Select pictures to merge: {c_time}\n')

        log_lines = []
        for foldername, _, filenames in os.walk(source_folder):
            for filename in filenames:
                if filename[-4:] not in ['.jpg', '.JPG']:
                    continue

                full_file_name = os.path.join(foldername, filename)
                next(progress_message)

                pic_meta, file_meta = cls.get_pic_meta(full_file_name)

                # check on md5_signature
                sql_string = (f'SELECT id FROM {cls.table_pictures} WHERE '
                              f'md5_signature = \'{pic_meta.md5_signature}\';')
                cursor.execute(sql_string)
                if cursor.fetchone():
                    log_lines.append(f'{full_file_name} already in database: '
                                     f'match md5_signature, {pic_meta.md5_signature}')
                    continue

                # check on picture dates
                if pic_meta.date_picture:
                    sql_string = (f'SELECT id FROM {cls.table_pictures} WHERE '
                                  f'date_picture = \'{pic_meta.date_picture}\';')
                    cursor.execute(sql_string)
                    if cursor.fetchone():
                        log_lines.append(f'{full_file_name} seems already in database: '
                                         f'match date_picture {pic_meta.date_picture}')
                        continue

                else:
                    sql_string = (f'SELECT id FROM {cls.table_files} WHERE '
                                  f'file_modified = \'{file_meta.file_modified}\';')
                    cursor.execute(sql_string)
                    if cursor.fetchone():
                        log_lines.append(f'{full_file_name} seems already in database: '
                                         f'match file modified {file_meta.file_modified}')
                        continue

                log_lines.append(f'{full_file_name} not found in database '
                                 f'and moved to {destination_folder}')
                shutil.move(os.path.join(foldername, filename),
                            os.path.join(destination_folder, filename))

        with open(log_file, 'at') as f:
            for line in log_lines:
                f.write(line + '\n')

        print()

    @classmethod
    @DbUtils.connect
    def review_required(cls, accepted_review_date, picture_id, cursor):
        sql_string = (f'SELECT review_date FROM {cls.table_reviews} '
                      f'WHERE picture_id={picture_id};')
        cursor.execute(sql_string)
        latest_review_date = datetime.datetime(1800, 1, 1)
        for review_date in cursor.fetchall():
            if review_date[0] > latest_review_date:
                latest_review_date = review_date[0]

        if latest_review_date > accepted_review_date:
            return False
        else:
            return True

    @classmethod
    @DbUtils.connect
    def delete_ids(cls, deleted_ids, cursor):
        if deleted_ids:
            sql_string = (f'DELETE FROM {cls.table_pictures} '
                          f'WHERE id=any(array{deleted_ids});')
            cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def update_reviews(cls, pic_selection, reviewer_name, cursor):
        for pic in pic_selection:
            sql_string = (f'INSERT INTO {cls.table_reviews} ('
                          f'picture_id, reviewer_name, review_date) '
                          f'VALUES (%s, %s, %s);')
            cursor.execute(
                sql_string, (pic.get('id'), reviewer_name,
                             datetime.datetime.now()))

    @classmethod
    @DbUtils.connect
    def remove_duplicate_pics(cls, deleted_folder, cursor, method='md5',
                              accepted_review_date=datetime.datetime(1900, 1, 1)):
        '''  sort out duplicate pictures by either using the md5_signature or picture
             date.
        '''
        utils = DbUtils()
        reviewer_name = utils.get_name()

        if method == 'md5':
            method = 'md5_signature'

        elif method == 'date':
            method = 'date_picture'

        else:
            print(f'{method} not valid, choose \'md5\' or \'time\'...')
            return

        log_file = os.path.join(deleted_folder, '_delete_duplicate_pictures.log')
        with open(log_file, 'at') as f:
            c_time = datetime.datetime.now()
            f.write(f'===> Remove duplicates with method \'{method}\': {c_time}\n')

        sql_string = (f'SELECT {method} FROM {cls.table_pictures} WHERE {method} IN '
                      f'(SELECT {method} FROM {cls.table_pictures} GROUP BY {method} '
                      f'HAVING count(*) > 1) ORDER BY id;')
        cursor.execute(sql_string)
        list_duplicates = {item[0] for item in cursor.fetchall()}

        for item in list_duplicates:
            sql_string = (f'SELECT id, thumbnail '
                          f'FROM {cls.table_pictures} WHERE {method}=\'{item}\';')
            cursor.execute(sql_string)

            pic_selection = []
            choices = []
            for i, pic_tuple in enumerate(cursor.fetchall()):

                sql_string = (f'SELECT file_path, file_name '
                              f'FROM {cls.table_files} WHERE picture_id={pic_tuple[0]};')
                cursor.execute(sql_string)
                file_path, file_name = cursor.fetchone()

                if not cls.review_required(accepted_review_date, pic_tuple[0]):
                    print(f'no review required for: '
                          f'{pic_tuple[0]}, {file_path}, {file_name}')
                    continue
                else:
                    pass

                choices.append(i + 1)
                pic_selection.append({
                    'index': i + 1,
                    'id': pic_tuple[0],
                    'file_path': file_path,
                    'file_name': file_name,
                    'thumbnail': io.BytesIO(pic_tuple[1].encode(exif.codec))})

            if not pic_selection:
                continue

            print('-'*80)
            pic_arrays = []
            for pic in pic_selection:
                height, width = (200, 200)
                array_padded = np.ones((height, width, 3), dtype=np.uint8)*200

                print(f'[{pic.get("index")}] '
                      f'[{os.path.join(pic.get("file_path"), pic.get("file_name"))}]')
                image_array = np.array(Image.open(pic.get('thumbnail')))

                height = min(image_array.shape[0], height)
                width = min(image_array.shape[1], width)
                array_padded[:height, :width, :] = image_array[:height, :width, :]
                pic_arrays.append(array_padded)

            Image.fromarray(np.hstack(pic_arrays)).show()

            # -1 skip removal, 0 quit method, 1..n pictures index to be removed
            # in case of skip, update the reviews table
            answer_delete = utils.get_answer(choices)
            if answer_delete[0] == -1:
                cls.update_reviews(pic_selection, reviewer_name)

            elif answer_delete[0] == 0:
                return

            else:
                log_lines = []
                deleted_ids = []
                for pic in pic_selection:
                    if pic.get('index') in answer_delete:
                        deleted_ids.append(pic.get('id'))
                        _from = os.path.join(pic.get('file_path'), pic.get('file_name'))
                        _to = os.path.join(deleted_folder, pic.get('file_name'))

                        try:
                            shutil.move(_from, _to)
                            log_line = (f'file deleted, id: {pic.get("id")}, '
                                        f'file_name: {_from}')
                        except FileNotFoundError:
                            log_line = (f'file not in folder, id: {pic.get("id")}, '
                                        f'file_name: {_from}')

                        print(log_line)
                        log_lines.append(log_line)

                # call seperately to make sure change to db is committed on
                # return of this function
                cls.delete_ids(deleted_ids)

                with open(log_file, 'at') as f:
                    for line in log_lines:
                        f.write(line + '\n')

    @classmethod
    @DbUtils.connect
    def remove_pics_by_id(cls, deleted_folder, start_id, cursor, end_id=None):
        '''  remove pictures that are in dabase with id between start_id and end_id
             patch needed as google photo may merge duplicate photos
        '''
        log_file = os.path.join(deleted_folder, '_delete_pictures_by_id.log')
        with open(log_file, 'at') as f:
            c_time = datetime.datetime.now()
            if end_id:
                f.write(f'===> Remove pictured by id from '
                        f'{start_id} to {end_id}: {c_time}\n')

            else:
                f.write(f'===> Remove pictured by id from '
                        f'{start_id} until last: {c_time}\n')

        if end_id:
            sql_string = (f'select id, file_path, file_name from {cls.table_files} '
                          f'where id between {start_id} and {end_id}')

        else:
            sql_string = (f'select id, file_path, file_name from {cls.table_files} '
                          f'where id >= {start_id}')

        cursor.execute(sql_string)

        log_lines = []
        deleted_ids = []
        for pic in cursor.fetchall():
            print(pic)
            _id = pic[0]
            _file_path = pic[1]
            _file_name = pic[2]
            _from = os.path.join(_file_path, _file_name)
            _to = os.path.join(deleted_folder, _file_name)

            try:
                shutil.move(_from, _to)
                log_line = (f'file deleted, id: {_id}, '
                            f'file_name: {_from}')

            except FileNotFoundError:
                log_line = (f'file not in folder, id: {_id}, '
                            f'file_name: {_from}')

            log_lines.append(log_line)
            deleted_ids.append(_id)

        cls.delete_ids(deleted_ids)

        with open(log_file, 'at') as f:
            for line in log_lines:
                f.write(line + '\n')

    @classmethod
    @DbUtils.connect
    def add_to_locations_table(cls, picture_id, date_picture, location, cursor):
        ''' add record to locations map. It first checks if picture_id is already
            in database.
            :arguments:
                picture_id: integer
                data_picture: time stamp or None
                location: tuple(latitude, longitude, altitude)
            :return:
                True: if record is add
                False: if record already in database
        '''
        sql_string = (
            f'select picture_id from {cls.table_locations} '
            f'where picture_id = {picture_id} '
        )
        cursor.execute(sql_string)
        if cursor.fetchone():
            return False

        # Point has format (Longitude, Latitude) like (x, y)
        point = Point(location[1], location[0])

        if date_picture:
            pass

        else:
            sql_string_files = (
                f'select file_modified from {cls.table_files} '
                f'where picture_id={picture_id} '
            )

            cursor.execute(sql_string_files)
            date_picture = cursor.fetchone()

        sql_string_locations = (
            f'INSERT INTO {cls.table_locations} '
            f'(picture_id, date_picture, latitude, longitude, altitude, geom) '
            f'VALUES (%s, %s, %s, %s, %s, ST_SetSRID(%s::geometry, %s)) '
        )

        #TODO fix patch elevation is Null
        altitude = 0.0 if location[2] is None else location[2]
        cursor.execute(
            sql_string_locations,
            (picture_id, date_picture,
             location[0], location[1], altitude,
             point.wkb_hex, EPSG_WGS84)
        )
        return True

    @classmethod
    @DbUtils.connect
    def populate_locations_table(cls, cursor, json_filename=None):
        if json_filename is None:
            sql_string_pictures = (
                f'select id, date_picture, gps_latitude, gps_longitude, gps_altitude '
                f'from {cls.table_pictures} where not rotate_checked'
            )
        else:
            with open(json_filename) as json_file:
                picture_ids = tuple(json.load(json_file))
            sql_string_pictures = (
                f'select id, date_picture, gps_latitude, gps_longitude, gps_altitude '
                f'from {cls.table_pictures} where id in {picture_ids}'
            )
        cursor.execute(sql_string_pictures)

        counter = 0
        for i, pic in enumerate(cursor.fetchall()):

            lat_lon_str, lat_lon_val = exif.convert_gps(pic[2], pic[3], pic[4])
            if lat_lon_str:
                if cls.add_to_locations_table(pic[0], pic[1], lat_lon_val):
                    counter += 1
                    print(
                        f'{i:5}: {counter:4} pic id: {pic[0]}, '
                        f'date: {pic[1]}, {lat_lon_str}'
                    )

    @classmethod
    @DbUtils.connect
    def update_rotate_checked(cls, json_filename, cursor):
        with open(json_filename) as json_file:
            picture_ids = json.load(json_file)

        for picture_id in picture_ids:
            sql_string = (
                f'update {cls.table_pictures} '
                f'set rotate_checked = True '
                f'where id = {picture_id}'
            )
            cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def update_image(cls, picture_id, image, rotate, cursor):
        '''  Assumes thumbnail is changed by a rotation. This method replaces
             the thumbnail and rotation in the database.
             Note the original md5 signature based on the original thumbnail
             at rotation 0 remains unchanged.
        '''
        img_bytes = io.BytesIO()
        image.save(img_bytes, format='JPEG')
        picture_bytes = img_bytes.getvalue()

        thumbnail = json.dumps(picture_bytes.decode(exif.codec))

        sql_str = (
            f'UPDATE {cls.table_pictures} '
            f'SET thumbnail = (%s), '
            f'rotate = (%s) '
            f'WHERE id= (%s) '
        )
        cursor.execute(sql_str, (thumbnail, rotate, picture_id))

    @classmethod
    @DbUtils.connect
    def store_attributes(cls, picture_id, image, pic_meta, cursor):
        img_bytes = io.BytesIO()
        image.save(img_bytes, format='JPEG')
        picture_bytes = img_bytes.getvalue()

        thumbnail = json.dumps(picture_bytes.decode(exif.codec))
        pic_meta = exif.convert_to_json(pic_meta)

        sql_str = (
            f'UPDATE {cls.table_pictures} '
            f'SET thumbnail = (%s), '
            f'date_picture = (%s), '
            f'camera_make = (%s), '
            f'camera_model = (%s), '
            f'gps_latitude = (%s), '
            f'gps_longitude = (%s), '
            f'gps_altitude = (%s), '
            f'gps_img_dir = (%s), '
            f'rotate = (%s) '
            f'WHERE id= (%s) '
        )
        cursor.execute(sql_str, (
            thumbnail,
            pic_meta.date_picture, pic_meta.camera_make, pic_meta.camera_model,
            pic_meta.gps_latitude, pic_meta.gps_longitude, pic_meta.gps_altitude,
            pic_meta.gps_img_direction, pic_meta.rotate, picture_id
        ))

    @classmethod
    @DbUtils.connect
    def get_folder_ids(cls, folder, cursor, db_filter=None):
        ''' get the ids of pictures where folder matches.
            It only returns ids where the rotation checked is False
        '''
        if db_filter == 'nogps':
            sql_str = (
                f'SELECT p.id from {cls.table_pictures} as p '
                f'JOIN {cls.table_files} as f on f.picture_id = p.id '
                f'WHERE lower(f.file_path) LIKE \'%{folder}%\' AND '
                f'length(p.gps_latitude::text) < 3 AND '
                f'not p.rotate_checked'
            )
        else:
            sql_str = (
                f'SELECT p.id from {cls.table_pictures} as p '
                f'JOIN {cls.table_files} as f on f.picture_id = p.id '
                f'WHERE lower(f.file_path) LIKE \'%{folder}%\' AND '
                f'not p.rotate_checked'
            )
        cursor.execute(sql_str)
        return [val[0] for val in cursor.fetchall()]

    @classmethod
    @DbUtils.connect
    def replace_thumbnail(cls, base_folder, cursor):
        progress_message = progress_message_generator(
            f'update picture for {base_folder}')

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:

                sql_foldername = foldername.replace("'", "''")
                sql_filename = filename.replace("'", "''")

                sql_str = (f'SELECT picture_id FROM {cls.table_files} '
                           f'WHERE file_path = \'{sql_foldername}\\\' AND '
                           f'file_name = \'{sql_filename}\'')

                cursor.execute(sql_str)
                try:
                    picture_id = cursor.fetchone()[0]

                except TypeError:
                    logger.info(
                        f'file {os.path.join(foldername, filename)} '
                        f'not found in database')
                    continue

                try:
                    im = Image.open(os.path.join(foldername, filename))
                    im.thumbnail(DATABASE_PICTURE_SIZE, Image.ANTIALIAS)

                except Exception as e:  #pylint: disable=broad-except
                    logger.info(
                        f'error found: {e} for file {os.path.join(foldername, filename)}')
                    continue

                cls.update_image(picture_id, im, 0)
                next(progress_message)

    @classmethod
    @DbUtils.connect
    def update_image_md5(cls, picture_id, image, rotate, cursor):
        '''  This method replaces the thumbnail and md5.
        '''
        img_bytes = io.BytesIO()
        image.save(img_bytes, format='JPEG')
        picture_bytes = img_bytes.getvalue()

        thumbnail = json.dumps(picture_bytes.decode(exif.codec))
        md5_signature = hashlib.md5(picture_bytes).hexdigest()

        sql_str = (
            f'UPDATE {cls.table_pictures} '
            f'SET thumbnail = (%s), '
            f'md5_signature = (%s), '
            f'rotate = (%s) '
            f'WHERE id= (%s) '
        )
        cursor.execute(sql_str, (thumbnail, md5_signature, rotate, picture_id))

    @classmethod
    @DbUtils.connect
    def replace_thumbnail_md5(cls, id_list, cursor):
        ''' patch to put back missing files of pictures that were already in the database
            but with different size thumbnail and md5
        '''
        progress_message = progress_message_generator(
            f'update picture and md5 for {id_list[0]} ... {id_list[-1]}')

        for picture_id in id_list:

            sql_str = (f'SELECT file_path, file_name FROM {cls.table_files} '
                       f'WHERE picture_id = {picture_id} ')

            cursor.execute(sql_str)
            result = cursor.fetchone()

            try:
                filename_abs = os.path.join(
                    result[0], result[1])

            except TypeError:
                logger.info(
                    f'file for {picture_id} not found in database'
                )
                continue

            try:
                im = Image.open(filename_abs)
                im.thumbnail(DATABASE_PICTURE_SIZE, Image.ANTIALIAS)

            except Exception as e:  #pylint: disable=broad-except
                logger.info(
                    f'error found: {e} for file {filename_abs}'
                )
                continue

            cls.update_image_md5(picture_id, im, 0)
            next(progress_message)
