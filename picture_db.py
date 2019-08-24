import os
import io
import hashlib
import json
import datetime
from functools import wraps
import psycopg2
from PIL import Image
import piexif
from recordtype import recordtype


def progress_message_generator(message):
    loop_dash = ['\u2014', '\\', '|', '/']
    i = 1
    print_interval = 1
    while True:
        print(
            f'\r{loop_dash[int(i/print_interval) % 4]} {i} {message}', end='')
        i += 1
        yield


class Exif:
    ''' utility methods to handle picture exif
    '''
    codec = 'ISO-8859-1'  # or latin-1

    @classmethod
    def exif_to_tag(cls, exif_dict):
        exif_tag_dict = {}
        thumbnail = exif_dict.pop('thumbnail')
        exif_tag_dict['thumbnail'] = thumbnail.decode(cls.codec)

        for ifd in exif_dict:
            exif_tag_dict[ifd] = {}
            for tag in exif_dict[ifd]:
                try:
                    element = exif_dict[ifd][tag].decode(cls.codec)

                except AttributeError:
                    element = exif_dict[ifd][tag]

                exif_tag_dict[ifd][piexif.TAGS[ifd][tag]["name"]] = element

        return exif_tag_dict

    @staticmethod
    def convert_gps(gps_latitude, gps_longitude, gps_altitude):
        ''' input based on tuples of fractions
        '''
        def convert_to_degrees(lat_long_value):
            ref = lat_long_value.get('ref', '')
            fractions = lat_long_value.get('pos', [0, 1])
            degrees = fractions[0][0] / fractions[0][1]
            minutes = fractions[1][0] / fractions[1][1]
            seconds = fractions[2][0] / fractions[2][1]

            if fractions[1][0] == 0 and fractions[2][0] == 0:
                return f'{ref} {degrees:.4f}\u00B0'

            elif fractions[2][0] == 0:
                return f'{ref} {degrees:.0f}\u00B0 {minutes:.2f}"'

            else:
                return f'{ref} {degrees:.0f}\u00B0 {minutes:.0f}" {seconds:.0f}\''

        try:
            latitude = convert_to_degrees(gps_latitude)
            longitude = convert_to_degrees(gps_longitude)
            alt_fraction = gps_altitude.get('alt')
            altitude = f'{alt_fraction[0]/ alt_fraction[1]:.2f}'

            return latitude, longitude, altitude

        except (TypeError, AttributeError, ZeroDivisionError) as error:
            print(f'unable to convert coordinate: {error}')
            return None, None, None

    @classmethod
    def exif_to_json(cls, exif_tag_dict):
        try:
            return json.dumps(exif_tag_dict)

        except Exception as e:
            print(f'Convert exif to tag first, error is {e}')
            raise()


class DbUtils:
    '''  utility methods for database
    '''
    host = 'localhost'
    db_user = 'db_tester'
    db_user_pw = 'db_tester_pw'
    database = 'picture_base'

    @classmethod
    def connect(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            connect_string = f'host=\'{cls.host}\' dbname=\'{cls.database}\''\
                             f'user=\'{cls.db_user}\' password=\'{cls.db_user_pw}\''
            result = None
            try:
                connection = psycopg2.connect(connect_string)
                cursor = connection.cursor()
                result = func(*args, cursor, **kwargs)
                connection.commit()
                print('sql action succes')

            except psycopg2.Error as error:
                print(f'error while connect to PostgreSQL {cls.database}: '
                      f'{error}')

            finally:
                if connection:
                    cursor.close()
                    connection.close()
                    print(f'PostgreSQL connection to {cls.database} is closed')

            return result

        return wrapper

    @staticmethod
    def get_cursor(cursor):
        if cursor:
            return cursor[0]

        else:
            print('unable to connect to database')
            raise()


class PictureDb:
    table_name = 'pictures'
    PicturesTable = recordtype('PicturesTable',
                               'id, file_path, file_name,'
                               'file_modified, file_created, file_size,'
                               'date_picture, md5_signature, camera_make, camera_model,'
                               'gps_latitude, gps_longitude, gps_altitude,'
                               'gps_img_direction, thumbnail, exif')

    @classmethod
    @DbUtils.connect
    def create_pictures_table(cls, *args):
        cursor = DbUtils().get_cursor(args)

        pics_tbl = cls.PicturesTable(
            id='id SERIAL PRIMARY KEY',
            file_path='file_path VARCHAR(250)',
            file_name='file_name VARCHAR(250)',
            file_modified='file_modified TIMESTAMP',
            file_created='file_created TIMESTAMP',
            file_size='file_size INTEGER',
            date_picture='date_picture TIMESTAMP',
            md5_signature='md5_signature VARCHAR(32)',
            camera_make='camera_make VARCHAR(50)',
            camera_model='camera_model VARCHAR(50)',
            gps_latitude='gps_latitude JSON',
            gps_longitude='gps_longitude JSON',
            gps_altitude='gps_altitude JSON',
            gps_img_direction='gps_img_dir JSON',
            thumbnail='thumbnail JSON',
            exif='exif JSON')

        sql_string = f'CREATE TABLE {cls.table_name}'\
                     f'({pics_tbl.id}, {pics_tbl.file_path}, {pics_tbl.file_name}, '\
                     f'{pics_tbl.file_modified}, {pics_tbl.file_created}, '\
                     f'{pics_tbl.file_size}, {pics_tbl.date_picture}, ' \
                     f'{pics_tbl.md5_signature}, '\
                     f'{pics_tbl.camera_make}, {pics_tbl.camera_model}, '\
                     f'{pics_tbl.gps_latitude}, {pics_tbl.gps_longitude}, '\
                     f'{pics_tbl.gps_altitude}, {pics_tbl.gps_img_direction}, '\
                     f'{pics_tbl.thumbnail}, {pics_tbl.exif});'

        print(f'create table {cls.table_name}')
        cursor.execute(sql_string)

    @classmethod
    def get_pic_meta(cls, filename):
        pic_meta = cls.PicturesTable(*[None]*16)

        # file attributes
        file_stat = os.stat(filename)
        pic_meta.file_name = os.path.basename(filename)
        pic_meta.file_path = os.path.abspath(filename).replace(pic_meta.file_name, '')
        pic_meta.file_modified = datetime.datetime.fromtimestamp(file_stat.st_mtime)
        pic_meta.file_created = datetime.datetime.fromtimestamp(file_stat.st_ctime)
        pic_meta.file_size = file_stat.st_size

        # exif attributes
        try:
            im = Image.open(filename)
        except OSError:
            return cls.PicturesTable(*[None]*16)

        thumbnail_bytes = b''

        try:
            exif_dict = piexif.load(im.info.get('exif'))
            exif_dict = Exif().exif_to_tag(exif_dict)

        except Exception:  #pylint: disable=W0703
            exif_dict = {}

        if exif_dict:
            thumbnail_bytes = exif_dict.pop('thumbnail').encode(Exif().codec)
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

            gps = exif_dict.get('GPS')
            if gps:
                pic_meta.gps_latitude = json.dumps(
                    {'ref': gps.get('GPSLatitudeRef'),
                     'pos': gps.get('GPSLatitude')})
                pic_meta.gps_longitude = json.dumps(
                    {'ref': gps.get('GPSLongitudeRef'),
                     'pos': gps.get('GPSLongitude')})
                pic_meta.gps_altitude = json.dumps(
                    {'ref': gps.get('GPSAltitudeRef'),
                     'alt': gps.get('GPSAltitude')})
                pic_meta.gps_img_direction = json.dumps(
                    {'ref': gps.get('GPSImgDirectionRef'),
                     'dir': gps.get('GPSImgDirection')})

            else:
                pic_meta.gps_latitude, pic_meta.gps_longitude, \
                pic_meta.gps_altitude, pic_meta.gps_img_direction = [json.dumps({})]*4

        else:
            pic_meta.camera_make, pic_meta.camera_model, \
                pic_meta.date_picture = None, None, None
            pic_meta.gps_latitude, pic_meta.gps_longitude, \
                pic_meta.gps_altitude, pic_meta.gps_img_direction = [json.dumps({})]*4

        if not thumbnail_bytes:
            size = (180, 180)
            im.thumbnail(size, Image.ANTIALIAS)
            img_bytes = io.BytesIO()
            im.save(img_bytes, format='JPEG')
            thumbnail_bytes = img_bytes.getvalue()

        pic_meta.thumbnail = json.dumps(thumbnail_bytes.decode(Exif().codec))
        pic_meta.md5_signature = hashlib.md5(thumbnail_bytes).hexdigest()
        pic_meta.exif = Exif().exif_to_json(exif_dict)

        return pic_meta

    @classmethod
    @DbUtils.connect
    def store_picture_meta(cls, filename, *args):
        pic_meta = cls.get_pic_meta(filename)
        if not pic_meta.file_name:
            return

        cursor = DbUtils().get_cursor(args)

        sql_string = f'INSERT INTO {cls.table_name} ('\
                     f'file_path, file_name, file_modified, file_created, file_size, '\
                     f'date_picture, md5_signature, camera_make, camera_model, '\
                     f'gps_latitude, gps_longitude, gps_altitude, gps_img_dir, '\
                     f'thumbnail, exif) '\
                     f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '\
                     f'%s, %s, %s, %s, %s);'

        print(f'store meta data for {filename}')
        cursor.execute(sql_string, (
            pic_meta.file_path, pic_meta.file_name,
            pic_meta.file_modified, pic_meta.file_created, pic_meta.file_size,
            pic_meta.date_picture, pic_meta.md5_signature, pic_meta.camera_make,
            pic_meta.camera_model, pic_meta.gps_latitude, pic_meta.gps_longitude,
            pic_meta.gps_altitude, pic_meta.gps_img_direction, pic_meta.thumbnail,
            pic_meta.exif))


    @classmethod
    @DbUtils.connect
    def store_pictures_base_folder(cls, base_folder, *args):
        cursor = DbUtils().get_cursor(args)
        progress_message = progress_message_generator(
            f'loading picture meta data from {base_folder}')

        sql_string = f'INSERT INTO {cls.table_name} ('\
                     f'file_path, file_name, file_modified, file_created, file_size, '\
                     f'date_picture, md5_signature, camera_make, camera_model, '\
                     f'gps_latitude, gps_longitude, gps_altitude, gps_img_dir, '\
                     f'thumbnail, exif) '\
                     f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '\
                     f'%s, %s, %s, %s, %s);'

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:
                if filename[-4:] in ['.jpg', '.JPG']:
                    # print(foldername, filename)
                    pic_meta = cls.get_pic_meta(os.path.join(foldername, filename))
                    if not pic_meta.file_name:
                        continue

                    cursor.execute(sql_string, (
                        pic_meta.file_path, pic_meta.file_name,
                        pic_meta.file_modified, pic_meta.file_created,
                        pic_meta.file_size, pic_meta.date_picture,
                        pic_meta.md5_signature, pic_meta.camera_make,
                        pic_meta.camera_model, pic_meta.gps_latitude,
                        pic_meta.gps_longitude, pic_meta.gps_altitude,
                        pic_meta.gps_img_direction, pic_meta.thumbnail,
                        pic_meta.exif))

                    next(progress_message)

        print()

    @classmethod
    @DbUtils.connect
    def load_picture_meta(cls, _id, *args):
        cursor = DbUtils().get_cursor(args)

        sql_string = f'SELECT * FROM {cls.table_name} where id = \'{_id}\';'
        print(f'load meta data for id {_id}')
        cursor.execute(sql_string)

        data_from_db = cursor.fetchone()

        if not data_from_db:
            return

        pic_meta = cls.PicturesTable(
            id=data_from_db[0],
            file_path=data_from_db[1],
            file_name=data_from_db[2],
            file_modified=data_from_db[3],
            file_created=data_from_db[4],
            file_size=data_from_db[5],
            date_picture=data_from_db[6],
            md5_signature=data_from_db[7],
            camera_make=data_from_db[8],
            camera_model=data_from_db[9],
            gps_latitude=data_from_db[10],
            gps_longitude=data_from_db[11],
            gps_altitude=data_from_db[12],
            gps_img_direction=data_from_db[13],
            thumbnail=data_from_db[14],
            exif=data_from_db[15],
        )

        if pic_meta.thumbnail:
            img_bytes = io.BytesIO(
                pic_meta.thumbnail.encode(Exif().codec))
            im = Image.open(img_bytes)
            im.show()

        for key, val in pic_meta._asdict().items():
            if key == 'thumbnail':
                continue

            print(f'{key}: {val}')

        latitude, longitude, altitude = Exif().\
            convert_gps(pic_meta.gps_latitude, pic_meta.gps_longitude,
                        pic_meta.gps_altitude)
        print(f'coordinate: {latitude}, {longitude}, altitude: {altitude}')

    @classmethod
    @DbUtils.connect
    def select_pics_for_merge(cls, source_folder, destination_folder, *args):
        '''  method that checks if picture if in the database. If it is
             not moves picture from source folder to the destination folder
        '''
        cursor = DbUtils().get_cursor(args)

        for foldername, _, filenames in os.walk(source_folder):
            for filename in filenames:
                if filename[-4:] not in ['.jpg', '.JPG']:
                    continue

                pic_meta = cls.get_pic_meta(os.path.join(foldername, filename))
                print(filename)

                sql_string = f'select id from {cls.table_name} where '\
                             f'\'{pic_meta.md5_signature}\'=md5_signature'
                print(sql_string)
                cursor.execute(sql_string)
                result = cursor.fetchone()
                print(f'result: {result}')
                if result:
                    continue

                if pic_meta.date_picture:
                    sql_string = f'select id from {cls.table_name} where '\
                                 f'date_picture = \'{pic_meta.date_picture}\''
                    print(sql_string)
                    cursor.execute(sql_string)
                    result = cursor.fetchone()
                    print(f'result: {result}')
                    if result:
                        continue

                else:
                    sql_string = f'select id from {cls.table_name} where '\
                                 f'file_modified timestamp \'{pic_meta.file_modified}\''
                    print(sql_string)
                    cursor.execute(sql_string)
                    result = cursor.fetchone()
                    print(f'result: {result}')
                    if result:
                        continue

                print(f'move file {filename} to {destination_folder}')


def test():
    picdb = PictureDb()
    # picdb.create_pictures_table()

    # base_folder = 'd:\\pictures'
    # picdb.store_pictures_base_folder(base_folder)

    # picdb.load_picture_meta(3312)

    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


if __name__ == '__main__':
    test()
