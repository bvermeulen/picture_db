import psycopg2
import os
import ast
import io
import json
import datetime
from functools import wraps
from collections import namedtuple
from PIL import Image
import piexif
from pprint import pprint


class Exif:
    codec = 'ISO-8859-1'  # or latin-1

    def exif_to_tag(cls, exif_dict):
        exif_tag_dict = {}
        thumbnail = exif_dict.pop('thumbnail')
        exif_tag_dict['thumbnail'] = thumbnail.decode(cls.codec)

        for ifd in exif_dict:
            exif_tag_dict[ifd] = {}
            for tag in exif_dict[ifd]:
                try:
                   element =  exif_dict[ifd][tag].decode(cls.codec)

                except AttributeError:
                    element = exif_dict[ifd][tag]

                exif_tag_dict[ifd][piexif.TAGS[ifd][tag]["name"]] = element

        return exif_tag_dict

    def exif_to_json(cls, exif_tag_dict):
        try:
            return json.dumps(exif_tag_dict)
        except Exception as e:
            print('Convert exif to tag first')
            raise()


class PictureDb:
    table_name = 'pictures'
    PicturesTable = namedtuple('PicturesTable',
                               'id, file_path, file_name,'
                               'file_last_modified, file_created, file_size,'
                               'date_picture, camera_make, camera_model,'
                               'gps_latitude, gps_longitude, gps_altitude,'
                               'gps_img_direction, thumbnail, exif')


    class DbDecorators:
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
                    with psycopg2.connect(connect_string) as connection:
                        cursor = connection.cursor()
                        result = func(*args, cursor, **kwargs)

                except (Exception, psycopg2.Error) as error:
                        print(f'error while connect to PostgreSQL {cls.database}: {error}')

                finally:
                    if connection:
                        cursor.close()
                        connection.close()
                        print(f'PostgreSQL connection to {cls.database} is closed')
                return result
            return wrapper


    @DbDecorators.connect
    def create_pictures_table(cls, *args):
        if args:
            cursor = args[0]

        else:
            print(f'no connection to database')
            return

        # create table
        pics_tbl = cls.PicturesTable(
            id='id SERIAL PRIMARY KEY',
            file_path='file_path VARCHAR(250)',
            file_name='file_name VARCHAR(250)',
            file_last_modified='file_modified TIMESTAMP',
            file_created='file_created TIMESTAMP',
            file_size='file_size INTEGER',
            date_picture='pic_date TIMESTAMP',
            camera_make='camera_make VARCHAR(20)',
            camera_model='camera_model VARCHAR(20)',
            gps_latitude='gps_latitude JSON',
            gps_longitude='gps_longitude JSON',
            gps_altitude='gps_altitude JSON',
            gps_img_direction='gps_img_dir JSON',
            thumbnail='thumbnail JSON',
            exif='exif JSON')

        sql_string = f'CREATE TABLE {cls.table_name}({pics_tbl.id}, {pics_tbl.file_path}, {pics_tbl.file_name}, '\
                     f'{pics_tbl.file_last_modified}, {pics_tbl.file_created}, {pics_tbl.file_size}, '\
                     f'{pics_tbl.date_picture}, {pics_tbl.camera_make}, {pics_tbl.camera_model}, '\
                     f'{pics_tbl.gps_latitude}, {pics_tbl.gps_longitude}, {pics_tbl.gps_altitude}, '\
                     f'{pics_tbl.gps_img_direction}, {pics_tbl.thumbnail}, {pics_tbl.exif});'

        print(sql_string)
        cursor.execute(sql_string)

    @DbDecorators.connect
    def store_picture_meta(cls, filename, *args):
        if args:
            cursor = args[0]

        else:
            print(f'no connection to database')
            return

        # file attributes
        file_stat = os.stat(filename)
        file_name = os.path.basename(filename)
        file_path = os.path.abspath(filename).replace(file_name, '')
        file_last_modified = datetime.datetime.fromtimestamp(file_stat.st_mtime)
        file_created = datetime.datetime.fromtimestamp(file_stat.st_ctime)
        file_size = file_stat.st_size

        # exif attributes
        im = Image.open(filename)
        thumbnail = ''

        try:
            exif_dict = piexif.load(im.info.get('exif'))
            exif_dict = Exif().exif_to_tag(exif_dict)

        except TypeError:
            exif_dict = {}

        if exif_dict:
            thumbnail = json.dumps(exif_dict.pop('thumbnail'))
            camera_make = exif_dict.get('0th').get('Make')
            camera_model = exif_dict.get('0th').get('Model')
            date_picture = datetime.datetime.strptime(exif_dict.get('0th').get('DateTime'), '%Y:%m:%d %H:%M:%S')
            gps = exif_dict.get('GPS')
            if gps:
                gps_latitude = json.dumps(
                                { 'ref': gps.get('GPSLatitudeRef'),
                                  'pos': gps.get('GPSLatitude')})
                gps_longitude = json.dumps(
                                { 'ref': gps.get('GPSLongitudeRef'),
                                  'pos': gps.get('GPSLongitude')})
                gps_altitude = json.dumps(
                                { 'ref': gps.get('GPSAltitudeRef'),
                                  'alt': gps.get('GPSAltitude')})
                gps_img_direction = json.dumps(
                                { 'ref': gps.get('GPSImgDirectionRef'),
                                  'dir': gps.get('GPSImgDirection')})

            else:
                gps_latitude, gps_longitude, gps_altitude, gps_img_direction = [json.dumps({})]*4

        else:
            camera_make, camera_model, date_picture = None, None, None
            gps_latitude, gps_longitude, gps_altitude, gps_img_direction = [json.dumps({})]*4

        if not thumbnail:
            size = (180, 180)
            im.thumbnail(size, Image.ANTIALIAS)
            im.show()
            img_bytes = io.BytesIO()
            im.save(img_bytes, format='JPEG')
            thumbnail = json.dumps(img_bytes.getvalue().decode(Exif().codec))

        exif_json = Exif().exif_to_json(exif_dict)

        sql_string = f'INSERT INTO {cls.table_name} ('\
                     f'file_path, file_name, file_modified, file_created, file_size, '\
                     f'pic_date, camera_make, camera_model, '\
                     f'gps_latitude, gps_longitude, gps_altitude, gps_img_dir, '\
                     f'thumbnail, exif) '\
                     f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

        print(sql_string)
        cursor.execute(sql_string, (file_path, file_name, file_last_modified, file_created, file_size,
                                    date_picture, camera_make, camera_model,
                                    gps_latitude, gps_longitude, gps_altitude, gps_img_direction,
                                    thumbnail, exif_json))

    @DbDecorators.connect
    def load_picture_meta(cls, id, *args):
        if args:
            cursor = args[0]

        else:
            print(f'no connection to database')
            return

        sql_string = f'SELECT * FROM {cls.table_name} where id = \'{id}\';'
        print(sql_string)
        cursor.execute(sql_string)

        data_from_db = cursor.fetchone()
        pic_meta = cls.PicturesTable(
            id=data_from_db[0],
            file_path=data_from_db[1],
            file_name=data_from_db[2],
            file_last_modified=data_from_db[3],
            file_created=data_from_db[4],
            file_size=data_from_db[5],
            date_picture=data_from_db[6],
            camera_make=data_from_db[7],
            camera_model=data_from_db[8],
            gps_latitude=data_from_db[9],
            gps_longitude=data_from_db[10],
            gps_altitude=data_from_db[11],
            gps_img_direction=data_from_db[12],
            thumbnail=data_from_db[13],
            exif=data_from_db[14],
        )

        if pic_meta.thumbnail:
            img_bytes = io.BytesIO(
                pic_meta.thumbnail.encode(Exif().codec))
            im = Image.open(img_bytes)
            im.show()

        for key, val in pic_meta._asdict().items():
            if key != 'thumbnail':
                print(f'{key}: {val}')


def test():
    filepath = './pics/'
    filenames = ['Burgers 014.jpg', 'Burgers 016.jpg','groep.jpg', 'IMG_2218.JPG',  'IMG_2219.JPG', 'IMG_2220.JPG',
                 'IMG_2221.JPG', 'IMG_2223.JPG', 'IMG_2224.JPG', 'IMG_2225.JPG', 'IMG_2226.JPG', 'IMG_2230.JPG',
                 'moonwalk.jpg', 'Various 013.jpg', 'Various 018.jpg']

    picdb = PictureDb()
    # picdb.create_pictures_table()

    # for pic in filenames:
    #     filename = filepath + pic
    #     picdb.store_picture_meta(filename)

    picdb.load_picture_meta(10)

if __name__ == '__main__':
    test()
