import psycopg2
import os
import ast
import io
import json
import datetime
from collections import namedtuple
from PIL import Image
import piexif
from pprint import pprint

host = 'localhost'
db_user = 'db_tester'
db_user_pw = 'db_tester_pw'
database = 'picture_base'
codec = 'ISO-8859-1'  # or latin-1

class Exif:

    def exif_to_json(cls, exif_dict):
        thumbnail = exif_dict.pop('thumbnail')
        exif_data = {}
        exif_data['thumbnail'] = thumbnail.decode(codec)

        for ifd in exif_dict:
            exif_data[ifd] = {}
            for tag in exif_dict[ifd]:
                try:
                   element =  exif_dict[ifd][tag].decode(codec)

                except AttributeError:
                    element = exif_dict[ifd][tag]

                exif_data[ifd][piexif.TAGS[ifd][tag]["name"]] = element

        return json.dumps(exif_data)


# open database
connect_string = f'host=\'{host}\' dbname=\'{database}\' user=\'{db_user}\' password=\'{db_user_pw}\''
print(connect_string)
# con = psycopg2.connect(connect_string)
# cur = con.cursor()

with psycopg2.connect(connect_string) as connection:

    cur = connection.cursor()
    table_name = 'pictures'
    try:
        # create table
        PicturesTable = namedtuple('PicturesTable', 'id, file_name, file_last_modified, file_created, exif')
        pics_tbl = PicturesTable(id='Id INTEGER PRIMARY KEY',
                                file_name = 'File_name VARCHAR(250)',
                                file_last_modified = 'File_modified TIMESTAMP',
                                file_created = 'File_created TIMESTAMP',
                                exif = 'Exif JSON')

        sql_string = f'CREATE TABLE {table_name}({pics_tbl.id}, {pics_tbl.file_name}, '\
                     f'{pics_tbl.file_last_modified}, {pics_tbl.file_created}, {pics_tbl.exif});'
        print(sql_string)
        cur.execute(sql_string)

    except Exception as e:
        print(f'================> error creating table {e}')

    try:
        filename = './pics/Various 018.jpg'
        file_last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(filename))
        file_created = datetime.datetime.fromtimestamp(os.path.getctime(filename))
        im = Image.open(filename)
        exif_dict = piexif.load(im.info.get('exif'))
        exif_json = Exif().exif_to_json(exif_dict)

        sql_string = f'INSERT INTO {table_name} (Id, File_name, File_modified, File_created, Exif) VALUES (%s, %s, %s, %s, %s);'
        print(sql_string)
        cur.execute(sql_string, (1, filename, file_last_modified, file_created, exif_json))

    except Exception as e:
        print(f'================> error inserting data {e}')

with psycopg2.connect(connect_string) as connection:

    cur = connection.cursor()
    table_name = 'pictures'

    sql_string = f'SELECT * FROM {table_name};'
    print(sql_string)
    cur.execute(sql_string)
    data_read_from_db = cur.fetchall()
    exif_dict_db = data_read_from_db[0][4]
    im = Image.open(io.BytesIO(exif_dict_db.get('thumbnail').encode(codec)))
    im.show()

    print(data_read_from_db[0][:4])
    Make = data_read_from_db[0][4]['0th']['Make']
    Model = data_read_from_db[0][4]['0th']['Model']
    print(f'Make: {Make}, Model: {Model}')


