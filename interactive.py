import ast
import os
import time
import io
import pickle
import json
import datetime
from PIL import Image
import piexif
from pprint import pprint
filename = './pics/Various 018.jpg'
file_last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(filename))
file_created = datetime.datetime.fromtimestamp(os.path.getctime(filename))
im = Image.open(filename)
exif_dict = piexif.load(im.info.get('exif'))
date_picture = datetime.datetime.strptime(exif_dict.get('0th').get(306).decode(),'%Y:%m:%d %H:%M:%S')
camera = exif_dict.get('0th').get(271).decode()
camera_model = exif_dict.get('0th').get(272).decode()
pprint(json.dumps(exif_dict), io.StringIO))
new_file = 'test.pydict'
with open(new_file, 'r') as file:
    dict_string = file.read()
exif_dict_read = ast.literal_eval(dict_string)
thumbnail = exif_dict_read.get('thumbnail')
im = Image.open(io.BytesIO(thumbnail))
im.show()