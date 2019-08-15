import os
import io
import time
import json
import datetime
from PIL import Image
import piexif
from pprint import pprint
import ast

filename = './pics/Various 018.jpg'
im = Image.open(filename)
codec = 'ISO-8859-1'  # or latin-1

exif_dict = piexif.load(im.info.get('exif'))

thumbnail = exif_dict.pop('thumbnail')
exif_data = {}
for ifd in exif_dict:
    exif_data[ifd] = {}
    for tag in exif_dict[ifd]:
        try:
           element =  exif_dict[ifd][tag].decode(codec)

        except AttributeError:
            element = exif_dict[ifd][tag]

        exif_data[ifd][piexif.TAGS[ifd][tag]["name"]] = element

exif_data['thumbnail'] = thumbnail.decode(codec)

exif_json = json.dumps(exif_data)

exif_dict2 = json.loads(exif_json)
im = Image.open(io.BytesIO(exif_dict2.get('thumbnail').encode(codec)))
im.show()

pprint(exif_dict)
print('-'*80)
pprint(exif_data)
