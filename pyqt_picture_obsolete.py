import sys
import io
from enum import Enum
import json
from PIL import Image
from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QLabel, QApplication, QPushButton,
    QShortcut
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QImage, QPixmap
from picture_db import PictureDb

anticlockwise_symbol = '\u21b6'
clockwise_symbol = '\u21b7'
right_arrow_symbol = '\u25B6'
left_arrow_symbol = '\u25C0'

class Mode(Enum):
    Single = 1
    Multi = 2


def pil2pixmap(pil_image):
    if pil_image is None:
        return None

    bytes_img = io.BytesIO()
    pil_image.save(bytes_img, format='JPEG')

    qimg = QImage()
    qimg.loadFromData(bytes_img.getvalue())

    return QPixmap.fromImage(qimg)


def meta_to_text(pic_meta, file_meta, lat_lon_str, index=None, total=None):
    try:
        _date_pic = pic_meta.date_picture.strftime("%d-%b-%Y %H:%M:%S")

    except AttributeError:
        _date_pic = None

    text = (
        f'id: {pic_meta.id:6}\n'
        f'file name: {file_meta.file_name}\n'
        f'file path: {file_meta.file_path}\n'
        f'file modified: {file_meta.file_modified.strftime("%d-%b-%Y %H:%M:%S")}\n'
        f'date picture: {_date_pic}\n'
        f'md5: {pic_meta.md5_signature}\n'
        f'camera make: {pic_meta.camera_make}\n'
        f'camera model: {pic_meta.camera_model}\n'
        f'location: {lat_lon_str}\n'
        f'file check: {file_meta.file_checked}\n'
        f'rotate: {pic_meta.rotate:3}\n'
        f'rotate_checked: {pic_meta.rotate_checked}'
    )

    if index is not None:
        text += f'\nindex: {index+1} of {total}'

    return text


class PictureShow(QWidget):

    def __init__(self, mode=Mode.Single):
        super().__init__()

        self.mode = mode
        self.picdb = PictureDb()

        self.rotate = None
        self.index = None
        self.total = None
        self.id_list = []

        self.initUI()

    def initUI(self):
        vbox = QVBoxLayout()

        hbox_pic_text = QHBoxLayout()

        self.pic_lbl = QLabel()
        hbox_pic_text.addWidget(self.pic_lbl)
        self.text_lbl = QLabel()
        self.text_lbl.setAlignment(Qt.AlignTop)
        hbox_pic_text.addWidget(self.text_lbl)

        hbox_buttons = QHBoxLayout()
        quit_button = QPushButton('Quit')
        quit_button.clicked.connect(self.cntr_quit)
        if self.mode == Mode.Multi:
            prev_button = QPushButton(left_arrow_symbol)
            prev_button.clicked.connect(self.cntr_prev)
            next_button = QPushButton(right_arrow_symbol)
            next_button.clicked.connect(self.cntr_next)

        save_button = QPushButton('save')
        save_button.clicked.connect(self.cntr_save)
        clockwise_button = QPushButton(clockwise_symbol)
        clockwise_button.clicked.connect(self.rotate_clockwise)
        anticlockwise_button = QPushButton(anticlockwise_symbol)
        anticlockwise_button.clicked.connect(self.rotate_anticlockwise)
        # hbox_buttons.addStretch()
        hbox_buttons.setAlignment(Qt.AlignLeft)
        hbox_buttons.addWidget(anticlockwise_button)
        hbox_buttons.addWidget(clockwise_button)
        if self.mode == Mode.Multi:
            hbox_buttons.addWidget(prev_button)
            hbox_buttons.addWidget(next_button)

        hbox_buttons.addWidget(save_button)
        hbox_buttons.addWidget(quit_button)

        vbox.addLayout(hbox_pic_text)
        vbox.addLayout(hbox_buttons)

        self.setLayout(vbox)

        if self.mode == Mode.Multi:
            QShortcut(Qt.Key_Left, self, self.cntr_prev)
            QShortcut(Qt.Key_Right, self, self.cntr_next)

        QShortcut(Qt.Key_S, self, self.cntr_save)
        QShortcut(Qt.Key_Space, self, self.rotate_clockwise)

        self.move(400, 300)
        self.setWindowTitle('Picture ... ')
        self.show()

    def show_picture(self):
        pixmap = pil2pixmap(self.image)
        if not pixmap:
            return

        self.pic_lbl.setPixmap(pixmap)
        self.text = meta_to_text(
            self.pic_meta, self.file_meta, self.lat_lon_str,
            index=self.index, total=self.total)
        self.text_lbl.setText(self.text)

    def rotate_clockwise(self):
        # note degrees are defined in counter clockwise direction !
        if self.image:
            self.image = self.image.rotate(-90, expand=True, resample=Image.BICUBIC)
            self.rotate += 90
            self.rotate = self.rotate % 360
            self.pic_meta.rotate = self.rotate
            self.show_picture()

    def rotate_anticlockwise(self):
        # note degrees are defined in counter clockwise direction !
        if self.image:
            self.image = self.image.rotate(+90, expand=True, resample=Image.BICUBIC)
            self.rotate -= 90
            self.rotate = self.rotate % 360
            self.pic_meta.rotate = self.rotate
            self.show_picture()

    def cntr_select_pic(self, picture_id):
        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, _ = (
            self.picdb.load_picture_meta(picture_id))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_prev(self):
        self.index -= 1
        if self.index < 0:
            self.index = len(self.id_list) - 1

        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, _ = (
            self.picdb.load_picture_meta(self.id_list[self.index]))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_next(self):
        self.index += 1
        if self.index > len(self.id_list) - 1:
            self.index = 0

        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, _ = (
            self.picdb.load_picture_meta(self.id_list[self.index]))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_save(self):
        self.picdb.update_image(
            self.id_list[self.index], self.image, self.rotate)

    def call_by_id(self):
        picture_id = int(input('Picture id [0 to exit]: '))
        if picture_id == 0:
            self.cntr_quit()
            sys.exit()

        self.cntr_select_pic(picture_id)
        self.call_by_id()

    def call_by_list(self, id_list):
        self.id_list = id_list
        self.index = 0
        self.total = len(self.id_list)
        self.cntr_select_pic(self.id_list[self.index])

    def cntr_quit(self):
        self.close()


def main(mode=Mode.Single, filename=None, pic_ids=None):
    #pylint: disable='anomalous-backslash-in-string
    '''
    make a selection in psql, for example:
    \o name.json
    select json_build_object('id', json_agg(id)) from pictures
        where gps_latitude ->> 'ref' in ('N', 'S');
    '''
    app = QApplication([])

    if mode == Mode.Multi:
        if filename:
            with open(filename) as json_file:
                id_list = json.load(json_file)

        elif pic_ids:
            id_list = pic_ids

        else:
            print('either give json file or list of picture ids')
            sys.exit()

        pic_show = PictureShow(mode=mode)
        pic_show.call_by_list(id_list)

    elif mode == Mode.Single:
        pic_show = PictureShow(mode=mode)
        pic_show.call_by_id()

    else:
        print('incorrect mode')
        sys.exit()

    sys.exit(app.exec_())


if __name__ == '__main__':
    main(mode=Mode.Multi, filename='./ids.json')
    #, pic_ids=list(range(8500, 9000)))
