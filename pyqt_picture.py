'''
    This script interactively displays pictures from the picture_db database by typing:
    >>>python pyqt_picture.py [db_filter] [json_file]
    >>>
    whereby db_filter and json_file are optional. json_file is a json file containing a
    list of picture ids, like [100, 101, 102]
    db_filter can have the values:
        ALL, no filter applied
        NOGPS, pictures that have no coordinates
        CHECKED, pictures where rotate flag is set true
        NOT_CHECKED, pictures where rotate flag is false
    The default filter is ALL.
    Interactively pictures can be selected by the folder a pictures was originally stored
    or by the creation date.
'''
import sys
import json
import io
from pathlib import PureWindowsPath
from PIL import Image
from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QFormLayout, QLabel, QApplication, QPushButton,
    QComboBox, QShortcut, QLineEdit, QCalendarWidget, QDialog
)
from PyQt5.QtCore import Qt, QDate
from PyQt5.QtGui import QImage, QPixmap
from picture_exif import Exif
from picture_db import PictureDb, DbFilter

anticlockwise_symbol = '\u21b6'
clockwise_symbol = '\u21b7'
right_arrow_symbol = '\u25B6'
left_arrow_symbol = '\u25C0'
dialogue_rel_position = (10, 285)
date_widget_size = (400, 250)
folder_widget_size = (520, 192)
exif = Exif()


def parse_argv(argv):
    ''' arguments: [DbFilter] [json file with list]
        no argument or more then 2 arguments return:
            (default_db_filter, [])
        one argument returns:
            (default_db_filter, []), (db_filter, []) or (default_db_filter, [n1..nn])
        two arguments return:
            (defaukt_db_filter, []), (db_filter, [n1..nn]), (db_filter, []) or (default_db_filter, [n1..nn])
    '''
    default_db_filter = DbFilter.ALL
    default = (default_db_filter, [])

    if not argv or len(argv) < 2:
        return default

    elif len(argv) == 2:
        try:
            with open(argv[1], 'r') as jsonfile:
                ids = json.load(jsonfile)

            if isinstance(ids, list):
                return (default_db_filter, ids)

        except (FileNotFoundError, json.decoder.JSONDecodeError):
            try:
                return (DbFilter[argv[1].upper()], [])

            except KeyError:
                return default

    elif len(argv) == 3:
        try:
            with open(argv[2], 'r') as jsonfile:
                ids = json.load(jsonfile)

            if not isinstance(ids, list):
                raise ValueError

        except (ValueError, FileNotFoundError, json.decoder.JSONDecodeError):
            ids = []

        try:
            return (DbFilter[argv[1].upper()], ids)

        except KeyError:
            return (default_db_filter, ids)

    else:
        return default


def pil2pixmap(pil_image):
    if pil_image is None:
        return None

    bytes_img = io.BytesIO()
    try:
        pil_image.save(bytes_img, format='JPEG')
    except:
        return None

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
        f'file created: {file_meta.file_created.strftime("%d-%b-%Y %H:%M:%S")}\n'
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

class DateDialog(QDialog):
    def __init__(self, parent):
        super().__init__()
        self.setWindowTitle('Select date ...')
        self.setGeometry(
            parent.pos().x() + dialogue_rel_position[0] + 70,
            parent.pos().y() + dialogue_rel_position[1],
            date_widget_size[0] + 10, date_widget_size[1] + 10
        )
        self.date_widget = QCalendarWidget(self)
        self.date_widget.setGeometry(5, 5, date_widget_size[0], date_widget_size[1])
        self.date_widget.clicked[QDate].connect(self.clicked)

    def clicked(self):
        self.accept()

    @property
    def date(self):
        return self.date_widget.selectedDate().toPyDate()


class FolderDialog(QDialog):
    def __init__(self, folders, parent):
        super().__init__()
        self.setWindowTitle('Select folder ...')
        self.setGeometry(
            parent.pos().x() + dialogue_rel_position[0],
            parent.pos().y() + dialogue_rel_position[1],
            folder_widget_size[0] + 10, folder_widget_size[1] + 10
        )
        self.folder_widget = QComboBox(self)
        self.folder_widget.setGeometry(5, 5, folder_widget_size[0], 20)
        self.folder_widget.addItems(['Select folder'] + folders)
        self.folder_widget.currentIndexChanged.connect(self.selected)

    def selected(self):
        self.accept()

    @property
    def folder(self):
        return self.folder_widget.currentText()

class PictureShow(QWidget):

    def __init__(self, argv):
        super().__init__()
        self.db_filter, self.id_list = parse_argv(argv)
        self.picdb = PictureDb()
        self.rotate = None
        self.index = None
        self.total = None
        self.file_path = None
        self.lat_lon_str = ''
        self.lat_lon_val = (None, None, None)
        self.initUI()
        if self.id_list:
            self.id_list = self.picdb.filter_ids(self.id_list, db_filter=self.db_filter)
            self.update_id_list()
            self.show_picture()

    def initUI(self):
        vbox = QVBoxLayout()
        hbox_pic_action = QHBoxLayout()
        self.pic_lbl = QLabel()

        vbox_text_action = QVBoxLayout()
        self.text_lbl = QLabel()
        self.text_lbl.setAlignment(Qt.AlignTop)

        box_form = QFormLayout()
        self.e_make = QLineEdit()
        self.e_make.editingFinished.connect(self.update_attributes)
        self.e_model = QLineEdit()
        self.e_model.editingFinished.connect(self.update_attributes)
        self.e_pic_date = QLineEdit()
        self.e_pic_date.editingFinished.connect(self.update_attributes)
        self.e_lat_lon = QLineEdit()
        self.e_lat_lon.editingFinished.connect(self.update_attributes)
        box_form.addRow('Camera make', self.e_make)
        box_form.addRow('Camera model', self.e_model)
        box_form.addRow('Date picture', self.e_pic_date)
        box_form.addRow('Latitude, Longitude', self.e_lat_lon)
        vbox_text_action.addWidget(self.text_lbl)
        vbox_text_action.addLayout(box_form)

        hbox_pic_action.addWidget(self.pic_lbl)
        hbox_pic_action.addLayout(vbox_text_action)

        hbox_buttons = QHBoxLayout()
        quit_button = QPushButton('Quit')
        quit_button.clicked.connect(self.cntr_quit)
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
        folderselect_button = QPushButton('Select folder')
        folderselect_button.clicked.connect(self.cntr_folderselect)
        dateselect_button = QPushButton('Select date')
        dateselect_button.clicked.connect(self.cntr_dateselect)

        confirm_rotations_button = QPushButton('Confirm changes')
        confirm_rotations_button.clicked.connect(self.cntr_confirm_changes)
        reset_rotations_button = QPushButton('Reset')
        reset_rotations_button.clicked.connect(self.cntr_reset_changes)

        # hbox_buttons.addStretch()
        hbox_buttons.setAlignment(Qt.AlignLeft)
        hbox_buttons.addWidget(folderselect_button)
        hbox_buttons.addWidget(dateselect_button)
        hbox_buttons.addWidget(anticlockwise_button)
        hbox_buttons.addWidget(clockwise_button)
        hbox_buttons.addWidget(prev_button)
        hbox_buttons.addWidget(next_button)
        hbox_buttons.addWidget(confirm_rotations_button)
        hbox_buttons.addWidget(reset_rotations_button)
        hbox_buttons.addWidget(save_button)
        hbox_buttons.addWidget(quit_button)

        vbox.addLayout(hbox_pic_action)
        vbox.addLayout(hbox_buttons)

        self.setLayout(vbox)

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
            print(self.id_list[self.index])
            return

        if self.pic_meta.date_picture is None:
            date_ = self.file_meta.file_modified

        else:
            date_ = self.pic_meta.date_picture

        self.pic_lbl.setPixmap(pixmap)
        self.text = meta_to_text(
            self.pic_meta, self.file_meta, self.lat_lon_str,
            index=self.index, total=self.total
        )
        self.text_lbl.setText(self.text)
        self.e_make.setText(self.pic_meta.camera_make)
        self.e_model.setText(self.pic_meta.camera_model)
        self.e_pic_date.setText(str(date_))
        self.e_lat_lon.setText(
            ', '.join([f'{v:0.5f}' for v in self.lat_lon_val
            if isinstance(v, (float, int))])
        )

    def rotate_clockwise(self):
        # note degrees are defined in counter clockwise direction !
        if self.image:
            self.image = self.image.rotate(-90, expand=True, resample=Image.Resampling.BICUBIC)
            self.rotate += 90
            self.rotate = self.rotate % 360
            self.pic_meta.rotate = self.rotate
            self.show_picture()

    def rotate_anticlockwise(self):
        # note degrees are defined in counter clockwise direction !
        if self.image:
            self.image = self.image.rotate(+90, expand=True, resample=Image.Resampling.BICUBIC)
            self.rotate -= 90
            self.rotate = self.rotate % 360
            self.pic_meta.rotate = self.rotate
            self.show_picture()

    def cntr_select_pic(self, picture_id):
        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, self.lat_lon_val = (
            self.picdb.load_picture_meta(picture_id))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_prev(self):
        if self.index is None:
            return

        self.index -= 1
        if self.index < 0:
            self.index = len(self.id_list) - 1

        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, self.lat_lon_val = (
            self.picdb.load_picture_meta(self.id_list[self.index]))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_next(self):
        if self.index is None:
            return

        self.index += 1
        if self.index > len(self.id_list) - 1:
            self.index = 0

        self.image, self.pic_meta, self.file_meta, self.lat_lon_str, self.lat_lon_val = (
            self.picdb.load_picture_meta(self.id_list[self.index]))

        if self.pic_meta:
            self.rotate = self.pic_meta.rotate
            self.show_picture()

    def cntr_save(self):
        if self.index is None:
            return

        self.picdb.store_attributes(
            self.id_list[self.index], self.image, self.pic_meta)

    def update_id_list(self):
        if self.id_list:
            self.index = 0
            self.total = len(self.id_list)
            self.cntr_select_pic(self.id_list[self.index])

        else:
            self.index = None
            self.total = None
            self.cntr_select_pic(-1)

    def cntr_folderselect(self):
        folder_dialog = FolderDialog(self.picdb.get_file_paths(), self)
        if folder_dialog.exec_():
            folder = str(
                PureWindowsPath(folder_dialog.folder)
            ).lower().replace('\\', '\\\\')
            id_list = self.picdb.get_ids_by_folder(folder)
            self.id_list = self.picdb.filter_ids(id_list, db_filter=self.db_filter)
            self.update_id_list()

    def cntr_dateselect(self):
        date_dialog = DateDialog(self)
        if date_dialog.exec_():
            id_list = self.picdb.get_ids_by_date(date_dialog.date)
            self.id_list = self.picdb.filter_ids(id_list, db_filter=self.db_filter)
            self.update_id_list()

    def cntr_confirm_changes(self):
        self.picdb.set_rotate_check(self.id_list, set_value=True)
        self.picdb.populate_locations_table(picture_ids=self.id_list)

    def cntr_reset_changes(self):
        self.picdb.set_rotate_check(self.id_list, set_value=False)
        self.picdb.remove_from_locations_table(picture_ids=self.id_list)

    def update_attributes(self):
        if self.index is None:
            return

        self.pic_meta.camera_make = self.e_make.text()
        self.pic_meta.camera_model = self.e_model.text()
        self.pic_meta.date_picture = exif.format_date(self.e_pic_date.text())
        (
            self.pic_meta.gps_latitude, self.pic_meta.gps_longitude,
            self.pic_meta.gps_altitude, self.pic_meta.gps_img_direction
        ) = exif.decimalgps_to_json(self.e_lat_lon.text())

    def cntr_quit(self):
        self.close()


def main():
    app = QApplication([])
    _ = PictureShow(sys.argv)
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
