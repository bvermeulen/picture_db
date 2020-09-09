#!/usr/bin/python

"""
ZetCode PyQt5 tutorial

In this example, we display an image
on the window.

Author: Jan Bodnar
Website: zetcode.com
"""
import sys
import io
from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QLabel, QApplication, QPushButton,
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QImage, QPixmap
from PIL import Image

clockwise_symbol = '\u21b6'
anticlockwise_symbol = '\u21b7'

def pil2pixmap(pil_image):
    bytes_img = io.BytesIO()
    pil_image.save(bytes_img, format='JPEG')

    qimg = QImage()
    qimg.loadFromData(bytes_img.getvalue())

    return QPixmap.fromImage(qimg)


class Example(QWidget):

    def __init__(self):
        super().__init__()

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
        quit_button = QPushButton("Quit")
        quit_button.clicked.connect(self.cntr_quit)
        clockwise_button = QPushButton(clockwise_symbol)
        clockwise_button.clicked.connect(self.rotate_clockwise)
        anticlockwise_button = QPushButton(anticlockwise_symbol)
        anticlockwise_button.clicked.connect(self.rotate_anticlockwise)
        # hbox_buttons.addStretch()
        hbox_buttons.setAlignment(Qt.AlignLeft)
        hbox_buttons.addWidget(clockwise_button)
        hbox_buttons.addWidget(anticlockwise_button)
        hbox_buttons.addWidget(quit_button)

        vbox.addLayout(hbox_pic_text)
        vbox.addLayout(hbox_buttons)

        self.setLayout(vbox)

        self.move(400, 300)
        self.setWindowTitle('Picture ... ')
        self.show()

        self.image = Image.open('.\\pics\\resize1.jpg')
        self.show_picture()

    def show_picture(self):
        pixmap = pil2pixmap(self.image)
        self.pic_lbl.setPixmap(pixmap)
        self.text_lbl.setText('Quota est non volare in permiso')

    def rotate_clockwise(self):
        self.image = self.image.rotate(+90)
        self.show_picture()

    def rotate_anticlockwise(self):
        self.image = self.image.rotate(-90)
        self.show_picture()

    def cntr_quit(self):
        QApplication.quit()


def main():
    app = QApplication([])
    _ = Example()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
