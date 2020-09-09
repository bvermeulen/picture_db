import gc
from tkinter import (
    Tk, Frame, Button, Label,
)
from PIL import ImageTk, Image

clockwise_symbol = '\u21b6'
anticlockwise_symbol = '\u21b7'

class PicDb:
    def __init__(self):
        self.image_names = ['.\\pics\\resize1.jpg', '.\\pics\\resize2.jpg', '.\\pics\\IMG_0780.JPG']
        self.index = -1

    def next_image(self):
        self.index += 1
        self.index = self.index % 3
        image = Image.open(self.image_names[self.index])
        image.thumbnail((600, 600))
        return image


class PicGUI:

    def __init__(self, root):
        self.root = root
        self.root.title('photo shower')
        self.button_area()
        self.pdb = PicDb()
        self._image = self.pdb.next_image()
        self.photo_area()

    def photo_area(self):
        img = ImageTk.PhotoImage(self._image)
        self.photo_label = Label(self.root, image=img)
        # keep a reference to the image
        self.photo_label.image = img
        self.photo_label.grid(row=0, column=0, sticky='nw')

    def button_area(self):
        button_frame = Frame(self.root)
        button_frame.grid(row=1, column=0, sticky='new')

        self.clockwise_button = Button(
            button_frame, text=clockwise_symbol,
            command=self.cntr_rotate_clockwise)
        self.clockwise_button.pack(side='left')

        self.anticlockwise_button = Button(
            button_frame, text=anticlockwise_symbol,
            command=self.cntr_rotate_anticlockwise)
        self.anticlockwise_button.pack(side='left')

        self.save_button = Button(
            button_frame, text='save', command=self.cntr_save_image)
        self.save_button.pack(side='left')

        self.quit_button = Button(
            button_frame, text='next', command=self.cntr_next)
        self.quit_button.pack(side='left')

    def cntr_rotate_clockwise(self):
        self.photo_label.destroy()
        gc.collect()
        self._image = self._image.rotate(+90)
        self.photo_area()

    def cntr_rotate_anticlockwise(self):
        self.photo_label.destroy()
        gc.collect()
        self._image = self._image.rotate(-90)
        self.photo_area()

    def cntr_save_image(self):
        self._save_image_flag = True

    def cntr_next(self):
        # delete the label and run the garbage collection
        self.photo_label.destroy()
        gc.collect()
        self._image = self.pdb.next_image()
        self.photo_area()


def main():
    root = Tk()
    PicGUI(root)
    root.mainloop()

if __name__ == '__main__':
    main()
