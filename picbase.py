from picture_db import PictureDb

picdb = PictureDb()


def run_delete_table():
    picdb.delete_pictures_table()


def run_create_table():
    picdb.create_pictures_table()


def run_fill_pic_base():
    base_folder = 'd:\\pictures'
    picdb.store_pictures_base_folder(base_folder)


def run_load_picture():
    picdb.load_picture_meta(3312)


def run_merge_pictures():
    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


def run_remove_duplicates():
    deleted_folder = 'd:\\Pics_deleted'
    picdb.remove_duplicate_pics(deleted_folder, method='md5')


if __name__ == '__main__':
    # run_delete_table()
    # run_create_table()
    # run_fill_pic_base()
    # run_load_picture()
    # run_merge_pictures()
    run_remove_duplicates()
