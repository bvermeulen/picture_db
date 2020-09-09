import datetime
from picture_db import PictureDb, Exif

picdb = PictureDb()
BASE_FOLDER = 'd:\\pictures'
# BASE_FOLDER = 'd:\\test_pictures'

def run_delete_tables():
    picdb.delete_table('reviews')
    picdb.delete_table('locations')
    picdb.delete_table('files')
    picdb.delete_table('pictures')


def run_create_tables():
    picdb.create_pictures_table()
    picdb.create_files_table()
    picdb.create_locations_table()
    picdb.create_reviews_table()


def run_delete_reviews_table():
    picdb.delete_table('reviews')


def run_create_reviews_table():
    picdb.create_reviews_table()


def run_fill_pic_base():
    picdb.store_pictures_base_folder(BASE_FOLDER)


def run_load_picture():
    picdb.load_picture_meta(23250)


def run_merge_pictures():
    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


def run_remove_duplicates(method='md5'):
    deleted_folder = 'd:\\Pics_deleted'
    accepted_review_date = datetime.datetime(2019, 10, 1)
    picdb.remove_duplicate_pics(deleted_folder, method=method,
                                accepted_review_date=accepted_review_date)


def run_remove_by_id(start_id, end_id=None):
    deleted_folder = 'd:\\Pics_deleted'
    if end_id:
        picdb.remove_pics_by_id(deleted_folder, start_id, end_id=end_id)

    else:
        picdb.remove_pics_by_id(deleted_folder, start_id)


def run_update_pic_base():
    picdb.update_pictures_base_folder(BASE_FOLDER)


def run_load_pic_by_id():
    while True:
        answer = input("Give me a picture id: ")
        Exif().remove_display()
        picdb.load_picture_meta(int(answer))


def run_pic_gis():
    picdb.delete_table('locations')
    picdb.create_locations_table()
    picdb.populate_locations_table()


def run_replace_picture():
    picdb.replace_thumbnail(BASE_FOLDER)

if __name__ == '__main__':
    # run_delete_tables()
    # run_create_tables()
    # run_fill_pic_base()
    # run_load_picture()
    # run_merge_pictures()
    # run_create_reviews_table()
    # run_delete_reviews_table()
    # run_remove_duplicates(method='date')  # method='md4' or 'date'
    # run_remove_by_id(99999)
    # run_update_pic_base()
    run_load_pic_by_id()
    # run_pic_gis()
    # run_replace_picture()
