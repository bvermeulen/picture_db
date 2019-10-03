import datetime
from picture_db import PictureDb

picdb = PictureDb()
# BASE_FOLDER = 'd:\\pictures'
BASE_FOLDER = 'd:\\test_pictures'

def run_delete_tables():
    picdb.delete_reviews_table()
    picdb.delete_files_table()
    picdb.delete_pictures_table()


def run_create_tables():
    picdb.create_pictures_table()
    picdb.create_files_table()
    picdb.create_reviews_table()


def run_delete_reviews_table():
    picdb.delete_reviews_table()


def run_create_reviews_table():
    picdb.create_reviews_table()


def run_fill_pic_base():
    picdb.store_pictures_base_folder(BASE_FOLDER)


def run_load_picture():
    picdb.load_picture_meta(12500)


def run_merge_pictures():
    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


def run_remove_duplicates(method='md5'):
    deleted_folder = 'd:\\Pics_deleted'
    accepted_review_date = datetime.datetime(2019, 10, 1)
    picdb.remove_duplicate_pics(deleted_folder, method=method,
                                accepted_review_date=accepted_review_date)


def run_update_pic_base():
    picdb.update_pictures_base_folder(BASE_FOLDER)


if __name__ == '__main__':
    # run_delete_tables()
    # run_create_tables()
    # run_fill_pic_base()
    # run_load_picture()
    # run_merge_pictures()
    # run_create_reviews_table()
    # run_delete_reviews_table()
    # run_remove_duplicates(method='date')
    run_update_pic_base()
