import datetime
from picture_db import PictureDb

picdb = PictureDb()


def run_delete_tables():
    picdb.delete_pictures_table()
    picdb.delete_reviews_table()


def run_create_tables():
    picdb.create_pictures_table()
    picdb.create_reviews_table()


def run_delete_reviews_table():
    picdb.delete_reviews_table()


def run_create_reviews_table():
    picdb.create_reviews_table()


def run_fill_pic_base():
    base_folder = 'd:\\pictures'
    picdb.store_pictures_base_folder(base_folder)


def run_load_picture():
    picdb.load_picture_meta(8147)


def run_merge_pictures():
    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


def run_remove_duplicates(method='md5'):
    deleted_folder = 'd:\\Pics_deleted'
    picdb.remove_duplicate_pics(deleted_folder, method=method)


if __name__ == '__main__':
    # run_delete_tables()
    # run_create_tables()
    # run_fill_pic_base()
    # run_load_picture()
    # run_merge_pictures()
    # run_create_reviews_table()
    # run_delete_reviews_table()
    run_remove_duplicates(method='date')
    # picdb.test_review_date(accepted_review_date=datetime.datetime(2019, 9, 30))
