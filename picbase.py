from picture_db import PictureDb
import pyqt_picture
from pyqt_picture import Mode

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


def run_fill_pic_base():
    picdb.store_pictures_base_folder(BASE_FOLDER)


def run_merge_pictures():
    source_folder = 'd:\\Pics_google'
    destination_folder = 'd:\\Pics_unsorted'
    picdb.select_pics_for_merge(source_folder, destination_folder)


def run_update_picbase():
    picdb.check_and_add_files(BASE_FOLDER)
    # WARNING: below method should be run carefully. Check the database
    # which pictures will be deleted by runnning sql:
    # select picture_id, file_path, file_name from files where not file_checked;
    #picdb.check_and_remove_non_existing_files()


def run_show_picture():
    pyqt_picture.main(mode=Mode.Multi, pic_ids=list(range(28980, 28988)))


def run_remove_pics(method='md5', start_id=None, end_id=None):
    ''' removed pics by id if either start_id or end_id are give otherwise
        runs delete picture on check on method
    '''
    deleted_folder = 'd:\\Pics_deleted'
    if end_id:
        picdb.remove_pics_by_id(deleted_folder, start_id, end_id=end_id)

    elif start_id:
        picdb.remove_pics_by_id(deleted_folder, start_id)

    else:
        picdb.remove_duplicate_pics(deleted_folder, method=method)


def run_pic_gis(json_file):
    picdb.populate_locations_table(json_filename=json_file)


def run_update_rotate_checked(json_file):
    picdb.update_rotate_checked(json_file)


def run_replace_picture():
    picdb.replace_thumbnail(BASE_FOLDER)


def run_replace_picture_md5():
    id_list = [
        26414, 6855, 6865, 6869, 6874, 6882, 6883, 6922, 6926,
        6933, 6952, 6954, 6958, 6960, 6968, 7012, 7020, 7027, 7038,
        7061, 7065, 7077, 7083, 7091, 7103, 7111, 7116, 7145, 7593,
        7594, 20878, 21181, 21211, 21250, 21358, 21419, 21429, 21653,
        21700, 21755, 21789, 21810
    ]
    picdb.replace_thumbnail_md5(id_list)


if __name__ == '__main__':
    # run_delete_tables()
    # run_create_tables()
    # run_delete_reviews_table()
    # run_fill_pic_base()
    # run_remove_pics(method='md5')  # method='md4' or 'date'
    # run_pic_gis('id_with_location_009.json')
    # run_replace_picture()
    # run_merge_pictures()
    # run_update_picbase()
    run_update_rotate_checked('id_with_location_009.json')
    # run_show_picture()
