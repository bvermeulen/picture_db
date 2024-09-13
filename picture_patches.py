import os
import io
import datetime
import json
import hashlib
import shutil
import numpy as np
from picture_exif import Exif
from picture_db import DbUtils, PictureDb, progress_message_generator
from Utils.plogger import Logger

logger = Logger.getlogger()
exif = Exif()


class PictureDbPatches(PictureDb):

    @classmethod
    @DbUtils.connect
    def review_required(cls, accepted_review_date, picture_id, cursor):
        sql_string = (
            f"SELECT review_date FROM {cls.table_reviews} "
            f"WHERE picture_id={picture_id};"
        )
        cursor.execute(sql_string)
        latest_review_date = datetime.datetime(1800, 1, 1)
        for review_date in cursor.fetchall():
            if review_date[0] > latest_review_date:
                latest_review_date = review_date[0]

        if latest_review_date > accepted_review_date:
            return False
        else:
            return True

    @classmethod
    @DbUtils.connect
    def delete_ids(cls, deleted_ids, cursor):
        if deleted_ids:
            sql_string = (
                f"DELETE FROM {cls.table_pictures} WHERE id=any(array{deleted_ids});"
            )
            cursor.execute(sql_string)

    @classmethod
    @DbUtils.connect
    def update_reviews(cls, pic_selection, reviewer_name, cursor):
        for pic in pic_selection:
            sql_string = (
                f"INSERT INTO {cls.table_reviews} ("
                f"picture_id, reviewer_name, review_date) "
                f"VALUES (%s, %s, %s);"
            )
            cursor.execute(
                sql_string, (pic.get("id"), reviewer_name, datetime.datetime.now())
            )

    @classmethod
    @DbUtils.connect
    def remove_duplicate_pics(
        cls,
        deleted_folder,
        cursor,
        method="md5",
        accepted_review_date=datetime.datetime(1900, 1, 1),
    ):
        """sort out duplicate pictures by either using the md5_signature or picture
        date.
        """
        utils = DbUtils()
        reviewer_name = utils.get_name()

        if method == "md5":
            method = "md5_signature"

        elif method == "date":
            method = "date_picture"

        else:
            print(f"{method} not valid, choose 'md5' or 'time'...")
            return

        log_file = os.path.join(deleted_folder, "_delete_duplicate_pictures.log")
        with open(log_file, "at") as f:
            c_time = datetime.datetime.now()
            f.write(f"===> Remove duplicates with method '{method}': {c_time}\n")

        sql_string = (
            f"SELECT {method} FROM {cls.table_pictures} WHERE {method} IN "
            f"(SELECT {method} FROM {cls.table_pictures} GROUP BY {method} "
            f"HAVING count(*) > 1) ORDER BY id;"
        )
        cursor.execute(sql_string)
        list_duplicates = {item[0] for item in cursor.fetchall()}

        for item in list_duplicates:
            sql_string = (
                f"SELECT id, thumbnail "
                f"FROM {cls.table_pictures} WHERE {method}='{item}';"
            )
            cursor.execute(sql_string)

            pic_selection = []
            choices = []
            for i, pic_tuple in enumerate(cursor.fetchall()):
                sql_string = (
                    f"SELECT file_path, file_name "
                    f"FROM {cls.table_files} WHERE picture_id={pic_tuple[0]};"
                )
                cursor.execute(sql_string)
                file_path, file_name = cursor.fetchone()

                if not cls.review_required(accepted_review_date, pic_tuple[0]):
                    print(
                        f"no review required for: "
                        f"{pic_tuple[0]}, {file_path}, {file_name}"
                    )
                    continue
                else:
                    pass

                choices.append(i + 1)
                pic_selection.append(
                    {
                        "index": i + 1,
                        "id": pic_tuple[0],
                        "file_path": file_path,
                        "file_name": file_name,
                        "thumbnail": io.BytesIO(pic_tuple[1].encode(exif.codec)),
                    }
                )

            if not pic_selection:
                continue

            print("-" * 80)
            pic_arrays = []
            for pic in pic_selection:
                height, width = (200, 200)
                array_padded = np.ones((height, width, 3), dtype=np.uint8) * 200

                print(
                    f'[{pic.get("index")}] '
                    f'[{os.path.join(pic.get("file_path"), pic.get("file_name"))}]'
                )
                if not (im := exif.get_pil_image(pic.get("thumbnail"))):
                    continue

                image_array = np.array(im)
                height = min(image_array.shape[0], height)
                width = min(image_array.shape[1], width)
                array_padded[:height, :width, :] = image_array[:height, :width, :]
                pic_arrays.append(array_padded)

            exif.show_image_array(np.hstack(pic_arrays))

            # -1 skip removal, 0 quit method, 1..n pictures index to be removed
            # in case of skip, update the reviews table
            answer_delete = utils.get_answer(choices)
            if answer_delete[0] == -1:
                cls.update_reviews(pic_selection, reviewer_name)

            elif answer_delete[0] == 0:
                return

            else:
                log_lines = []
                deleted_ids = []
                for pic in pic_selection:
                    if pic.get("index") in answer_delete:
                        deleted_ids.append(pic.get("id"))
                        _from = os.path.join(pic.get("file_path"), pic.get("file_name"))
                        _to = os.path.join(deleted_folder, pic.get("file_name"))

                        try:
                            shutil.move(_from, _to)
                            log_line = (
                                f'file deleted, id: {pic.get("id")}, '
                                f"file_name: {_from}"
                            )
                        except FileNotFoundError:
                            log_line = (
                                f'file not in folder, id: {pic.get("id")}, '
                                f"file_name: {_from}"
                            )

                        print(log_line)
                        log_lines.append(log_line)

                # call seperately to make sure change to db is committed on
                # return of this function
                cls.delete_ids(deleted_ids)

                with open(log_file, "at") as f:
                    for line in log_lines:
                        f.write(line + "\n")

    @classmethod
    @DbUtils.connect
    def check_and_remove_non_existing_files(cls, cursor):
        """check if files are in the database, but not on file, in that case remove
        from the database
        """
        sql_string = f"SELECT picture_id FROM {cls.table_files} WHERE NOT file_checked;"
        cursor.execute(sql_string)
        deleted_ids = [id[0] for id in cursor.fetchall()]
        cls.delete_ids(deleted_ids)

    @classmethod
    @DbUtils.connect
    def remove_pics_by_id(cls, deleted_folder, start_id, cursor, end_id=None):
        """remove pictures that are in dabase with id between start_id and end_id
        patch needed as google photo may merge duplicate photos
        """
        log_file = os.path.join(deleted_folder, "_delete_pictures_by_id.log")
        with open(log_file, "at") as f:
            c_time = datetime.datetime.now()
            if end_id:
                f.write(
                    f"===> Remove pictured by id from "
                    f"{start_id} to {end_id}: {c_time}\n"
                )

            else:
                f.write(
                    f"===> Remove pictured by id from "
                    f"{start_id} until last: {c_time}\n"
                )

        if end_id:
            sql_string = (
                f"select id, file_path, file_name from {cls.table_files} "
                f"where id between {start_id} and {end_id}"
            )

        else:
            sql_string = (
                f"select id, file_path, file_name from {cls.table_files} "
                f"where id >= {start_id}"
            )

        cursor.execute(sql_string)

        log_lines = []
        deleted_ids = []
        for pic in cursor.fetchall():
            print(pic)
            _id = pic[0]
            _file_path = pic[1]
            _file_name = pic[2]
            _from = os.path.join(_file_path, _file_name)
            _to = os.path.join(deleted_folder, _file_name)

            try:
                shutil.move(_from, _to)
                log_line = f"file deleted, id: {_id}, " f"file_name: {_from}"

            except FileNotFoundError:
                log_line = f"file not in folder, id: {_id}, " f"file_name: {_from}"

            log_lines.append(log_line)
            deleted_ids.append(_id)

        cls.delete_ids(deleted_ids)

        with open(log_file, "at") as f:
            for line in log_lines:
                f.write(line + "\n")

    @classmethod
    @DbUtils.connect
    def remove_from_locations_table(cls, cursor, picture_ids=None):
        if not picture_ids:
            return

        sql_remove_locations = f"delete from {cls.table_locations} where picture_id=any(array{picture_ids})"
        cursor.execute(sql_remove_locations)

    @classmethod
    @DbUtils.connect
    def update_rotate_checked(cls, json_filename, cursor):
        with open(json_filename) as json_file:
            picture_ids = json.load(json_file)
        cls.set_rotate_check(picture_ids, set_value=True)

    @classmethod
    @DbUtils.connect
    def replace_thumbnail(cls, base_folder, cursor):
        progress_message = progress_message_generator(
            f"update picture for {base_folder}"
        )

        for foldername, _, filenames in os.walk(base_folder):
            for filename in filenames:
                sql_foldername = foldername.replace("'", "''")
                sql_filename = filename.replace("'", "''")

                sql_str = (
                    f"SELECT picture_id FROM {cls.table_files} "
                    f"WHERE file_path = '{sql_foldername}\\' AND "
                    f"file_name = '{sql_filename}'"
                )

                cursor.execute(sql_str)
                try:
                    picture_id = cursor.fetchone()[0]

                except TypeError:
                    logger.info(
                        f"file {os.path.join(foldername, filename)} "
                        f"not found in database"
                    )
                    continue

                fn = os.path.join(foldername, filename)
                if not (im := exif.get_pil_image(fn)):
                    logger.info(f"unable to get pil_image for file {fn}")
                    continue

                cls.update_image(picture_id, im, 0)
                next(progress_message)

    @classmethod
    @DbUtils.connect
    def update_image_md5(cls, picture_id, image, rotate, cursor):
        """This method replaces the thumbnail and md5."""
        picture_bytes = exif.get_image_bytes(image)
        thumbnail = json.dumps(picture_bytes.decode(exif.codec))
        md5_signature = hashlib.md5(picture_bytes).hexdigest()

        sql_str = (
            f"UPDATE {cls.table_pictures} "
            f"SET thumbnail = (%s), "
            f"md5_signature = (%s), "
            f"rotate = (%s) "
            f"WHERE id= (%s) "
        )
        cursor.execute(sql_str, (thumbnail, md5_signature, rotate, picture_id))

    @classmethod
    @DbUtils.connect
    def replace_thumbnail_md5(cls, id_list, cursor):
        """patch to put back missing files of pictures that were already in the database
        but with different size thumbnail and md5
        """
        progress_message = progress_message_generator(
            f"update picture and md5 for {id_list[0]} ... {id_list[-1]}"
        )

        for picture_id in id_list:
            sql_str = (
                f"SELECT file_path, file_name FROM {cls.table_files} "
                f"WHERE picture_id = {picture_id} "
            )

            cursor.execute(sql_str)
            result = cursor.fetchone()

            try:
                filename_abs = os.path.join(result[0], result[1])

            except TypeError:
                logger.info(f"file for {picture_id} not found in database")
                continue

            if not (im := exif.get_pil_image(filename_abs)):
                logger.info(f"unable to get pil_image for file {filename_abs}")
                continue

            cls.update_image_md5(picture_id, im, 0)
            next(progress_message)

    @classmethod
    @DbUtils.connect
    def add_geolocation_info(cls, start_id, end_id, cursor):
        """patch to add geolocation info to the locations table"""
        sql_str = (
            f"SELECT id, longitude, latitude, geolocation_info FROM {cls.table_locations} "
            f"where id >= %s and id < %s order by id;"
        )
        cursor.execute(sql_str, (start_id, end_id))
        results = cursor.fetchall()

        sql_str = (
            f"UPDATE {cls.table_locations} SET geolocation_info = %s WHERE id = %s;"
        )
        for id, longitude, latitude, gl_info in results:
            if not gl_info:
                geolocation_info = cls.get_geolocation_info(longitude, latitude)
                if geolocation_info:
                    print(f"index {id:6,} has been updated ")
                    cursor.execute(sql_str, (geolocation_info, id))
