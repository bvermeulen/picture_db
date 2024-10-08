import os
import io
import hashlib
from dataclasses import dataclass
import datetime
import json
from PIL import Image, ImageShow
from pillow_heif import register_heif_opener
import piexif

register_heif_opener()
# note: the MD5 signature is based on the thumbnail made with the
# size below. So changing the size will make check on signature invalid
DATABASE_PICTURE_SIZE = (600, 600)


@dataclass
class PictureMetaDataSerialized:
    date_picture: str
    md5_signature: str
    camera_make: str
    camera_model: str
    gps_latitude: str
    gps_longitude: str
    gps_altitude: str
    gps_img_direction: str
    thumbnail: str
    exif: str


@dataclass
class FileMetaDataSerialized:
    file_path: str
    file_name: str
    file_modified: str
    file_created: str
    file_size: int


class Exif:
    """utility methods to handle picture exif"""

    codec = "ISO-8859-1"  # or latin-1

    @classmethod
    def exif_to_tag(cls, exif_dict):
        exif_tag_dict = {}
        thumbnail = exif_dict.pop("thumbnail")

        try:
            exif_tag_dict["thumbnail"] = thumbnail.decode(cls.codec)

        except AttributeError:
            exif_tag_dict["thumbnail"] = None

        for ifd in exif_dict:
            exif_tag_dict[ifd] = {}
            for tag in exif_dict[ifd]:
                try:
                    element = exif_dict[ifd][tag].decode(cls.codec)

                except AttributeError:
                    element = exif_dict[ifd][tag]

                exif_tag_dict[ifd][piexif.TAGS[ifd][tag]["name"]] = element

        return exif_tag_dict

    @staticmethod
    def convert_gps(gps_latitude, gps_longitude, gps_altitude):
        """input based on tuples of fractions"""

        def convert_to_degrees(lat_long_value):
            ref = lat_long_value.get("ref", "")
            fractions = lat_long_value.get("pos", [0, 1])
            degrees = fractions[0][0] / fractions[0][1]
            minutes = fractions[1][0] / fractions[1][1]
            seconds = fractions[2][0] / fractions[2][1]

            if fractions[1][0] == 0 and fractions[2][0] == 0:
                lat_long_str = f"{ref} {degrees:.4f}\u00B0"

            elif fractions[2][0] == 0:
                lat_long_str = f'{ref} {degrees:.0f}\u00B0 {minutes:.2f}"'

            else:
                lat_long_str = (
                    f"{ref} {degrees:.0f}\u00B0 {minutes:.0f}\" {seconds:.0f}'"
                )

            lat_long = degrees + minutes / 60 + seconds / 3600
            if ref in ["S", "W", "s", "w"]:
                lat_long *= -1

            return lat_long_str, lat_long

        try:
            latitude, lat_val = convert_to_degrees(gps_latitude)
            longitude, lon_val = convert_to_degrees(gps_longitude)

            try:
                alt_fraction = gps_altitude.get("alt")
                altitude = f"{alt_fraction[0]/ alt_fraction[1]:.2f}"
                alt_val = alt_fraction[0] / alt_fraction[1]

            except (TypeError, AttributeError, ZeroDivisionError):
                altitude = "-"
                alt_val = None

            return (
                f"{latitude}, {longitude}, altitude: {altitude}",
                (lat_val, lon_val, alt_val),
            )

        except (TypeError, AttributeError, ZeroDivisionError):
            return None, (None, None, None)

    @classmethod
    def get_exif_dict(cls, im, file):
        if im_info := im.info.get("exif"):
            try:
                return cls.exif_to_tag(piexif.load(im_info))

            except Exception as e:
                print(f"file: {file}, exception: {e}")

        return {}

    @classmethod
    def distill_serialized_picfile_meta_data(
        cls, filename: str
    ) -> tuple[PictureMetaDataSerialized, FileMetaDataSerialized]:
        pic_meta = PictureMetaDataSerialized(*[None] * 10)
        file_meta = FileMetaDataSerialized(*[None] * 5)

        valid_name = filename[-4:].lower() in [".jpg", ".png"] or filename[
            -5:
        ].lower() in [".heic", "jpeg"]
        if not valid_name:
            return pic_meta, file_meta

        try:
            im = Image.open(filename)

        except OSError:
            return pic_meta, file_meta

        # file meta data attributes
        file_stat = os.stat(filename)
        file_meta.file_name = os.path.basename(filename)
        file_meta.file_path = os.path.abspath(filename).replace(file_meta.file_name, "")
        file_meta.file_modified = datetime.datetime.fromtimestamp(file_stat.st_mtime)
        if (
            fct := datetime.datetime.fromtimestamp(file_stat.st_birthtime)
        ) != datetime.datetime(1980, 1, 1, 0, 0):
            file_meta.file_created = fct
        else:
            file_meta.file_created = file_meta.file_modified
        file_meta.file_size = file_stat.st_size

        # picture meta data attributes from exif
        exif_dict = cls.get_exif_dict(im, filename)

        if exif_dict:
            pic_meta.camera_make = exif_dict.get("0th").get("Make")
            if pic_meta.camera_make:
                pic_meta.camera_make = pic_meta.camera_make.replace("\x00", "")

            pic_meta.camera_model = exif_dict.get("0th").get("Model")
            if pic_meta.camera_model:
                pic_meta.camera_model = pic_meta.camera_model.replace("\x00", "")

            try:
                pic_meta.date_picture = datetime.datetime.strptime(
                    exif_dict.get("0th").get("DateTime"), "%Y:%m:%d %H:%M:%S"
                )

            except (TypeError, ValueError):
                pic_meta.date_picture = None

            (
                pic_meta.gps_latitude,
                pic_meta.gps_longitude,
                pic_meta.gps_altitude,
                pic_meta.gps_img_direction,
            ) = cls.serialize_exifgps(exif_dict.get("GPS"))

        else:
            pic_meta.camera_make, pic_meta.camera_model, pic_meta.date_picture = (
                None,
                None,
                None,
            )
            (
                pic_meta.gps_latitude,
                pic_meta.gps_longitude,
                pic_meta.gps_altitude,
                pic_meta.gps_img_direction,
            ) = [json.dumps({})] * 4

        try:
            im.thumbnail(DATABASE_PICTURE_SIZE, Image.Resampling.LANCZOS)

        except OSError as e:
            print(f"file {filename}, exception: {e}")

        picture_bytes = cls.get_image_bytes(im)
        pic_meta.thumbnail = json.dumps(picture_bytes.decode(cls.codec))
        pic_meta.md5_signature = hashlib.md5(picture_bytes).hexdigest()
        pic_meta.exif = cls.serialize_exif(exif_dict)

        return pic_meta, file_meta

    @staticmethod
    def serialize_exif(exif_tag_dict):
        try:
            return json.dumps(exif_tag_dict)

        except Exception as e:
            print(f"Convert exif to tag first, error is {e}")
            raise ()

    @staticmethod
    def serialize_exifgps(gps) -> tuple[str, str, str]:
        if not gps:
            return (json.dumps({}),) * 4

        else:
            return (
                json.dumps(
                    {"ref": gps.get("GPSLatitudeRef"), "pos": gps.get("GPSLatitude")}
                ),
                json.dumps(
                    {"ref": gps.get("GPSLongitudeRef"), "pos": gps.get("GPSLongitude")}
                ),
                json.dumps(
                    {"ref": gps.get("GPSAltitudeRef"), "alt": gps.get("GPSAltitude")}
                ),
                json.dumps(
                    {
                        "ref": gps.get("GPSImgDirectionRef"),
                        "dir": gps.get("GPSImgDirection"),
                    }
                ),
            )

    @staticmethod
    def serialize_decimalgps(lat_lon_val: str | tuple):
        if isinstance(lat_lon_val, str):
            try:
                lat, lon, alt = lat_lon_val.replace(",", " ").split()
                lat = float(lat)
                lon = float(lon)
                alt = float(alt)

            except (IndexError, ValueError):
                try:
                    lat, lon = lat_lon_val.replace(",", " ").split()
                    lat = float(lat)
                    lon = float(lon)
                    alt = None

                except (IndexError, ValueError):
                    lat, lon, alt = None, None, None

        elif isinstance(lat_lon_val, tuple):
            try:
                lat, lon, alt = lat_lon_val
                lat = float(lat)
                lon = float(lon)
                if alt is not None:
                    alt = float(alt)

            except ValueError:
                lat, lon, alt = None, None, None

        else:
            lat, lon, alt = None, None, None

        if lat is None or lon is None or -180 > lat > 180 or -180 > lon > 180:
            return (json.dumps({}),) * 4

        def dd_to_dms_conv(dd):
            dd = abs(dd)
            d = int(dd)
            m = int((dd - d) * 60)
            s = (dd - d - m / 60) * 3600
            d = [d, 1]
            m = [m, 1]
            s = [int(s * 100_000), 100_000]
            return [d, m, s]

        gps_latitude = json.dumps(
            {"ref": "S" if lat < 0 else "N", "pos": dd_to_dms_conv(lat)}
        )
        gps_longitude = json.dumps(
            {"ref": "W" if lon < 0 else "E", "pos": dd_to_dms_conv(lon)}
        )
        if alt is None:
            gps_altitude = json.dumps({})

        else:
            gps_altitude = json.dumps({"ref": "", "pos": [int(alt * 100), 100]})
        gps_img_direction = json.dumps({})

        return gps_latitude, gps_longitude, gps_altitude, gps_img_direction

    @staticmethod
    def format_date(date_str: str):
        if not isinstance(date_str, str):
            return None

        try:
            return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")

        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")

        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date_str, "%Y-%m-%d")

        except ValueError:
            return None

    @staticmethod
    def serialize_gps_data_fields(pic_meta):
        if not isinstance(pic_meta.gps_latitude, str):
            pic_meta.gps_latitude = json.dumps(pic_meta.gps_latitude)

        if not isinstance(pic_meta.gps_longitude, str):
            pic_meta.gps_longitude = json.dumps(pic_meta.gps_longitude)

        if not isinstance(pic_meta.gps_altitude, str):
            pic_meta.gps_altitude = json.dumps(pic_meta.gps_altitude)

        if not isinstance(pic_meta.gps_img_direction, str):
            pic_meta.gps_img_direction = json.dumps(pic_meta.gps_img_direction)

        return pic_meta

    @staticmethod
    def get_pil_image(file_name):
        try:
            im = Image.open(file_name)
            im.thumbnail(DATABASE_PICTURE_SIZE, Image.Resampling.LANCZOS)

        except Exception as e:  # pylint: disable=broad-except
            print(f"file {file_name}, exception: {e}")
            im = None

        return im

    @staticmethod
    def get_display_process():
        if os.name == "nt":
            ImageShow.WindowsViewer.format = "PNG"
            display_process = "Microsoft.Photos.exe"

        elif os.name == "posix":
            ImageShow.UnixViewer = "PNG"
            display_process = "eog"

        else:
            assert False, f"operating system: {os.name} is not implemented"

        return display_process

    @staticmethod
    def show_image_array(image_array):
        Image.fromarray(image_array).show()

    @staticmethod
    def get_image_bytes(image):
        img_bytes = io.BytesIO()

        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        image.save(img_bytes, format="JPEG")
        return img_bytes.getvalue()
