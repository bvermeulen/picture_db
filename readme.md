#Picture db

##Upload pictures in the database
To use the picture database in sync with Google Photo, do not place pictures directly in the folders under the base folder
in this case "Pictures". Instead let Google Photo update the pictures from any device, including from the location
"Pics_to_google", which is the location if you have pictures on the desktop to send for google to upload.

To update the database, periodically select pictures from "Google Photos" from the previous date you have done this to a recent date
and download the pictures in a zipfile. Extract the pictures in this zipfile to the folder "Pics_google".

From the functions in picbase.py, run_merge_pictures(). This will read the pictures in "Pics_google" and any picture not yet in the
database will be moved to the folder "Pics_unsorted". Pictures and movies will stay in "Pics_google". (You can manually move movies
to "Pictures" directly as movies are not uploaded in the database.)

Then move pictures in "Pics_unsorted" to their appropriate folders in "Pictures". Google Photo will sync the pictures from here.

Once done remove any pictures in "Pics_to_google".

From the functions in picbase.py, run_update_picbase(), this will look at all the pictures under "Pictures" and upload any pictures
that are not yet in the database.

##Sync picture database with "Pictures"
It may be you have removed or moved pictures under "Pictures". In this case the picture is no longer at that location on disk, but
still in the database under that location and possibly in another location as well. To sync the database with "Pictures" you can
remove these pictures using the method "check_and_remove_non_existing_files(). Before doing so you better check what files will be
removed from the database by running sql:
>>>select picture_id, file_path, file_name from files where not file_checked;
If you are ok, you can run the method in a python shell:
>>>from picture_db import PictureDb
>>>PictureDb().check_and_remove_non_existing_files()

##Remove duplicate pictures
It may be you accidently have duplicate pictures on file and in the database. Duplicate pictures will have an exact same md5 signature
from the picture that is stored in the database. To remove these from the database and from file you can run the function
run_remove_pics(method='md5') in picbase.py. You can also use method='date' and a comparison will be made by date and time. This
function will run interactively and images will be shown of the duplicate pictures after which the user can decide which picture
to remove. As a safeguard deleted pictures are moved to "Pics_deleted".

##Remove pictures by id
To remove pictures by id you can call the function run_remove_pics(start_id=x, [end_id=y]). In case you give a start_id and no end_id
all pictures with an id greater or equal to start_id will be removed, otherwise all pictures with an id between start_id and end_id.
Note pictures on file and in the database will be removed in bulk. As a safeguard deleted pictures are moved to "Pics_deleted".

##Update picture locations for GIS
To update the lat, long locations for the GIS database table run the function run_pic_gis() in picbase.py. Any picture that has a location
and is not yet in this table will be added

##Check rotation of pictures and set rotate_checked flag
Run an sql to get a json list with id's for pictures that have a location but rotate_checked flag has not yet been set.
>>>select json_agg(id) from pictures where gps_latitude ->> 'ref' in ('N', 'S') and rotate_checked=false \t \pset format unaligned \g ids.json;
In the program pyqt_picture.py change the filename keyword argument to the filename that was output of the line above in the call to
main - main(mode=Mode.Multi, filename='./ids.json')

Now run the program pyqt_picture.py and rotate pictures where required. After this is done update the function run_update_rotate_checked() in
picbase.py with the filename of ids, ids.json and run it to set the rotate_checked_flag.
