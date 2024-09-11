import sys
from picture_db import PictureDb

if __name__ == "__main__":
    pd = PictureDb()
    argv = sys.argv
    batch_size = 100
    if len(argv) == 2:
        try:
            batches = int(argv[1])
        except ValueError:
            print(
                f"Provide an argument with the maximum number of batches (each batch is {batch_size} records)."
            )
            sys.exit()

    for batch in range(1, batches + 1):
        start_id = (batch - 1) * batch_size
        end_id = batch * batch_size
        pd.add_geolocation_info(start_id, end_id)
        print(f"===> batch {batch:4} has been done ...")
