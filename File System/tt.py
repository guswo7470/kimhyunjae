from multiprocessing import Lock
import Disk
import FAT
from datetime import timedelta
from timeit import default_timer as timer

vol_list = Disk.scan_volumes()
for i in range(len(vol_list)-1, -1, -1):
    vol = vol_list[i]
    if not(vol['file_system'] == 'FAT32' or vol['file_system'] == 'EXFAT'):
        del vol_list[i]

for vol in vol_list:
    print(vol)

    if vol['file_system'] == 'FAT32':
        file_system = FAT.FAT32(vol['physical_path'], vol['start_byte'], vol['vol_letter'], Lock(),
                                carving_size=524288000)

        start = timer()
        for i in file_system.scan():
            print(i)

        end = timer()
        print(timedelta(seconds=end-start))

