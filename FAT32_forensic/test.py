import fat32
import os

save_folder = os.path.join('C:\\', 'Users', 'hjkim', 'OneDrive', '바탕 화면', 'python')
img_path = '\\\\.\\PHYSICALDRIVE2'
disk = fat32.DiskParser(img_path)
for partition in disk.get_partitions():
    if partition.file_system != 'FAT32':
        continue
    print('\n--- volume_name', partition.volume_name)
    files = partition.scan_files()
    for file in files:
        print(file.path_, file.offset_array,  file.cluster_size, file.file_size)
        file.save_file(os.path.join(save_folder, file.path_))

