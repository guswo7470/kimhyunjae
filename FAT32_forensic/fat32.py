import struct
import os
import hashlib
import traceback


class File:
    def __init__(self, file_name, ext, is_dir, is_delete, c_datetime, a_datetime, w_datetime, file_size,
                 img_path, offset_array, cluster_size, path_, long_file_name, is_valid_data):
        self.file_name = file_name
        self.ext = ext
        self.is_dir = is_dir
        self.is_delete = is_delete
        self.c_datetime = c_datetime
        self.a_datetime = a_datetime
        self.w_datetime = w_datetime
        self.file_size = file_size
        self.img_path = img_path
        self.offset_array = offset_array
        self.cluster_size = cluster_size
        self.path_ = path_
        self.long_file_name = long_file_name
        self.is_valid_data = is_valid_data
        self.fp = open(self.img_path, mode='rb')

    def __del__(self):
        self.fp.close()

    def get_md5_hash(self):
        m = hashlib.md5()
        for data in self.read_data():
            m.update(data)
        return m.hexdigest()

    def get_sha1_hash(self):
        m = hashlib.sha1()
        for data in self.read_data():
            m.update(data)
        return m.hexdigest()

    def get_sha256_hash(self):
        m = hashlib.sha256()
        for data in self.read_data():
            m.update(data)
        return m.hexdigest()

    def seek_(self, offset):
        return self.fp.seek(offset)

    def read_(self, size):
        return self.fp.read(size)


    def save_file(self, save_path):
        print(save_path)
        if self.is_dir:
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            return

        folder_path, file_path = os.path.split(save_path)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        f = open(save_path, mode='wb')
        for file_buf in self.read_data():
            f.write(file_buf)
        f.close()

        return True

    def read_data(self):
        file_bufs = b''
        if not self.is_delete:
            for offset in self.offset_array:
                self.file_size -= self.cluster_size
                self.seek_(offset * self.cluster_size)
                if self.file_size > 0:
                    file_bufs = self.read_(self.cluster_size)
                else:
                    file_bufs = self.read_(self.file_size + self.cluster_size)
                yield file_bufs
        else:  # recover
            for offset in self.offset_array:
                self.seek_(offset * self.cluster_size)
                while self.file_size > self.cluster_size:
                    file_bufs += self.read_(self.cluster_size)
                    self.file_size -= self.cluster_size
                if self.file_size <= self.cluster_size:
                    try:
                        file_bufs += self.read_(self.file_size)
                    except:
                        None
                yield file_bufs

class Partition:
    def __init__(self, img_path_, type_, start_sector_):
        self.img_path = img_path_
        self.file_system = None
        self.type = type_
        if type_ == 7:
            self.file_system = 'NTFS'
        elif type_ == 12:
            self.file_system = 'FAT32'
        elif type_ == 131:
            self.file_system = 'EXT4'
        self.start_sector = start_sector_
        self.byte_per_sector = None
        self.sector_per_cluster = None
        self.start_unused_cluster = None
        self.cluster_size = None  # cluster_size = byte_per_sector * sector_per_cluster
        self.volume_name = None
        self.reserved_sector = None
        self.fat_num = None
        self.fat_size = None
        self.bitmap = None
        self.cluster_num = None
        self.parse_boot()

    def read_buf_from_cluster_chain(self, cluster_chain):
        ret_buf = b''
        f = open(self.img_path, mode='rb')
        for cluster_num in cluster_chain:
            if cluster_num == 0:
                ret_buf = b''
            f.seek(self.start_root_directory * self.byte_per_sector + (cluster_num - 2) * self.cluster_size)
            ret_buf += f.read(self.cluster_size)
        f.close()
        return ret_buf

    def parse_folder(self, cluster_num, path):
        try:
            unpack_offset_ = 0
            folder_buf = self.read_buf_from_cluster_chain(self.get_cluster_chain(cluster_num))

            long_file_name_bin = b''
            while unpack_offset_ < len(folder_buf):
                if folder_buf[unpack_offset_ + 11] == 0x10 or folder_buf[unpack_offset_ + 11] == 0x20 or folder_buf[
                    unpack_offset_ + 11] == 0x16 \
                        or folder_buf[unpack_offset_ + 11] == 0x26 or folder_buf[unpack_offset_ + 11] == 0x30:

                    file_name, ext, attribute, reserve_area, c_time, c_date, last_accessed_date, high, last_w_time, last_w_date, low, file_size = \
                        struct.unpack('<8s3sBHHHHHHHHI', folder_buf[unpack_offset_:unpack_offset_ + 32])

                    # 파일인지 폴더인지
                    if attribute & 0x10 == 0x10:
                        is_dir = True
                    else:
                        is_dir = False

                    # 파일이름 디코딩
                    if file_name[0] == 0xE5:
                        is_delete = True
                    else:
                        is_delete = False
                    file_name = file_name.decode('euc-kr', errors='replace').rstrip()
                    ext = ext.decode('euc-kr', errors='replace').rstrip()
                    if ext != '':
                        file_name += ('.' + ext)
                    if is_delete:
                        file_name = file_name[1:]

                    c_datetime = self.change_date(c_date, c_time)
                    a_datetime = self.change_date(last_accessed_date)
                    w_datetime = self.change_date(last_w_date, last_w_time)
                    cluster_num = low + high
                    cluster_chain = self.get_cluster_chain(cluster_num)
                    offset_array = self.cluster_chain_to_offset_array(cluster_chain)

                    if file_name == '.' or file_name == '..':
                        unpack_offset_ += 32
                        continue

                    long_file_name = ''
                    if long_file_name_bin != b'':
                        long_file_name_bin = long_file_name_bin.rsplit(b'\x00\x00', 1)[0]
                        long_file_name = long_file_name_bin.decode('utf-16', errors='replace')
                        if is_delete:
                            long_file_name = long_file_name[1:]
                        path_ = path + '\\' + long_file_name
                        long_file_name_bin = b''
                    else:
                        path_ = path + '\\' + file_name

                    if is_delete:
                        is_valid_data = False
                        if is_dir:
                            source_bin = b'\x2E\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20' + folder_buf[unpack_offset_ + 11:unpack_offset_ + 28]
                            target_bin = self.read_buf_from_cluster_chain([cluster_num])[:28]
                            if source_bin == target_bin:
                                is_valid_data = True
                    else:
                        is_valid_data = True

                    if cluster_num != 0:
                        if not is_valid_data:
                            cluster_chain = []
                            offset_array = []

                        for cluster_num in cluster_chain:
                            byte_offset = int(cluster_num / 8)
                            bit_offset = cluster_num % 8
                            self.bitmap[byte_offset] = self.bitmap[byte_offset] & (0x01 << bit_offset ^ 0xFF)

                    yield File(file_name, ext, is_dir, is_delete, c_datetime, a_datetime, w_datetime, file_size,
                               self.img_path, offset_array, self.cluster_size, path_, long_file_name, is_valid_data)

                    if is_dir and is_valid_data:
                        for file in self.parse_folder(cluster_num, path_):
                            yield file

                elif folder_buf[unpack_offset_ + 11] == 0x0F:
                    temp, name1, temp, name2, temp, name3 = \
                        struct.unpack('<s10s3s12s2s4s ', folder_buf[unpack_offset_:unpack_offset_ + 32])
                    name = name1 + name2 + name3
                    long_file_name_bin = name + long_file_name_bin

                unpack_offset_ += 32
        except:
            print(traceback.print_exc())

    def scan_files(self):
        for file in self.parse_folder(2, self.volume_name):
            yield file

        for file in self.carving():
            yield file

    def cluster_chain_to_offset_array(self, cluster_chain):
        offset_array = []
        for cluster in cluster_chain:
            offset_array.append(int((self.start_root_directory / self.sector_per_cluster) + cluster - 2))
        return offset_array

    def parse_boot(self):
        f = open(self.img_path, mode='rb')
        f.seek(self.start_sector * 512)
        boot_read = f.read(512)
        f.close()
        if self.type == 12:
            temp, byte_per_sector, sector_per_cluster, reserved_sector, fat_num, temp, hidden_sector, temp, fat_size \
                = struct.unpack('<11sHBHB11sI4sI', boot_read[0:40])
            self.byte_per_sector = byte_per_sector
            self.sector_per_cluster = sector_per_cluster
            self.cluster_size = byte_per_sector * sector_per_cluster
            self.reserved_sector = reserved_sector
            self.fat_num = fat_num
            self.fat_size = fat_size
            self.start_fat = self.start_sector + self.reserved_sector
            self.start_root_directory = self.start_fat + self.fat_size * 2
            root_dir_buf = self.read_buf_from_cluster_chain(self.get_cluster_chain(2))
            self.volume_name = struct.unpack('<7s', root_dir_buf[:7])[0].decode().rstrip()
            self.cluster_num = int(self.fat_size * 512 / 4)
            bit_map_size = int(self.cluster_num / 8)
            self.bitmap = bytearray(b'\xff' * bit_map_size)
            self.bitmap[0] = self.bitmap[0] & 0xF8  # 0,1,2번은 바로 unset 하기

        elif self.type == 7:  # NTFS
            return None
        elif self.type == 131:  # EXT4
            return None
        else:
            print('no file_system')

    def carving(self):
        for i in range(self.cluster_num):
            percent = int(i / self.cluster_num * 100)
            if i % 10000 == 0:
                print(percent, '%')

            byte_offset = int(i / 8)
            bit_offset = i % 8
            if self.bitmap[byte_offset] & 0x01 << bit_offset == 0x01 << bit_offset:
                buf = self.read_buf_from_cluster_chain([i])
                if self.signature(buf) == 1:
                    for file in self.parse_folder(i, os.path.join(self.volume_name, '{CARVING}')):
                        yield file

                elif self.signature(buf) == 2:  # png
                    yield self.parse_png(i)

                elif self.signature(buf) == 3:  # jpg
                    yield self.parse_jpg(i)
                else:
                    continue

            self.bitmap[byte_offset] = self.bitmap[byte_offset] & (0x01 << bit_offset ^ 0xFF)

    def parse_png(self, i):
        file_name = None
        ext = 'png'
        long_file_name = 'Free_space_' + str(i) + '.' + ext
        path_ = os.path.join(self.volume_name, '{CARVING}', long_file_name)
        is_dir = False
        is_delete = True
        c_datetime = None
        a_datetime = None
        w_datetime = None
        file_size = self.cluster_size * 10
        img_path = self.img_path
        offset_array = self.cluster_chain_to_offset_array([i])
        cluster_size = self.cluster_size
        is_valid_data = False

        return File(file_name, ext, is_dir, is_delete, c_datetime, a_datetime, w_datetime, file_size,
                    img_path, offset_array, cluster_size, path_, long_file_name, is_valid_data)

    def parse_jpg(self, i):
        file_name = None
        ext = 'jpg'
        long_file_name = 'Free_space_' + str(i) + '.' + ext
        path_ = os.path.join(self.volume_name, '{CARVING}', long_file_name)
        is_dir = False
        is_delete = True
        c_datetime = None
        a_datetime = None
        w_datetime = None
        file_size = self.cluster_size * 10
        img_path = self.img_path
        offset_array = self.cluster_chain_to_offset_array([i])
        cluster_size = self.cluster_size
        is_valid_data = False

        return File(file_name, ext, is_dir, is_delete, c_datetime, a_datetime, w_datetime, file_size,
                    img_path, offset_array, cluster_size, path_, long_file_name, is_valid_data)

    def signature(self, buf):
        if buf[0:11] == b'\x2E\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20' and buf[32:43] == b'\x2E\x2E\x20\x20\x20\x20\x20\x20\x20\x20\x20':  # folder
            return 1
        elif buf[:4] == b'\x89\x50\x4E\x47':  # png
            return 2
        elif buf[:4] == b'\xFF\xD8\xFF\xE0' or buf[:4] == b'\xFF\xD8\xFF\xE1' or buf[:4] == b'\xFF\xD8\xFF\xE8':  # jpeg
            return 3
        else:
            return 0

    def get_cluster_(self, cluster_num):
        f = open(self.img_path, mode='rb')
        read_fat_sector_offset = int(cluster_num / 128)
        f.seek((self.start_fat + read_fat_sector_offset) * self.byte_per_sector)
        fat_buf = f.read(self.byte_per_sector)
        cluster_offset = (cluster_num % 128) * 4
        f.close()
        return struct.unpack('<I', fat_buf[cluster_offset:cluster_offset + 4])[0]

    def get_cluster_chain(self, cluster_num):
        cluster_chain = []
        current_cluster_num = self.get_cluster_(cluster_num)
        cluster_chain.append(cluster_num)
        if current_cluster_num == 0 or current_cluster_num == 268435448:
            return cluster_chain
        while current_cluster_num != 268435455:
            cluster_chain.append(current_cluster_num)
            current_cluster_num = self.get_cluster_(current_cluster_num)
        return cluster_chain

    def change_date(self, c_date, c_time=None):
        c_date = bin(c_date)
        year = int(c_date[2:8], 2) + 1980
        month = int(c_date[8:12], 2)
        date = int(c_date[12:17], 2)
        output = str(year) + '년' + str(month) + '월' + str(date) + '일'
        if c_time is not None:
            c_time = bin(c_time)
            hour = int(c_time[2:7], 2)
            minute = int(c_time[7:13], 2)
            second = int(c_time[13:18], 2) * 2
            output += (str(hour) + '시' + str(minute) + '분' + str(second) + '초')
        return output


class DiskParser:
    def __init__(self, img_path):
        self.img_path = img_path
        self.partitions = []
        self.parse_mbr()

    def parse_mbr(self):
        f = open(self.img_path, mode='rb')
        f.seek(0)
        mbr_read = f.read(512)
        f.close()

        if not self.check_mbr():
            print('MBR not exist')

        else:
            print('MBR exist')

        unpack_offset_ = 446
        for i in range(4):
            temp, type, temp, start_sector, size_of_sector = \
                struct.unpack('<4sB3sII', mbr_read[unpack_offset_:unpack_offset_ + 16])
            if type == 0:
                unpack_offset_ += 16
                continue
            partion = Partition(self.img_path, type, start_sector)
            self.partitions.append(partion)
            unpack_offset_ += 16

    def check_mbr(self):
        fp = open(self.img_path, mode='rb')
        fp.seek(0)
        marker = fp.read(512)[-2:]
        fp.close()
        if marker == b'\x55\xaa':
            return True
        else:
            return False

    def get_partitions(self):
        return self.partitions

    def get_partition(self, index):
        return self.partitions[index]
