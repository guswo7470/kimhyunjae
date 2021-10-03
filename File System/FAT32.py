from struct import *
import threading
import pyewf
import Util
import os

BUF_SIZE = 10485760
SECTOR_SIZE = 0x200

class DiskHandle:
    def __init__(self, physical_path, starting_offset, _lock=None):
        self.handle = open(physical_path, 'rb')
        header = self.handle.read(16)
        self.handle.seek(0)

        if header == b'\x45\x56\x46\x09\x0D\x0a\xFF\x00\x01\x01\x00\x00\x00\x68\x65\x61':
            filename = pyewf.glob(physical_path)
            self.handle = pyewf.handle()
            self.handle.open(filename)

        self.starting_offset = starting_offset
        if _lock is None:
            self._lock = threading.Lock()
        else:
            self._lock = _lock

    def read_gen(self, offset, size):
        while size > BUF_SIZE:
            yield self.read(offset, BUF_SIZE)
            offset += BUF_SIZE
            size -= BUF_SIZE
        if size != 0:
            yield self.read(offset, size)

    def read(self, offset, size):
        self._lock.acquire()
        self.handle.seek(self.starting_offset + offset)
        buf = self.handle.read(size)
        self._lock.release()
        return buf

    def close(self):
        self.handle.close()

class FAT32:
    def __init__(self, physical_path, starting_offset, vol_letter='', _lock=None, carving_size=None):
        self.physical_path = physical_path
        self.carving_size = carving_size
        self.disk_handle = DiskHandle(physical_path, starting_offset, _lock)
        self.vol_letter = vol_letter
        buf = self.disk_handle.read(0, SECTOR_SIZE)
        struct = unpack_from('<11sHBHB11sI4sI', buf[0:40])
        self.byte_per_sector = struct[1]
        self.sector_per_cluster = struct[2]
        self.cluster_size = self.byte_per_sector * self.sector_per_cluster
        self.reserved_sector = struct[3]
        self.fat_num = struct[4]
        self.fat_size = struct[8]
        self.start_fat = self.reserved_sector
        self.start_root_directory = (self.start_fat + self.fat_size * 2) * self.byte_per_sector
        self.fat_buf = self.disk_handle.read(self.start_fat * SECTOR_SIZE, self.fat_size * self.byte_per_sector)
        self.total_cluster_num = int((self.fat_size * self.byte_per_sector - 512 + 8)/4)
        bit_map_size = int(self.total_cluster_num / 8) + 1
        self.bitmap = bytearray(b'\xff' * bit_map_size)
        self.bitmap[0] = self.bitmap[0] & 0xF8
        self.root = Fat32Entry(None, None, None, None, None, None, None, None, self.disk_handle, self.cluster_size, None, None, None, None, self.start_fat, self.start_root_directory).parse_folder(2, self.vol_letter)

        unpack_offset = 0
        for cluster_num in range(self.total_cluster_num):
            byte_offset = int(cluster_num / 8)
            bit_offset = cluster_num % 8
            cal_cluster = unpack_from('<I', self.fat_buf[unpack_offset:unpack_offset + 4])[0]
            if cal_cluster != 0:
                self.bitmap[byte_offset] = self.bitmap[byte_offset] & (0x01 << bit_offset ^ 0xFF)
            unpack_offset += 4

    def get_entry(self, path=''):
        for file in self.scan():
            full_path = file['file_name'][0]['full_path']
            want_path = path.split(os.sep)
            path_ = full_path.split(os.sep)
            if path in full_path and len(want_path) == len(path_):
                return file

    def read(self, path='', offset=0, size=None):
        if path != '':
            for file in self.scan():
                full_path = file['file_name'][0]['full_path']
                want_path = path.split(os.sep)
                path_ = full_path.split(os.sep)
                cluster_num = file['entry_num']
                size_of_byte = file['size_of_byte']
                start_offset = self.start_root_directory + (cluster_num - 2) * self.cluster_size
                if path in full_path and len(want_path) == len(path_):
                    if cluster_num != 0:
                        for buf in self.disk_handle.read_gen(start_offset, size_of_byte):
                            yield buf
        else:
            for buf in self.disk_handle.read_gen(offset, size):
                yield buf

    def list_dir(self, path='', mode=''):
        for file in self.scan():
            full_path = file['file_name'][0]['full_path']
            want_path = path.split(os.sep)
            path_ = full_path.split(os.sep)
            if mode == 1:
                if path in full_path and len(want_path) + 1 >= len(path_):
                    del path_[0]
                    result = '\\'.join(path_)
                    yield result

            elif mode == 2:
                if path in full_path:
                    path_ = full_path.split(os.sep)
                    del path_[0]
                    result = '\\'.join(path_)
                    yield result

    def scan(self, mode=''):
        for file in self.root:
            file_entry = Util.to_dic('entry_num', file.root_cluster_num, None)

            file_name_list = []
            if file.long_file_name != '' and file.file_name:
                file.file_name = file.long_file_name
            elif file.long_file_name == '' and file.file_name:
                file.file_name = file.file_name
            name_info = Util.to_dic('name', file.file_name, None)
            Util.to_dic('ext', file.ext, name_info)
            Util.to_dic('full_path', file.full_path, name_info)
            file_name_list.append(name_info)
            Util.to_dic('file_name', file_name_list, file_entry)
            Util.to_dic('is_delete', file.is_delete, file_entry)
            Util.to_dic('is_dir', file.is_dir, file_entry)
            Util.to_dic('c_date', file.c_datetime, file_entry)
            Util.to_dic('a_date', file.a_datetime, file_entry)
            Util.to_dic('m_date', file.m_datetime, file_entry)
            Util.to_dic('mft_date', None, file_entry)
            Util.to_dic('size_of_byte', file.file_size, file_entry)
            Util.to_dic('data_set', file.cluster_chain, file_entry)
            yield file_entry

    def get_freespace_set(self):
        for offset_list in self.get_chain():
            if len(offset_list) != 0:
                start_offset = self.start_root_directory + (offset_list[0] - 2) * self.cluster_size
                end_offset = self.start_root_directory + (offset_list[-1] - 2) * self.cluster_size

                size = end_offset - start_offset + self.cluster_size
                if size > self.carving_size:
                    while size > self.carving_size:
                        yield start_offset, self.carving_size
                        size -= self.carving_size
                    yield self.carving_size + start_offset, size

                else:
                    yield start_offset, size

    def get_chain(self):
        chain = []
        for i in range(self.total_cluster_num):
            byte_offset = int(i / 8)
            bit_offset = i % 8
            if self.bitmap[byte_offset] & 0x01 << bit_offset == 0x01 << bit_offset:
                chain.append(i)
            else:
                yield chain
                chain = []
        yield chain

class Fat32Entry:
    def __init__(self, file_name, ext, is_dir, is_delete, c_datetime, a_datetime, m_datetime, file_size, disk_handle, cluster_size, full_path, long_file_name, root_cluster_num, cluster_chain, start_fat, start_root_directory):
        self.file_name = file_name
        self.ext = ext
        self.is_dir = is_dir
        self.is_delete = is_delete
        self.c_datetime = c_datetime
        self.a_datetime = a_datetime
        self.m_datetime = m_datetime
        self.file_size = file_size
        self.disk_handle = disk_handle
        self.cluster_size = cluster_size
        self.full_path = full_path
        self.long_file_name = long_file_name
        self.root_cluster_num = root_cluster_num
        self.cluster_chain = cluster_chain
        self.start_fat = start_fat
        self.start_root_directory = start_root_directory

    def parse_folder(self, cluster_num, path):
        buf = self.read_buf_from_cluster_chain(self.get_cluster_chain(cluster_num))
        unpack_offset = 0
        long_file_name_bin = b''

        while unpack_offset < len(buf):
            if buf[unpack_offset+11] == 0x10 or buf[unpack_offset+11] == 0x20 or \
                buf[unpack_offset+11] == 0x16 or buf[unpack_offset+11] == 0x26 or \
                    buf[unpack_offset+11] == 0x30:

                struct = unpack_from('<8s3sBHHHHHHHHI', buf[unpack_offset:unpack_offset+32])
                file_name = struct[0]
                ext = struct[1]
                attribute = struct[2]
                c_time = struct[4]
                c_date = struct[5]
                a_date = struct[6]
                high = struct[7]
                m_time = struct[8]
                m_date = struct[9]
                low = struct[10]
                file_size = struct[11]

                if attribute & 0x10 == 0x10:
                    is_dir = True
                else:
                    is_dir = False

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

                c_datetime = Util.change_date(c_date, c_time)
                a_datetime = Util.change_date(a_date)
                m_datetime = Util.change_date(m_date, m_time)
                root_cluster_num = low + high
                cluster_chain = self.get_cluster_chain(root_cluster_num)

                if file_name == '.' or file_name == '..':
                    unpack_offset += 32
                    continue

                long_file_name = ''
                if long_file_name_bin != b'':
                    long_file_name_bin = long_file_name_bin.rsplit(b'\x00\x00', 1)[0]
                    long_file_name = long_file_name_bin.decode('utf-16', errors='replace')
                    if is_delete:
                        long_file_name = long_file_name[1:]
                    full_path = path + '\\' + long_file_name
                    long_file_name_bin = b''
                else:
                    full_path = path + '\\' + file_name

                if is_delete:
                    is_valid_data = False
                    if is_dir:
                        source_bin = b'\x2E\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20' + \
                                     buf[unpack_offset+11:unpack_offset+28]
                        target_bin = self.read_buf_from_cluster_chain([root_cluster_num])[:28]
                        if source_bin == target_bin:
                            is_valid_data = True
                else:
                    is_valid_data = True

                yield Fat32Entry(file_name, ext, is_dir, is_delete, c_datetime, a_datetime, m_datetime, file_size, self.disk_handle, self.cluster_size, full_path, long_file_name, root_cluster_num, cluster_chain, self.start_fat, self.start_root_directory)

                if is_dir and is_valid_data:
                    for file in self.parse_folder(root_cluster_num, full_path):
                        yield file
                unpack_offset += 32


            elif buf[unpack_offset+11] == 0x0F:
                struct = unpack_from('s10s3s12s2s4s', buf[unpack_offset:unpack_offset+32])
                name1, name2, name3 = struct[1], struct[3], struct[5]
                name = name1 + name2 + name3
                long_file_name_bin = name + long_file_name_bin

            unpack_offset += 32


    def read_buf_from_cluster_chain(self, cluster_chain):
        ret_buf = b''
        for cluster_num in cluster_chain:
            if cluster_num == 0:
                ret_buf = b''
            ret_buf += self.disk_handle.read(self.start_root_directory + (cluster_num-2) * self.cluster_size, self.cluster_size)
        return ret_buf

    def get_cluster_(self, cluster_num):
        read_fat_sector_offset = int(cluster_num / 128)
        fat_buf = self.disk_handle.read((self.start_fat + read_fat_sector_offset) * SECTOR_SIZE, self.cluster_size)
        cluster_offset = (cluster_num % 128) * 4
        result = unpack_from('<I', fat_buf[cluster_offset:cluster_offset + 4])[0]
        return result

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