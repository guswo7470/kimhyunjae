import traceback
from datetime import datetime
from struct import *
import threading
import pyewf
import os
import hashlib

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
        try:
            buf = self.handle.read(size)
        except:
            buf = b''
        self._lock.release()
        return buf

    def close(self):
        self.handle.close()


class FAT32:
    def __init__(self, physical_path, starting_offset, vol_letter='', _lock=None, carving_size=52428800):
        self.physical_path = physical_path
        self.carving_size = carving_size
        self.starting_offset = starting_offset
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
        self.total_cluster_num = int((self.fat_size * self.byte_per_sector - 512 + 8) / 4)
        bit_map_size = int(self.total_cluster_num / 8) + 1
        self.bitmap = bytearray(b'\xff' * bit_map_size)
        self.bitmap[0] = self.bitmap[0] & 0xF8
        self.fat_buf = self.disk_handle.read(self.start_fat * SECTOR_SIZE, self.fat_size * self.byte_per_sector)
        self.root = Fat32Entry(None, None, None, None, None, None, None, None, self.disk_handle, self.cluster_size,
                               None, None, None, self.start_fat, self.start_root_directory, self.fat_buf).parse_folder(2, self.vol_letter)

        unpack_offset = 0
        for cluster_num in range(self.total_cluster_num):
            byte_offset = int(cluster_num / 8)
            bit_offset = cluster_num % 8
            cal_cluster = unpack_from('<I', self.fat_buf[unpack_offset:unpack_offset + 4])[0]
            if cal_cluster != 0:
                self.bitmap[byte_offset] = self.bitmap[byte_offset] & (0x01 << bit_offset ^ 0xFF)
            unpack_offset += 4

    def get_entry(self, entry=None):
        entry = entry.lower()
        for file in self.scan():
            full_path = file['file_name'][0]['full_path'].lower()
            if full_path == entry:
                return file

    def read(self, entry=None, offset=0, size=None):
        if entry is not None:
            entry_num = entry['entry_num']
            size = entry['size_of_byte']
            fat32 = Fat32Entry(None, None, None, None, None, None, None, None, self.disk_handle, self.cluster_size,
                                  None, None, None, self.start_fat, self.start_root_directory,
                                  self.fat_buf)
            data_set = fat32.get_cluster_chain(entry_num)
            ret_buf = b''
            if data_set[0] == 0:
                yield ret_buf
            elif data_set[0] == 1:
                try:
                    start = self.start_root_directory + (entry_num - 2) * self.cluster_size
                    bufs = self.disk_handle.read_gen(start, size)
                    for buf in bufs:
                        yield buf
                except:
                    yield ret_buf
            else:
                if size == 0:
                    yield ret_buf
                else:
                    for cluster_num in data_set:
                        start = self.start_root_directory + (cluster_num - 2) * self.cluster_size
                        ret_buf += self.disk_handle.read(start, self.cluster_size)
                        if len(ret_buf) > BUF_SIZE:
                            yield ret_buf[:BUF_SIZE]
                            ret_buf = ret_buf[BUF_SIZE:]
                            size -= BUF_SIZE
                    yield ret_buf[:size]

        else:
            bufs = self.disk_handle.read_gen(offset, size)
            for buf in bufs:
                yield buf

    def get_md5(self, entry=None, offset=0, size=None):
        if entry is not None:
            md5 = hashlib.md5()
            for buf in self.read(entry):
                md5.update(buf)
            return md5.hexdigest()
        else:
            md5 = hashlib.md5()
            for i in self.disk_handle.read_gen(offset, size):
                md5.update(i)
            return md5.hexdigest()

    def list_dir(self, entry=None, mode=''):
        for file in self.scan():
            full_path = file['file_name'][0]['full_path']
            low_full_path = full_path.lower()
            entry = entry.lower()
            want_path = entry.split(os.sep)
            path_ = full_path.split(os.sep)
            if mode == 1:
                if entry in low_full_path and len(want_path) + 1 == len(path_):
                    yield full_path

            elif mode == 2:
                if entry in low_full_path:
                    yield full_path

    def scan(self):
        for file in self.root:
            file_entry = self.to_dic('entry_num', file.root_cluster_num, None)
            file_name_list = []
            if file.long_file_name != '' and file.file_name != '':
                file.file_name = file.long_file_name
            elif file.long_file_name == '' and file.file_name != '':
                file.file_name = file.file_name
            name_info = self.to_dic('name', file.file_name, None)
            self.to_dic('ext', file.ext, name_info)
            self.to_dic('full_path', file.full_path, name_info)
            file_name_list.append(name_info)
            self.to_dic('file_name', file_name_list, file_entry)
            self.to_dic('is_delete', file.is_delete, file_entry)
            self.to_dic('is_dir', file.is_dir, file_entry)
            self.to_dic('c_date', file.c_datetime, file_entry)
            self.to_dic('a_date', file.a_datetime, file_entry)
            self.to_dic('m_date', file.m_datetime, file_entry)
            self.to_dic('mft_date', None, file_entry)
            self.to_dic('size_of_byte', file.file_size, file_entry)
            yield file_entry

    def get_freespace_set(self):
        for cluster_chain in self.get_free_chain():
            if len(cluster_chain) != 0:
                start_offset = self.start_root_directory + (cluster_chain[0] - 2) * self.cluster_size
                end_offset = self.start_root_directory + (cluster_chain[-1] - 2) * self.cluster_size
                size = end_offset - start_offset + self.cluster_size
                if size > self.carving_size:
                    while size > self.carving_size:
                        yield start_offset, self.carving_size
                        size -= self.carving_size
                        start_offset += self.carving_size
                    yield start_offset, size

                else:
                    yield start_offset, size

    def get_free_chain(self):
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

    def to_dic(self, x, y, z):
        dic = {x: y}
        if z is not None:
            z.update(dic)
            return z
        return dic


class Fat32Entry:
    def __init__(self, file_name, ext, is_dir, is_delete, c_datetime, a_datetime, m_datetime, file_size, disk_handle,
                 cluster_size, full_path, long_file_name, root_cluster_num, start_fat, start_root_directory, fat_buf):
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
        self.start_fat = start_fat
        self.start_root_directory = start_root_directory
        self.fat_buf = fat_buf

    def parse_folder(self, cluster_num, path):
        buf = self.read_buf_from_cluster_chain(self.get_cluster_chain(cluster_num))
        unpack_offset = 0
        long_file_name_bin = b''

        while unpack_offset < len(buf):
            if buf[unpack_offset + 11] == 0x10 or buf[unpack_offset + 11] == 0x20 or \
                    buf[unpack_offset + 11] == 0x11 or buf[unpack_offset + 11] == 0x21 or \
                    buf[unpack_offset + 11] == 0x12 or buf[unpack_offset + 11] == 0x22 or \
                    buf[unpack_offset + 11] == 0x13 or buf[unpack_offset + 11] == 0x23 or \
                    buf[unpack_offset + 11] == 0x14 or buf[unpack_offset + 11] == 0x24 or \
                    buf[unpack_offset + 11] == 0x15 or buf[unpack_offset + 11] == 0x25 or \
                    buf[unpack_offset + 11] == 0x16 or buf[unpack_offset + 11] == 0x26 or \
                    buf[unpack_offset + 11] == 0x18 or buf[unpack_offset + 11] == 0x28 or \
                    buf[unpack_offset + 11] == 0x30 or buf[unpack_offset + 11] == 0x06:

                struct = unpack_from('<8s3sBHHHHHHHHI', buf[unpack_offset:unpack_offset + 32])
                file_name = struct[0]
                ext = struct[1]
                attribute = struct[2]
                c_time = struct[4]
                c_date = struct[5]
                a_date = struct[6]
                high = pack('>H', struct[7])
                m_time = struct[8]
                m_date = struct[9]
                low = pack('>H', struct[10])
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
                    file_name = '{DELETED}'+file_name[1:]

                c_datetime = self.change_date(c_date, c_time)
                a_datetime = self.change_date(a_date)
                m_datetime = self.change_date(m_date, m_time)
                root_cluster_num = unpack_from('>I', high + low)[0]

                if file_name == '.' or file_name == '..':
                    unpack_offset += 32
                    continue

                long_file_name = ''
                if long_file_name_bin != b'':
                    long_file_name_bin = long_file_name_bin.rsplit(b'\x00\x00', 1)[0]
                    long_file_name = long_file_name_bin.decode('utf-16', errors='replace')
                    if is_delete:
                        long_file_name = '{DELETED}'+long_file_name[1:]
                    full_path = path + '\\' + long_file_name
                    long_file_name_bin = b''
                else:
                    full_path = path + '\\' + file_name

                if is_delete:
                    is_valid_data = False
                    if is_dir:
                        source_bin = b'\x2E\x20\x20\x20\x20\x20\x20\x20\x20\x20\x20' + \
                                     buf[unpack_offset + 11:unpack_offset + 20]
                        target_bin = self.read_buf_from_cluster_chain([root_cluster_num])[:20]
                        if source_bin == target_bin:
                            is_valid_data = True
                else:
                    is_valid_data = True

                yield Fat32Entry(file_name, ext, is_dir, is_delete, c_datetime, a_datetime, m_datetime, file_size,
                                 self.disk_handle, self.cluster_size, full_path, long_file_name, root_cluster_num,
                                 self.start_fat, self.start_root_directory, None)

                if is_dir and is_valid_data:
                    for file in self.parse_folder(root_cluster_num, full_path):
                        yield file

            elif buf[unpack_offset + 11] == 0x0F:
                struct = unpack_from('s10s3s12s2s4s', buf[unpack_offset:unpack_offset + 32])
                name1, name2, name3 = struct[1], struct[3], struct[5]
                name = name1 + name2 + name3
                long_file_name_bin = name + long_file_name_bin

            unpack_offset += 32

    def read_buf_from_cluster_chain(self, cluster_chain):
        ret_buf = b''
        for cluster_num in cluster_chain:
            if cluster_num == 0:
                ret_buf = b''
            ret_buf += self.disk_handle.read(self.start_root_directory + (cluster_num - 2) * self.cluster_size,
                                             self.cluster_size)
        return ret_buf

    def get_cluster_(self, cluster_num):
        start = cluster_num * 4
        read_cluster = unpack_from('<I', self.fat_buf[start:start + 4])[0]
        return read_cluster

    def get_cluster_chain(self, cluster_num):
        cluster_chain = []
        current_cluster_num = self.get_cluster_(cluster_num)
        cluster_chain.append(cluster_num)
        if current_cluster_num == 268435448:
            return cluster_chain
        elif current_cluster_num == 0:
            return [1]
        while current_cluster_num != 268435455:
            cluster_chain.append(current_cluster_num)
            current_cluster_num = self.get_cluster_(current_cluster_num)
        return cluster_chain

    def change_date(self, date, time=None):
        try:
            date = pack('<H', date)
            value = int.from_bytes(date, byteorder='little', signed=False)
            day = value & 0x1F
            month = (value & 0x1E0) >> 5
            year = (value & 0xFE00) >> 9
            if day > 31:
                month += 1
                day -= 31
            if month > 12:
                year += 1
                month -= 12

            if time is not None:
                time = pack('<H', time)
                value = int.from_bytes(time, byteorder='little', signed=False)
                second = (value & 0x1F) * 2
                minute = (value & 0x7E0) >> 5
                hour = (value & 0xF800) >> 11
                if second >= 60:
                    minute += 1
                    second -= 60
                if minute >= 60:
                    hour += 1
                    minute -= 60
                if hour >= 24:
                    day += 1
                    hour -= 24
                return datetime(year + 1980, month, day, hour, minute, second)
            return datetime(year + 1980, month, day)
        except:
            return None


class EXFAT:
    def __init__(self, physical_path, starting_offset, vol_letter='', _lock=None, carving_size=52428800):
        self.physical_path = physical_path
        self.carving_size = carving_size
        self.starting_offset = starting_offset
        self.disk_handle = DiskHandle(physical_path, starting_offset, _lock)
        self.vol_letter = vol_letter
        buf = self.disk_handle.read(0, SECTOR_SIZE)
        struct = unpack_from('<3sQ53sQQIIIIIIHHBBBBB7s', buf[0:120])
        self.start_partition = struct[3]
        self.start_fat = struct[5]
        self.fat_size = struct[6]
        self.start_root_cluster_num = struct[9]
        self.byte_per_sector = 2 ** struct[13]
        self.sector_per_cluster = 2 ** struct[14]
        self.cluster_size = self.byte_per_sector * self.sector_per_cluster
        self.cluster_heap_offset = struct[7] * self.byte_per_sector
        self.start_root_directory = self.cluster_heap_offset + (self.start_root_cluster_num - 2) * self.cluster_size
        self.fat_buf = self.disk_handle.read(self.start_fat * self.byte_per_sector, self.fat_size * self.byte_per_sector)
        self.bitmap_table = self.disk_handle.read(self.start_root_directory, 64)
        struct = unpack_from('20sIQ', self.bitmap_table[32:64])
        self.bitmap_cluster_num = struct[1]
        self.bitmap_size = struct[2]
        self.root = EXFATEntry(None, None, None, None, None, None, None, None, self.disk_handle,
                               self.cluster_size, None, None, self.cluster_heap_offset, self.fat_buf,
                               self.start_root_directory, None).parse_folder(self.start_root_cluster_num, self.vol_letter, 0x01, False)

    def scan(self):
        for file in self.root:
            file_entry = self.to_dic('entry_num', file.cluster_num, None)
            file_name_list = []
            name_info = self.to_dic('name', file.long_file_name, None)
            self.to_dic('ext', file.ext, name_info)
            self.to_dic('full_path', file.full_path, name_info)
            file_name_list.append(name_info)
            self.to_dic('file_name', file_name_list, file_entry)
            self.to_dic('is_delete', file.is_delete, file_entry)
            self.to_dic('is_dir', file.is_dir, file_entry)
            self.to_dic('c_date', file.c_datetime, file_entry)
            self.to_dic('a_date', file.a_datetime, file_entry)
            self.to_dic('m_date', file.m_datetime, file_entry)
            self.to_dic('mft_date', None, file_entry)
            self.to_dic('size_of_byte', file.file_size, file_entry)
            self.flag = file.flag
            yield file_entry

    def list_dir(self, entry=None, mode=''):
        for file in self.scan():
            full_path = file['file_name'][0]['full_path']
            want_path = entry.split(os.sep)
            path_ = full_path.split(os.sep)
            if mode == 1:
                if entry in full_path and len(want_path) + 1 == len(path_):
                    yield full_path

            elif mode == 2:
                if entry in full_path:
                    yield full_path

    def get_entry(self, entry=None):
        for file in self.scan():
            full_path = file['file_name'][0]['full_path']
            if full_path == entry:
                return file

    def read(self, entry=None, offset=0, size=None):
        if entry is not None:
            entry_num = entry['entry_num']
            flag = self.flag
            size = entry['size_of_byte']
            if flag == 3 or flag == 2:
                start_offset = self.cluster_heap_offset + (entry_num - 2) * self.cluster_size
                bufs = self.disk_handle.read_gen(start_offset, size)
                for buf in bufs:
                    yield buf
            elif flag == 1:
                ret_buf = b''
                if entry_num != 0:
                    if size != 0:
                        data_set = EXFATEntry(None, None, None, None, None,None, None, None, self.disk_handle,
                                       self.cluster_size, None, None, self.cluster_heap_offset, self.fat_buf,
                                       self.start_root_directory, None).get_cluster_chain(entry_num)
                        for cluster_num in data_set:
                            if cluster_num == 0:
                                yield ret_buf
                            else:
                                try:
                                    start = self.start_root_directory + (cluster_num - 2) * self.cluster_size
                                    ret_buf += self.disk_handle.read(start, self.cluster_size)
                                    if len(ret_buf) > BUF_SIZE:
                                        yield ret_buf[:BUF_SIZE]
                                        ret_buf = ret_buf[BUF_SIZE:]
                                        size -= BUF_SIZE
                                    if cluster_num == data_set[-1]:
                                        yield ret_buf[:size]
                                except:
                                    yield ret_buf
                    else:
                        yield ret_buf
                else:
                    yield ret_buf
            else:
                yield b''

        else:
            bufs = self.disk_handle.read_gen(offset, size)
            for buf in bufs:
                yield buf

    def get_md5(self, entry=None, offset=0, size=None):
        if entry is not None:
            md5 = hashlib.md5()
            for buf in self.read(entry):
                md5.update(buf)
            return md5.hexdigest()
        else:
            md5 = hashlib.md5()
            for i in self.disk_handle.read_gen(offset, size):
                md5.update(i)
            return md5.hexdigest()

    def get_freespace_set(self):
        for cluster_chain in self.get_free_chain():
            if len(cluster_chain) != 0:
                start_offset = self.cluster_heap_offset + (cluster_chain[0] - 2) * self.cluster_size
                end_offset = self.cluster_heap_offset + (cluster_chain[-1] - 2) * self.cluster_size
                size = end_offset - start_offset + self.cluster_size

                if size > self.carving_size:
                    while size > self.carving_size:
                        yield start_offset, self.carving_size
                        size -= self.carving_size
                        start_offset += self.carving_size
                    yield start_offset, size
                else:
                    yield start_offset, size

    def get_free_chain(self):
        bitmap_chain = EXFATEntry(None, None, None, None, None, None, None, None, self.disk_handle,
                                 self.cluster_size, None, None, self.cluster_heap_offset, self.fat_buf,
                                 self.start_root_directory, None).get_cluster_chain(self.bitmap_cluster_num)
        bitmap = EXFATEntry(None, None, None, None, None, None, None, None, self.disk_handle,
                                 self.cluster_size, None, None, self.cluster_heap_offset, self.fat_buf,
                                 self.start_root_directory, None).read_buf_from_cluster_chain(bitmap_chain)
        bitmap_buf = bitmap[:self.bitmap_size]
        total_cluster_num = self.bitmap_size * 8
        chain = []
        for i in range(0, total_cluster_num):
            byte_offset = int(i / 8)
            bit_offset = i % 8
            if bitmap_buf[byte_offset] & 0x01 << bit_offset != 0x01 << bit_offset:
                chain.append(i+2)
            else:
                yield chain
                chain = []
        yield chain

    def to_dic(self, x, y, z):
        dic = {x: y}
        if z is not None:
            z.update(dic)
            return z
        return dic


class EXFATEntry:
    def __init__(self, cluster_num, ext, is_dir, is_delete,c_datetime, a_datetime, m_datetime, file_size, disk_handle,
                 cluster_size, full_path, long_file_name, cluster_heap_offset, fat_buf, start_root_directory, flag):

        self.cluster_num = cluster_num
        self.long_file_name = long_file_name
        self.ext = ext
        self.is_delete = is_delete
        self.is_dir = is_dir
        self.c_datetime = c_datetime
        self.m_datetime = m_datetime
        self.a_datetime = a_datetime
        self.file_size = file_size
        self.full_path = full_path
        self.disk_handle = disk_handle
        self.cluster_heap_offset = cluster_heap_offset
        self.cluster_size = cluster_size
        self.start_root_directory = start_root_directory
        self.fat_buf = fat_buf
        self.flag = flag

    def parse_folder(self, cluster_num, vol, flag, is_delete):
        if flag & 0x02 > 0:
            buf = self.disk_handle.read(self.cluster_heap_offset + (cluster_num - 2) * self.cluster_size, self.file_size)
        else:
            buf = self.read_buf_from_cluster_chain(self.get_cluster_chain(cluster_num))

        unpack_offset = 0
        long_file_name_bin = b''
        while unpack_offset < len(buf):
            if buf[unpack_offset] == 0x85 or buf[unpack_offset] == 0x05:
                if not is_delete:
                    if buf[unpack_offset] == 0x05:
                        self.is_delete = True
                    else:
                        self.is_delete = False

                struct = unpack_from('<B3sHHIII', buf[unpack_offset:unpack_offset+20])
                attr = struct[2]
                self.c_datetime = self.change_dos_date_time(struct[4])
                self.m_datetime = self.change_dos_date_time(struct[5])
                self.a_datetime = self.change_dos_date_time(struct[6])

                if attr & 0x10 == 0x10:
                    self.is_dir = True
                else:
                    self.is_dir = False

            elif buf[unpack_offset] == 0xC0 or buf[unpack_offset] == 0x40:
                self.flag = unpack_from('<B', buf[unpack_offset+1:unpack_offset+2])[0]
                self.cluster_num = unpack_from('<I', buf[unpack_offset + 20:unpack_offset + 24])[0]
                self.file_size = unpack_from('<Q', buf[unpack_offset + 24:unpack_offset + 32])[0]

            elif buf[unpack_offset] == 0xC1 or buf[unpack_offset] == 0x41:
                struct = unpack_from('<H30s', buf[unpack_offset:unpack_offset+32])
                name = struct[1]
                long_file_name_bin = long_file_name_bin + name

                if buf[unpack_offset+32] == 0x85 or buf[unpack_offset+32] == 0x05 or buf[unpack_offset+32] == 0x00:
                    split_long_file_name = long_file_name_bin.replace(b'\x00\x00', b'')
                    if self.is_delete:
                        self.long_file_name = '{DELETED}'+split_long_file_name.decode('utf-16', errors='replace')
                    else:
                        self.long_file_name = split_long_file_name.decode('utf-16', errors='replace')
                    self.full_path = vol + '\\' + self.long_file_name
                    name, self.ext = os.path.splitext(self.full_path)
                    long_file_name_bin = b''

                    yield EXFATEntry(self.cluster_num, self.ext[1:], self.is_dir, self.is_delete, self.c_datetime, self.a_datetime,
                                     self.m_datetime, self.file_size, self.disk_handle, self.cluster_size, self.full_path,
                                     self.long_file_name, None, None, None, self.flag)

                    if self.is_dir:
                        for file in self.parse_folder(self.cluster_num, self.full_path, self.flag, self.is_delete):
                            yield file

            unpack_offset += 32

    def read_buf_from_cluster_chain(self, cluster_chain):
        ret_buf = b''
        for cluster_num in cluster_chain:
            if cluster_num == 0:
                ret_buf = b''
            ret_buf += self.disk_handle.read(self.cluster_heap_offset + (cluster_num - 2) * self.cluster_size,
                                             self.cluster_size)
        return ret_buf

    def get_cluster_(self, cluster_num):
        start = cluster_num * 4
        read_cluster = unpack_from('<I', self.fat_buf[start:start + 4])[0]
        return read_cluster

    def get_cluster_chain(self, cluster_num):
        cluster_chain = []
        current_cluster_num = self.get_cluster_(cluster_num)
        cluster_chain.append(cluster_num)
        while current_cluster_num != 4294967295:
            cluster_chain.append(current_cluster_num)
            current_cluster_num = self.get_cluster_(current_cluster_num)
        return cluster_chain

    def change_dos_date_time(self, time):
        try:
            bitmask = "{0:032b}".format(time)
            year = int("{0:04d}".format(int(bitmask[0:7], 2) + 1980))
            month = int("{0:02d}".format(int(bitmask[7:11], 2)))
            day = int("{0:02d}".format(int(bitmask[11:16], 2)))
            hour = int("{0:02d}".format(int(bitmask[16:21], 2)))
            minute = int("{0:02d}".format(int(bitmask[21:27], 2)))
            seconds = int("{0:02d}".format(int(bitmask[27:32], 2) * 2))
            if seconds >= 60:
                minute += 1
            if minute >= 60:
                hour += 1
            if hour >= 24:
                day += 1
            if day >= 32:
                month += 1
            if month >= 13:
                year += 1
            return datetime(year, month, day, hour, minute, seconds)

        except:
            return None