import os
import threading
import traceback
from struct import *
from datetime import datetime
from itertools import chain
import pyewf

from Structure.compression.lznt1 import lznt1

MFT_SIZE = 0x400
SECTOR_SIZE = 0x200

STANDARD_INFORMATION = 0x10
ATTRIBUTE_LIST = 0x20
FILE_NAME = 0x30
DATA = 0x80
INDEX_ROOT = 0x90
INDEX_ALLOCATION = 0xA0
GET_ATTR_TYPES = [STANDARD_INFORMATION, ATTRIBUTE_LIST, FILE_NAME, DATA]
INDEX_ATTR_TYPES = [INDEX_ROOT, INDEX_ALLOCATION]

CARVING_SIZE = 10485760
BUF_SIZE = 10485760


def fixup(buf, fixup_offset, num_of_fixup):
    buf = bytearray(buf)
    fixup_array = []
    for i in range(num_of_fixup):
        fixup_array.append(buf[fixup_offset + i * 2: fixup_offset + i * 2 + 2])
    for i in range(1, int(len(buf)/SECTOR_SIZE) + 1):

        j = i * SECTOR_SIZE

        if buf[j - 2: j] == fixup_array[0]:
            buf[j - 2: j] = fixup_array[i]
        else:
            return
    return buf


def dt_to_filetime(file_time):
    try:
        return datetime.utcfromtimestamp((file_time - 116444736000000000) / 10000000)
    except OSError:
        return ''


def make_zero_gen(size, div):
    while size > div:
        yield b'\x00' * div
        size -= div
    yield b'\x00' * size


class DiskHandle:
    def __init__(self, device_id, starting_offset, _lock=None):
        self.handle = open(device_id, 'rb')
        header = self.handle.read(16)
        self.handle.seek(0)
        if header == b'\x45\x56\x46\x09\x0D\x0a\xFF\x00\x01\x01\x00\x00\x00\x68\x65\x61':
            filename = pyewf.glob(device_id)
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


class NTFS:
    def __init__(self, physical_path, starting_sector, vol_letter='', _lock=None):
        self.physical_path = physical_path
        self.disk_handle = DiskHandle(physical_path, starting_sector * SECTOR_SIZE, _lock)
        self.vol_letter = vol_letter

        buf = self.disk_handle.read(0, SECTOR_SIZE)
        struct = unpack_from('<3s8sHBH5ss2sHHI8sQQQIB3s8s', buf)
        self.byte_per_sector = struct[2]
        self.sector_per_cluster = struct[3]
        self.cluster_size = self.byte_per_sector * self.sector_per_cluster
        self.mft_offset = struct[13] * self.cluster_size
        self.mft = MFTEntry(self.disk_handle.read(self.mft_offset, MFT_SIZE), 0, None,
                            self.disk_handle, None, self)
        self.root = MFTEntry(b''.join(self.mft.read(DATA, 5 * MFT_SIZE, MFT_SIZE)), 5, None,
                             self.disk_handle, self.mft, self)

    def getfile(self, path=''):
        path = path.split(os.sep)
        del path[0]
        root_dir = self.root
        for cur_dir in self.scan_path(root_dir, b''.join(root_dir.read(INDEX_ROOT, attr_name='$I30'))[16:], path,
                                      [self.vol_letter], -1):
            return cur_dir

    def listdir(self, path=''):
        path = path.split(os.sep)
        del path[0]
        root_dir = self.root
        for cur_dir in self.scan_path(root_dir, b''.join(root_dir.read(INDEX_ROOT, attr_name='$I30'))[16:], path,
                                      [self.vol_letter], 0):
            yield cur_dir

    def scan(self, path=''):
        try:
            path = path.split(os.sep)
            del path[0]
            root_dir = self.root
            for cur_dir in self.scan_path(root_dir, b''.join(root_dir.read(INDEX_ROOT, attr_name='$I30'))[16:], path,
                                          [self.vol_letter], 1):
                yield cur_dir
        except:
            print(traceback.print_exc())
            raise

    def scan_path(self, cur_dir, buf, find_path, path, scan_mode):
        try:
            struct = unpack_from('<IIII', buf)
            reading_point = struct[0]
            buf = buf[:struct[1]]
            while len(buf) >= reading_point + 16:
                struct = list(unpack_from('<QHHI', buf, reading_point))
                entry_num = struct[0] & 0x0000FFFFFFFFFFFF
                seq_num = struct[0] >> 48
                index_entry_size = struct[1]
                if len(buf) < reading_point + index_entry_size:
                    break
                file_name_size = struct[2]
                child_node = struct[3] & 0x01
                end_entry = struct[3] & 0x02
                file_name = None
                if not end_entry and file_name_size != 0:
                    file_name_data = buf[reading_point + 16: reading_point + 16 + file_name_size]
                    # 0 부모참조주소 1 생성시간 2 수정시간 3 MFT 수정시간 4 접근시간 5 파일할당크기 6 파일실제크기 7 속성플레그 8 Reparse값
                    # 9 이름길이 10 이름형식 11 이름
                    file_name_struct = unpack_from('<QQQQQQQIIBB', file_name_data)
                    name_type = file_name_struct[10]  # 0 POSIX 1 Win32 2 Win32 3 Win32 & DOS
                    file_name = file_name_data[66: 66 + file_name_struct[9] * 2].decode('utf-16-le', errors='ignore')
                if child_node and (len(find_path) == 0 or file_name is None or find_path[0] < file_name):
                    index_alloc_vcn = unpack_from('<Q', buf, reading_point + index_entry_size - 8)[0]
                    index_alloc_buf = b''.join(cur_dir.read(INDEX_ALLOCATION, index_alloc_vcn * self.cluster_size, self.cluster_size, attr_name='$I30'))
                    if len(index_alloc_buf) >= 24:
                        struct = unpack_from('<4sHHQQ', index_alloc_buf)
                        fixup_offset = struct[1]
                        num_of_fixup = struct[2]
                        if struct[0] != b'INDX':
                            return
                        index_alloc_buf = fixup(index_alloc_buf, fixup_offset, num_of_fixup)
                        for yield_path in self.scan_path(cur_dir, index_alloc_buf[24:], find_path, path, scan_mode):
                            yield yield_path
                if file_name is not None and file_name != '.' and name_type != 2:
                    if len(find_path) > 0 and find_path[0] == file_name:
                        child_entry = MFTEntry(b''.join(self.mft.read(DATA, entry_num * MFT_SIZE, MFT_SIZE)),
                                               entry_num, seq_num, self.disk_handle, self.mft, self)
                        child_entry.full_path = os.sep.join(path + [file_name])
                        del find_path[0]
                        if len(find_path) == 0 and scan_mode < 0:
                            yield child_entry
                        elif child_entry.is_dir:
                            for child_entry in self.scan_path(
                                    child_entry, b''.join(child_entry.read(INDEX_ROOT, attr_name='$I30'))[16:],
                                    find_path, path + [file_name], scan_mode):
                                yield child_entry
                        return
                    elif len(find_path) == 0 and scan_mode >= 0:
                        child_entry = MFTEntry(b''.join(self.mft.read(DATA, entry_num * MFT_SIZE, MFT_SIZE)),
                                               entry_num, seq_num, self.disk_handle, self.mft, self)
                        child_entry.full_path = os.sep.join(path + [file_name])
                        yield child_entry
                        if scan_mode == 1:
                            if child_entry.is_dir:
                                for child_entry in self.scan_path(
                                        child_entry, b''.join(child_entry.read(INDEX_ROOT, attr_name='$I30'))[16:],
                                        find_path, path + [file_name], scan_mode):
                                    yield child_entry
                if end_entry:
                    break
                reading_point += index_entry_size
        except:
            print(traceback.print_exc())
            raise

class MFTEntry:
    def __init__(self, buf, entry_num, seq_num, disk_handle, mft, ntfs):
        try:
            # 0 Signature FILE 1 fixup_array 2 fixup_count 3 트랜잰션 값 4 MFT 할당 카운트 5 Hard LINK 6 첫번째 속성 위치
            # 7 속성(파일: 0x01, 삭제된 파일: 0x00, 폴더: 0x03, 삭제된 폴더: 0x02) 8 실제사용중인 크기 9 할당된 크기
            # 10 Baserecord 주소값 11 다음 속성 id 12 no 13 MFT Entry Number
            struct = unpack_from('<4sHHQHHHHIIQHHIH', buf)
            signature = struct[0]
            if signature != b'FILE':
                print('invalid MFT entry : signature error (', entry_num, seq_num, ')')
                return

            fixup_offset = struct[1]
            num_of_fixup = struct[2]
            buf = fixup(buf, fixup_offset, num_of_fixup)
            if buf is None:
                print('invalid MFT entry : fixup error (', entry_num, seq_num, ')')
                print('fixup_offset', fixup_offset, 'num_of_fixup', num_of_fixup)
                return

            self.entry_num = struct[13]
            self.seq_num = struct[4]
            if entry_num is not None and self.entry_num != entry_num:
                print('invalid MFT entry : entry_num', self.entry_num, 'not match (', entry_num, seq_num, ')')
                print('entry_num', self.entry_num, 'get_entry_num', entry_num)
                return
            if seq_num is not None and self.seq_num != seq_num:
                print('invalid MFT entry : seq_num', self.seq_num, 'not match (', entry_num, seq_num, ')')
                print('seq_num', self.seq_num, 'get_seq_num', seq_num)
                return

            self.file_name = []
            flag = struct[7]  # 0x01 In Use, 0x02 Is Directory
            if flag & 0x01:
                self.is_delete = False
            else:
                self.is_delete = True
            if flag & 0x02:
                self.is_dir = True
            else:
                self.is_dir = False
            self.c_time = 0
            self.a_time = 0
            self.m_time = 0
            self.mft_time = 0
            self.size = 0
            self.md5 = ''
            self.sha1 = ''

            self.mft = mft
            self.ntfs = ntfs
            self.disk_handle = disk_handle
            real_entry_size = struct[8]
            buf = buf[:real_entry_size]
            self.attr_list = {}
            base_entry_num = struct[10] & 0x0000FFFFFFFFFFFF
            reading_point = struct[6]
            while len(buf) >= reading_point + 4:
                # 0 속성 식별자
                struct = unpack_from('<I', buf, reading_point)
                attribute = {'type': struct[0]}
                if struct[0] != 0xFFFFFFFF and len(buf) >= reading_point + 16:
                    # 0 속성 길이 1 Non resident 플레그 1=Nonresident 2 속성 이름길이 3 속성 이름 시작위치 4 상태 플레그 6 속성 식별자
                    struct = unpack_from('<IBBHHH', buf, reading_point + 4)
                    attr_len = struct[0]
                    attribute['non_resident'] = struct[1]
                    attr_name_size = struct[2] * 2
                    attr_name_offset = struct[3]
                    attribute['flag'] = struct[4]  #0x0001 압축, 0x4000 암호화된 속성, 0x8000 Sparse 속성
                    if len(buf) < reading_point + attr_len:
                        break
                    if attribute['non_resident'] == 0 and len(buf) >= reading_point + 24:
                        # 7 속성 내용 크기 8 속성 내용 시작위치 9 indexed 플레그 10 not use 11 속성 이름 12 속성 내용
                        struct = unpack_from('<IHBB', buf, reading_point + 16)
                        attribute['real_contents_size'] = struct[0]
                        contents_offset = struct[1]
                        attribute['attr_name'] = buf[reading_point + attr_name_offset:
                                                     reading_point + attr_name_offset +
                                                     attr_name_size].decode(encoding='utf-16', errors='ignore')
                        attribute['contents'] = buf[reading_point + contents_offset:
                                                    reading_point + contents_offset + attribute['real_contents_size']]
                    elif attribute['non_resident'] == 1 and len(buf) >= reading_point + 64:
                        # 7 런리스트 시작 VCN 8 런리스트 끝 VCN 9 런리스트 시작 위치 10 압축단위 크기 11 not use 12 속성내용 할당 크기
                        # 13 속성내용 실제 크기 14 속성내용 초기화 크기 15 속성 이름 16 속성 내용
                        struct = unpack_from('<QQHHIQQQ', buf, reading_point + 16)
                        attribute['s_run_list'] = struct[0]
                        attribute['e_run_list'] = struct[1]
                        run_list_offset = struct[2]
                        attribute['zip'] = struct[3]
                        attribute['real_contents_size'] = struct[6]
                        attribute['real_alloc_size'] = struct[7]
                        attribute['attr_name'] = buf[reading_point + attr_name_offset:
                                                     reading_point + attr_name_offset +
                                                     attr_name_size].decode(encoding='utf-16', errors='ignore')
                        attribute['contents'] = buf[reading_point + run_list_offset: reading_point + attr_len]
                    else:
                        break

                    reading_point += attr_len
                    if attribute['type'] in GET_ATTR_TYPES:
                        self.add_attr(attribute['type'], attribute)
                    elif attribute['type'] in INDEX_ATTR_TYPES and attribute['attr_name'] == '$I30':
                        self.add_attr(attribute['type'], attribute)
                    if attribute['type'] == ATTRIBUTE_LIST:
                        attribute_list_buf = b''.join(self.read(ATTRIBUTE_LIST))
                        attribute_list = []
                        attribute_list_reading_point = 0
                        while len(attribute_list_buf) >= attribute_list_reading_point + 26:
                            # 0 속성타입 1 엔트리길이 2 이름길이 3 이름위치 4 시작VCN 5 속성MFT 주소 6 속성ID 7 속성이름
                            struct = list(unpack_from('<IHBBQQH', attribute_list_buf, attribute_list_reading_point))
                            child_entry_num = struct[5] & 0x0000FFFFFFFFFFFF
                            child_seq_num = struct[5] >> 48
                            if child_entry_num != entry_num and \
                                    [child_entry_num, child_seq_num] not in attribute_list:
                                attribute_list.append([child_entry_num, child_seq_num])
                            attribute_list_reading_point += struct[1]

                        for i in range(len(attribute_list) - 1, -1, -1):
                            child_entry_num, child_seq_num = attribute_list[i]
                            child_mft_entry = MFTEntry(
                                b''.join(self.mft.read(DATA, child_entry_num * MFT_SIZE, MFT_SIZE)),
                                child_entry_num, child_seq_num, self.disk_handle, self.mft, self.ntfs)

                            if child_mft_entry is not None:
                                for attr_type in child_mft_entry.attr_list:
                                    self.add_attrs(attr_type, child_mft_entry.attr_list[attr_type])
                        del self.attr_list[ATTRIBUTE_LIST]
                else:
                    break

            if base_entry_num != 0:
                return
            if FILE_NAME in self.attr_list:
                buf = b''.join(self.read(FILE_NAME))
                while len(buf) > 0:
                    # 0 부모참조주소 1 생성시간 2 수정시간 3 MFT수정시간 4 접근시간 5 파일할당크기 6 파일실제크기 7 속성플레그 8 Reparse값
                    # 9 이름길이 10 이름형식 11 이름
                    file_name_struct = unpack_from('<QQQQQQQIIBB', buf)
                    if file_name_struct[10] != 2:
                        self.file_name.append(buf[66: 66 + file_name_struct[9] * 2].decode('utf-16-le', errors='ignore'))
                    buf = buf[66 + file_name_struct[9] * 2:]
                del self.attr_list[FILE_NAME]
            if STANDARD_INFORMATION in self.attr_list:
                buf = b''.join(self.read(STANDARD_INFORMATION))
                # 0 생성시간 1 수정시간 2 MFT수정시간 3 접근시간
                timestamp_struct = unpack_from('<QQQQ', buf)
                self.c_time = dt_to_filetime(timestamp_struct[0])
                self.m_time = dt_to_filetime(timestamp_struct[1])
                self.mft_time = dt_to_filetime(timestamp_struct[2])
                self.a_time = dt_to_filetime(timestamp_struct[3])
                del self.attr_list[STANDARD_INFORMATION]
            if DATA in self.attr_list:
                self.size = self.attr_list[DATA][0]['real_contents_size']
        except:
            print(traceback.print_exc())
            raise

    def add_attrs(self, attr_type, attributes):
        for attribute in attributes:
            self.add_attr(attr_type, attribute)

    def add_attr(self, attr_type, attribute):
        if attr_type not in self.attr_list:
            self.attr_list[attr_type] = []
        attr_arr = self.attr_list[attr_type]
        if attribute['non_resident'] == 1:
            i = 0
            for attr in attr_arr:
                if attr['non_resident'] == 0 or attr['s_run_list'] > attribute['s_run_list']:
                    attr_arr.insert(i, attribute)
                    break
                i += 1
            if len(attr_arr) == i:
                attr_arr.append(attribute)
        else:
            attr_arr.append(attribute)

    def read(self, attr_type=DATA, offset=0, size=None, alloc_size=None, attr_name='', skip_zero_buf=False):
        if attr_type not in self.attr_list:
            return
        attrs = self.attr_list[attr_type]
        zip_ = None
        for attr in attrs:
            if attr['attr_name'] != attr_name:
                continue
            if attr['non_resident'] == 0:
                yield attr['contents']
                continue
            run_list = attr['contents']

            if size is None:
                size = attr['real_contents_size'] - offset

            if alloc_size is None:
                alloc_size = attr['real_alloc_size'] - offset
                if alloc_size > size:
                    alloc_size = size

            if self.is_delete:
                if size > CARVING_SIZE:
                    size = CARVING_SIZE
                if alloc_size > CARVING_SIZE:
                    alloc_size = CARVING_SIZE

            if zip_ is None:
                zip_ = attr['zip']
                zip_buf = b''
                comp_size = (2 ** zip_) * self.ntfs.cluster_size
                unzip_size = 0
                zero_gen = None
            run_list_reading_point = 0
            c_pos = 0
            while len(run_list) > run_list_reading_point and run_list[run_list_reading_point] != 0 and size != 0:
                _c_pos = int(run_list[run_list_reading_point] / 16)
                _c_size = run_list[run_list_reading_point] % 16
                run_list_reading_point += 1
                c_size = int.from_bytes(run_list[run_list_reading_point: run_list_reading_point + _c_size],
                                        byteorder='little') * self.ntfs.cluster_size
                run_list_reading_point += _c_size
                c_pos += int.from_bytes(run_list[run_list_reading_point: run_list_reading_point + _c_pos],
                                        byteorder='little', signed=True) * self.ntfs.cluster_size
                run_list_reading_point += _c_pos
                if offset >= c_size:
                    offset -= c_size
                else:
                    read_size = c_size - offset
                    if size < read_size:
                        read_size = size
                    gen = self.disk_handle.read_gen(c_pos + offset, read_size)
                    if zip_ != 0:
                        unzip_size += c_size
                        if _c_pos == 0:
                            try:
                                unzip_buf = lznt1(zip_buf)
                            except Exception as e:
                                print('---', e)
                                break
                            zip_buf = b''
                            unzip_size -= len(unzip_buf)
                            if unzip_size > 0:
                                if not skip_zero_buf:
                                    zero_gen = make_zero_gen(unzip_size, BUF_SIZE)
                                else:
                                    size -= unzip_size
                                    alloc_size -= unzip_size
                                unzip_size = 0
                        else:
                            zip_buf += b''.join(gen)
                            unzip_buf = b''
                            while len(zip_buf) >= comp_size:
                                unzip_buf += zip_buf[:comp_size]
                                zip_buf = zip_buf[comp_size:]
                            unzip_size -= len(unzip_buf)

                        if unzip_buf == b'' and zero_gen is None:
                            continue
                        gen = [unzip_buf]
                        if zero_gen is not None:
                            gen = chain(gen, zero_gen)
                            zero_gen = None

                    for buf in gen:
                        if size < len(buf):
                            buf = buf[:size]
                        if alloc_size <= 0:
                            buf = b'\x00' * len(buf)
                        elif alloc_size < len(buf):
                            buf = buf[:alloc_size] + b'\x00' * (len(buf) - alloc_size)
                        yield buf
                        size -= len(buf)
                        alloc_size -= len(buf)
                        if size == 0:
                            break
                    offset = 0