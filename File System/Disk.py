import copy
import os
import traceback
import wmi
from struct import *
import subprocess
from threading import Thread
import pyewf
import pywintypes
import win32api
import pythoncom
import shutil
import win32wnet

import Var


class PartitionEntry:
    def __init__(self, data):
        self.type = unpack("<c", data[4:5])[0]
        self.start_byte = unpack("<I", data[8:12])[0] * 0x200
        self.size_of_byte = unpack("<I", data[12:16])[0] * 0x200
        self.file_system = None
        self.vol_name = None
        self.vol_serial = None

    def print_(self):
        print('type', self.type, 'start_byte', self.start_byte, 'size_of_byte', self.size_of_byte,
              'file_system', self.file_system, 'vol_name', self.vol_name, 'vol_serial', self.vol_serial)


class GPTEntry(PartitionEntry):
    def __init__(self, data):
        PartitionEntry.__init__(self, data)
        start_lba, end_lba = unpack("<QQ", data[32:48])
        self.start_byte = start_lba * 0x200
        if self.start_byte == 0:
            self.type = b'\x00'
        else:
            self.type = b'\xff'
        self.size_of_byte = (end_lba - start_lba) * 0x200


class MBRParser:
    def __init__(self, disk_path):
        self.disk_path = disk_path
        self.disk_rf = open(disk_path, mode='rb')
        header = self.disk_rf.read(16)
        self.disk_rf.seek(0)
        if header == b'\x45\x56\x46\x09\x0D\x0a\xFF\x00\x01\x01\x00\x00\x00\x68\x65\x61':
            filename = pyewf.glob(disk_path)
            self.disk_rf = pyewf.handle()
            self.disk_rf.open(filename)
        self.partition_entries = []
        self.scan_partition_table(0)

    def scan_partition_table(self, pos):
        self.disk_rf.seek(pos * 0x200)
        data = self.disk_rf.read(0x200)
        partition_entry = PartitionEntry(b'\x00'*16)
        self.check_file_system(partition_entry)
        if partition_entry.file_system != '':
            self.partition_entries.append(partition_entry)
            return
        data = data[446:]
        if b'\x55\xaa' != unpack("<2s", data[64:66])[0]:
            print('Invalid MBR signature!', self.disk_path)

        for i in range(0, 64, 16):
            partition_entry = PartitionEntry(data[i: i + 16])
            if partition_entry.type == b'\x00':
                continue
            elif partition_entry.type == b'\x0f':
                extend_mbr_pos = partition_entry.start_byte
                self.scan_partition_table(partition_entry.start_byte)
            elif partition_entry.type == b'\x05':
                self.scan_partition_table(partition_entry.start_byte + extend_mbr_pos)
            elif partition_entry.type == b'\xee':
                self.scan_gpt()
                break
            else:
                partition_entry.start_byte += pos
                self.check_file_system(partition_entry)
                self.partition_entries.append(partition_entry)

    def scan_gpt(self):
        self.disk_rf.seek(0x200*2)
        for data in unpack("128s"*128, self.disk_rf.read(128 * 128)):
            partition_entry = GPTEntry(data)
            if partition_entry.type == b'\x00':
                continue
            else:
                self.check_file_system(partition_entry)
                self.partition_entries.append(partition_entry)

    def check_file_system(self, partition):
        self.disk_rf.seek(partition.start_byte)
        buffer = self.disk_rf.read(0x200)
        struct = unpack_from('<3s', buffer)
        partition.file_system = struct[0]
        if partition.file_system == b'\xEB\x52\x90':
            struct += unpack_from('<8sHBH5ssHHHIIIQQQ3sIsI', buffer, 3)
            partition.file_system = 'NTFS'
            partition.mft_pos = struct[2] * struct[3] * struct[14]
            partition.vol_name = self.get_ntfs_name(partition)
            partition.vol_serial = "%08X" % struct[19]
        elif partition.file_system == b'\xEB\x58\x90':
            try:
                struct += unpack_from('<8sHBH5ssHHHIIIQQQ3sI', buffer, 3)
                partition.file_system = 'FAT32'
                self.disk_rf.seek(partition.start_byte + (struct[4] + struct[12] * 2) * struct[2])
                partition.vol_name = self.disk_rf.read(11).decode(Var.system_encoding).rstrip()
                partition.vol_serial = "%08X" % struct[17]
            except Exception as e:
                print('---', e)
                print(traceback.print_exc())
                partition.file_system = ''
                partition.vol_name = ''
                partition.vol_serial = ''
        elif partition.file_system == b'\xEB\x3C\x90':
            try:
                struct += unpack_from('<8sHBH5ssHHHIIBBBI', buffer, 3)
                partition.file_system = 'FAT16'
                self.disk_rf.seek(partition.start_byte + (struct[4] + struct[7] * 2) * struct[2])
                partition.vol_name = self.disk_rf.read(11).decode(Var.system_encoding).rstrip()
                partition.vol_serial = "%08X" % struct[15]
            except Exception as e:
                print('---', e)
                print(traceback.print_exc())
                partition.file_system = ''
                partition.vol_name = ''
                partition.vol_serial = ''
        elif partition.file_system == b'\xEB\x76\x90':
            try:
                struct += unpack_from('<8sHBH5ssHHHIIIQQQ3sIsIIIIIIIIHHBB', buffer, 3)
                partition.file_system = 'EXFAT'
                self.disk_rf.seek(partition.start_byte +
                                  (struct[23] + (struct[25] - 2) * 2 ** struct[30]) * 2 ** struct[29])
                buffer = self.disk_rf.read(0x20)
                partition.vol_name = buffer[2:2 + buffer[1] * 2].decode('utf-16le').rstrip()
                partition.vol_serial = "%08X" % struct[26]
            except Exception as e:
                print('---', e)
                print(traceback.print_exc())
                partition.file_system = ''
                partition.vol_name = ''
                partition.vol_serial = ''
        else:
            partition.file_system = ''
            partition.vol_name = ''
            partition.vol_serial = ''

    def get_ntfs_name(self, partition):
        self.disk_rf.seek(partition.start_byte + partition.mft_pos + (0x400 * 3))
        buffer = self.disk_rf.read(0x400)
        struct = unpack('<4sHHQHHHHIIQHHI', buffer[:48])
        if struct[0] != b'FILE':
            print("no MFT_FILE", struct)
            return
        buffer = bytearray(buffer)
        fixup_array = []
        for i in range(struct[2]):
            fixup_array.append(buffer[struct[1] + i * 2: struct[1] + i * 2 + 2])
        for i in range(1, int(len(buffer)/0x200) + 1):
            j = i * 0x200
            if buffer[j - 2: j] == fixup_array[0]:
                buffer[j - 2: j] = fixup_array[i]
            else:
                print("fix array error", struct)
                return
        buffer = buffer[:struct[8]]
        reading_point = struct[6]
        while True:
            struct = unpack('<IIBBHHHIHBB', buffer[reading_point: reading_point + 24])
            if struct[0] == 0xFFFFFFFF:
                break
            if struct[0] == 0x60:
                attribute_reading_point = reading_point + 24 + (struct[3] * 2)
                return buffer[attribute_reading_point: attribute_reading_point + struct[7]].decode('utf-16le')
            reading_point += struct[1]


def get_vol_letter_list():
    pythoncom.CoInitialize()
    drives = win32api.GetLogicalDriveStrings()
    drives = drives.split('\000')[:-1]
    return drives


def get_disk_drive_list():
    pythoncom.CoInitialize()
    disk_drive_list = {}
    for disk in wmi.WMI().Win32_DiskDrive():
        disk_drive_list[disk.name] = {'physical_path': disk.name, 'physical_num': int(disk.name[-1]),
                                      'model': disk.Model.strip(), 'serial': disk.SerialNumber.strip()}

    for physical_num in range(256):
        disk_name = '\\\\.\\PHYSICALDRIVE' + str(physical_num)
        if os.path.exists(disk_name) and disk_name not in disk_drive_list:
            disk_drive_list[disk_name] = {'physical_path': disk.name, 'physical_num': physical_num,
                                          'model': 'Virtual Storage', 'serial': ''}
    for physical_path in disk_drive_list:
        try:
            size = int(wmi.WMI().Win32_DiskDrive(Index=disk_drive_list[physical_path]['physical_num'])[0].size)
        except IndexError:
            size = 0
        except Exception as e:
            print('---', e)
            print(traceback.print_exc())
        disk_drive_list[physical_path]['size_of_byte'] = size

    return disk_drive_list


def get_volume_list(disk_img_path=None):
    try:
        pythoncom.CoInitialize()
        temp_volume_list = []
        disk_drive_list = get_disk_drive_list()
        if disk_img_path is not None:
            if disk_img_path in disk_drive_list:
                disk_drive_list = {disk_img_path: {'physical_path': disk_img_path,
                                                   'model': disk_drive_list[disk_img_path]['model'],
                                                   'serial': disk_drive_list[disk_img_path]['serial']}}
            else:
                disk_drive_list = {disk_img_path: {'physical_path': disk_img_path, 'model': '', 'serial': ''}}
        serial_to_letter = {}
        for vol_letter_num in range(65, 91):
            vol_letter = chr(vol_letter_num) + ':'
            if os.path.isdir(vol_letter):
                try:
                    vol_serial = subprocess.check_output(["cmd", "/c vol " + vol_letter])
                    vol_serial = vol_serial.decode(errors='ignore').split(' ')[-1][:-2].replace('-', '')
                except Exception as e:
                    print('---', e)
                    print(traceback.print_exc())
                else:
                    serial_to_letter[vol_serial] = vol_letter
        for physical_path in disk_drive_list:
            temp_disk = copy.deepcopy(disk_drive_list[physical_path])
            temp_disk['file_system'] = ''
            temp_disk['vol_letter'] = ''
            temp_disk['vol_name'] = ''
            temp_disk['vol_serial'] = ''
            temp_disk['start_byte'] = 0
            temp_disk['free_space'] = 0
            temp_disk['type'] = 'DISK'
            temp_volume_list.append(temp_disk)
            try:
                partitions = MBRParser(physical_path)
            except Exception as e:
                print('---', e)
                print(traceback.print_exc())
                continue
            for partition in partitions.partition_entries:
                temp_volume = copy.deepcopy(temp_disk)
                temp_volume['file_system'] = partition.file_system
                temp_volume['vol_letter'] = ''
                temp_volume['vol_name'] = partition.vol_name
                temp_volume['vol_serial'] = partition.vol_serial
                temp_volume['size_of_byte'] = partition.size_of_byte
                temp_volume['start_byte'] = partition.start_byte
                temp_volume['free_space'] = 0
                temp_volume['type'] = 'VOLUME'
                if partition.vol_serial in serial_to_letter:
                    temp_volume['vol_letter'] = serial_to_letter[partition.vol_serial]
                    temp_volume['free_space'] = shutil.disk_usage(temp_volume['vol_letter'])[2]
                    del serial_to_letter[partition.vol_serial]
                temp_volume_list.append(temp_volume)

        if disk_img_path is None:
            for vol_serial in serial_to_letter:
                temp_volume = {'physical_path': 'NETWORK_DRIVE', 'type': 'VOLUME',
                               'vol_letter': serial_to_letter[vol_serial], 'vol_serial': vol_serial, 'file_system': ''}
                temp_volume['size_of_byte'], used, temp_volume['free_space'] = shutil.disk_usage(temp_volume['vol_letter'])
                try:
                    temp_volume['vol_name'] = win32wnet.WNetGetUniversalName(temp_volume['vol_letter'])
                except pywintypes.error:
                    temp_volume['vol_name'] = ''
                    continue
                except Exception as e:
                    print('---', e)
                    print(traceback.print_exc())
                    continue
                temp_volume_list.append(temp_volume)
            global volume_list
            volume_list = temp_volume_list
        else:
            global img_volume_list
            img_volume_list = temp_volume_list
    except:
        print(traceback.print_exc())
        raise


volume_list = []
img_volume_list = []


def scan_volumes(disk_img_path=None):
    th = Thread(target=get_volume_list, args=(disk_img_path,))
    th.start()
    if disk_img_path is None:
        th.join()
        global volume_list
        return volume_list
    else:
        th.join()
        global img_volume_list
        return img_volume_list
