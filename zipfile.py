import sys
import argparse
import os
import struct
import shutil

f = open('20161905zip.zip' , 'rb')
f.seek(-12,2)
files = f.read(2)
z = files[0]*1 + files[1]*16
print('파일갯수:',z)

f.seek(0)


local_1_signature = f.read(4)
local_1_version = f.read(2)
local_1_flags = f.read(2)
local_1_compressed_method = f.read(2)
local_1_modified_time = f.read(2)
local_1_modified_year = f.read(2)
local_1_crc = f.read(4)
local_1_compressed_size = f.read(4)
local_1_size= struct.unpack('<L', local_1_compressed_size)[0]
print(local_1_size)
local_1_not_compressed_size = f.read(4)
c= struct.unpack('<L', local_1_not_compressed_size)[0]
print(c)
local_1_file_name_length = f.read(2) 
print(local_1_file_name_length) # ;\x00
a = local_1_file_name_length[0]*1 + local_1_file_name_length[1]*16
print(a) # 59
local_1_extra_field_length = f.read(2)
b = local_1_extra_field_length[0]*1 + local_1_extra_field_length[1]*16
print(b)
local_1_file_name = f.read(a)
print('첫번째 파일명=',local_1_file_name)
local_1_extra_field = f.read(b)
print(local_1_extra_field)

data_1_start = 30+a+b # 30은 고정된 값 + a(파일이름길이) + b(엑스트라길이)
print('첫번째 데이터 시작=',hex(data_1_start))
    


data_1 = f.read(local_1_size)
data111 = data_1.hex()
#print('첫번째 데이터=',data111)

##################################################################################################################
local_2_signature = f.read(4)
print(local_2_signature)
local_2_version = f.read(2)
local_2_flags = f.read(2)
local_2_compressed_method = f.read(2)
local_2_modified_time = f.read(2)
local_2_modified_year = f.read(2)
local_2_crc = f.read(4)
local_2_compressed_size = f.read(4)
local_2_size= struct.unpack('<L', local_2_compressed_size)[0]
print(local_2_size)
local_2_not_compressed_size = f.read(4)
d= struct.unpack('<L', local_2_not_compressed_size)[0]
print(d) 
local_2_file_name_length = f.read(2)  
print(local_2_file_name_length) # '#\x00'
e = local_2_file_name_length[0]*1 + local_2_file_name_length[1]*16
print(e) # 35
local_2_extra_field_length = f.read(2)
g = local_2_extra_field_length[0]*1 + local_2_extra_field_length[1]*16
print(g) # 53
local_2_file_name = f.read(e)
print('두번째 파일명=',local_2_file_name)
local_2_extra_field = f.read(g)
print(local_2_extra_field)

data_2_start = data_1_start + local_1_size + 30 + e + g
print('두번째 데이터 시작=', hex(data_2_start))

data_2 = f.read(local_2_size)
data222 = data_2.hex()
#print('두번째 데이터 =',data222)

################################################################################
text1 = open('first.txt','w')
text1.write(data111)

text2 = open('second.txt','w')
text2.write(data222)
