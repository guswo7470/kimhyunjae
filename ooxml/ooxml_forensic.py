import struct
import os

# little endian -> decimal // 2byte 가능
def led(hex):
    return struct.unpack('<H',hex)[0] 

# little endian -> decimal // 4byte 가능
def le4(hex):
    return struct.unpack('<L',hex)[0]

f = open('android.pptx','rb')

# 로컬 시그니처 오프셋
local_signature = b'\x50\x4B\x03\x04'
endofdir_signature = b'\x50\x4B\x05\x06'
offset=0
local_start_offset=[]
endofdir_start_offset=[]

while True:
    f.seek(offset)
    a = f.read(4)

    if a == local_signature:
        local_start_offset.append(offset) #리스트 추가
    elif a == endofdir_signature:
        endofdir_start_offset = offset
        break
    offset += 1
print('로컬 시그니처 시작 주소=',local_start_offset)

# 파일 이름/경로, 데이터 시작 오프셋, 파일크기 
name = []
data_start_offset = []
file_size = []
for i in range(len(local_start_offset)):
    namelen_offset=local_start_offset[i] + 26
    f.seek(namelen_offset)
    namelen_hex = f.read(2)
    namelen = led(namelen_hex)

    extrafield_offset = local_start_offset[i] + 28
    f.seek(extrafield_offset)
    extralen_hex = f.read(2)
    extralen = led(extralen_hex)

    name_offset = local_start_offset[i] + 30 
    f.seek(name_offset)
    name_hex = f.read(namelen)
    name.append(name_hex.decode())

    extra_offset = name_offset + namelen
    f.seek(extra_offset)
    extra_hex = f.read(extralen)

    data_offset = extra_offset + extralen
    data_start_offset.append(data_offset)

    file_size_offset = local_start_offset[i] + 18
    f.seek(file_size_offset)
    file_size_hex=f.read(4)
    file_size_decimal = le4(file_size_hex)
    file_size.append(file_size_decimal)

print('path/file name =',name)
print('data start offset =', data_start_offset)
print('file size=', file_size)

# 파일 안 데이터
data_len = []
for i in range(len(data_start_offset)):
    f.seek(data_start_offset[i])
    data = f.read(file_size[i]-1)
    data_len.append(data)

print('data =',data_len)

# 파일 데이터 저장
txt1 = open('txt1.txt','wb')
txt1.writelines(data_len)

txt2 = open('txt2.txt','w')
txt2.writelines(name)