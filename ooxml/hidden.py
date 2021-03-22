import struct
import os

# little endian -> decimal // 2byte 가능
def led(hex):
    return struct.unpack('<H',hex)[0] 

# little endian -> decimal // 4byte 가능
def le4(hex):
    return struct.unpack('<L',hex)[0]

f = open('test1.pptx','rb')

# 엔드 오브 디렉토리 시그니처 오프셋
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
print('엔드오브 디렉토리 시그니처 시작 주소=',hex(endofdir_start_offset))

f.seek(endofdir_start_offset + 20)
file_comment_len_hex = f.read(2)
file_comment_len = led(file_comment_len_hex)
print(file_comment_len) 

# comment 있고 없고
while True:
    f.seek(endofdir_start_offset+20)
    c=f.read(2)
    if c == b'\x00\x00':
        print('파일 comment 없음')
        break
    else:
        print('파일 comment 있음')
        break
print('파일 comment 길이 =',file_comment_len)

# comment 있으면 파일 시그니처로 숨긴파일 형태 찾고 확장자 형태에 맞춰서 저장
while c !=0 :
    f.seek(endofdir_start_offset+22)
    what_sig=f.read(2)
    
    if what_sig == b'\x89\x50':
        print('숨긴파일형태 : png파일')
        f.seek(endofdir_start_offset+22)
        data = f.read(file_comment_len)
        file = open('hidden.png','wb')
        file.write(data)
        break
    elif what_sig == b'\xFF\xD8':
        print('숨긴파일형태 : jpeg파일')
        f.seek(endofdir_start_offset+22)
        data = f.read(file_comment_len)
        file = open('hidden.jpeg','wb')
        file.write(data)
        break
    elif what_sig == b'\x50\x4B' :
        print('숨긴파일형태 : zip파일')
        f.seek(endofdir_start_offset+22)
        data=f.read(file_comment_len)
        file = open('hidden.zip','wb')
        file.write(data)
        break
    else:
        print('숨긴파일형태 : text파일')
        f.seek(endofdir_start_offset+22)
        data=f.read(file_comment_len)
        file = open ('hidden.txt','wb')
        file.write(data)
        break





