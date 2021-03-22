import sys
import argparse
import os
import struct
import shutil

f = open('ext4_실습이미지_1' , 'rb')
f.seek(0)
mbr=f.read(512)
if mbr[510] == 0x55 and mbr[511] == 0xAA :
    print('mbr있음')

else:
    print('mbr없음')

super_block_start = 0x800*512
f.seek(super_block_start)
zero_padding = 1024
f.read(zero_padding)

inode_count = f.read(4)
print(inode_count.hex())
block_count = f.read(4)
print(block_count.hex())
no_use1 = f.read(12)
first_data_block = f.read(4)
print('첫번째 데이터블록 = ' , first_data_block.hex())
block_size = f.read(4)
if block_size[0] == 0x02 :
    block_size = 4096
else:
    block_size = 1024
print (block_size)

cluster_size = f.read(4)
print(cluster_size.hex())
block_per_group = f.read(4)
print(block_per_group.hex())
cluster_per_group = f.read(4)
inodes_per_group = f.read(4)
print('그룹당 아이노드 수 =' , inodes_per_group.hex())
no_use2 = f.read(44)
inode_size = f.read(2)
if inode_size[1] == 0x01 :
    inode_size = 256
else:
    inode_size = 128
print('아이노드 크기 =', inode_size)

gdt = super_block_start + block_size # 슈퍼블록에서 한블록(4096)
f.seek(gdt) # 커서 gdt로 옮김

block_bitmap = f.read(4)
a=struct.unpack('<L',block_bitmap)[0]
inode_bitmap = f.read(4)
b=struct.unpack('<L',inode_bitmap)[0]
inode_table = f.read(4)
c=struct.unpack('<L',inode_table)[0]
print(a)
print(b)
print(c)
a_block_bitmap =(a * block_size + super_block_start)
print(a_block_bitmap)
b_inode_bitmap =(b * block_size + super_block_start)
print(b_inode_bitmap)
c_inode_table =(c * block_size + super_block_start)
print('아이노드 시작',c_inode_table)

f.seek(c_inode_table)
inode_num_1 = f.read(inode_size)
inode_num_2_nouse1 = f.read(4)
inode_num_2_file_size = f.read(4)
inode_num_2_time = f.read(12)
inode_num_2_nouse2 = f.read(40)
block_pointer = f.read(4)
print(block_pointer.hex())
root_directory = struct.unpack('<L',block_pointer)[0]
print(root_directory)
a_root_directory = root_directory * block_size + super_block_start
print(a_root_directory)
#################################################################################################
f.seek(a_root_directory) ## 루트 디렉토리로 커서이동

lost_found = f.read(44)
image1_inode_number = f.read(4)
image1_a_inode_number = struct.unpack('<L',image1_inode_number)[0] ## 아이노드 번호
print('아이노드번호1 =',image1_a_inode_number)
image1_inode_rec_len = f.read(2) ##아이노드 레코드 길이
print('아이노드레코드길이1=',image1_inode_rec_len.hex())
image1_inode_file_len = f.read(1) ##아이노드 파일길이 / 결과값 0a
print('아이노드 파일길이1=',image1_inode_file_len.hex())
iamge1_inode_file_forder = f.read(1) ##아이노드 폴더인지 파일인지
file_len1 = int.from_bytes(image1_inode_file_len, byteorder='big')#inode_file_len은 원래 데이터 타입이 bytes인데 정수형으로 바꾸는 코드
print(file_len1)

if file_len1 % 4 == 0: #### 파일길이가 4로나눠지면 그대로 읽기 
    file_name1 = f.read(file_len1) 
else :
    q,r = divmod(file_len1,4) ### 예를들면 파일길이가 10이니깐 10/4 몫이 2. 2*4+4 = 12까지 읽기
    file_name1 = f.read(4*q + 4)
print(file_name1)
###########################################################################################

f.seek(c_inode_table)
no_use3=f.read((image1_a_inode_number-1)*256)
no_use4 = f.read(60)
inode12_data_pointer = f.read(4)
a_inode12_data_pointer = struct.unpack('<L',inode12_data_pointer)[0]
print(a_inode12_data_pointer)
real_inode12_data = (a_inode12_data_pointer)*block_size + super_block_start
print(real_inode12_data)

f.seek(real_inode12_data)
image1_signiture = f.read(5)
image1_file_size = f.read(3)
tmp = bytearray(image1_file_size) ###struct.unpack은 4바이트읽을수있는데 위에 read(3)이라서 안되서 바꿔줘야함
tmp.append(0) ### 바이트어레이로 바꿔주고 append로 0추가해서 총 4바이트가 됨
image1_file_size = struct.unpack('<L',tmp)[0]
print(image1_file_size)

f.seek(real_inode12_data)
copy_image1 = f.read(image1_file_size)
print(copy_image1)

file1 = open('kakao.jpeg','wb')
file1.write(copy_image1)
print()
############################################################
f.seek(a_root_directory)
no_use5 = f.read(64)
image2_inode_number = f.read(4)
image2_a_inode_number = struct.unpack('<L',image2_inode_number)[0] ## 아이노드 번호
print('아이노드번호2 =',image2_a_inode_number)
image2_inode_rec_len = f.read(2) ##아이노드 레코드 길이
print('아이노드레코드길이2=',image2_inode_rec_len.hex())
image2_inode_file_len = f.read(1) ##아이노드 파일길이 / 결과값 0a
print('아이노드 파일길이2=',image2_inode_file_len.hex())
iamge2_inode_file_forder = f.read(1) ##아이노드 폴더인지 파일인지
file_len2 = int.from_bytes(image2_inode_file_len, byteorder='big')#inode_file_len은 원래 데이터 타입이 bytes인데 정수형으로 바꾸는 코드

if file_len2 % 4 == 0: #### 파일길이가 4로나눠지면 그대로 읽기 
    file_name2 = f.read(file_len2) 
else :
    q,r = divmod(file_len2,4) ### 예를들면 파일길이가 10이니깐 10/4 몫이 2. 2*4+4 = 12까지 읽기
    file_name2 = f.read(4*q + 4)
print(file_name2)

f.seek(c_inode_table)
no_use6=f.read((image2_a_inode_number-1)*256)
no_use7 = f.read(60)
inode13_data_pointer = f.read(4)
a_inode13_data_pointer = struct.unpack('<L',inode13_data_pointer)[0]
print(a_inode13_data_pointer)
real_inode13_data = (a_inode13_data_pointer)*block_size + super_block_start
print(real_inode13_data)

f.seek(real_inode13_data)
image2_signiture = f.read(5)
image2_file_size = f.read(3)
tmp2 = bytearray(image2_file_size) ###struct.unpack은 4바이트읽을수있는데 위에 read(3)이라서 안되서 바꿔줘야함
tmp2.append(0) ### 바이트어레이로 바꿔주고 append로 0추가해서 총 4바이트가 됨
image2_file_size = struct.unpack('<L',tmp2)[0]
print(image2_file_size)

f.seek(real_inode13_data)
copy_image2 = f.read(image2_file_size)

file2 = open('Test.txt','wb')
file2.write(copy_image2)
print()
###############################################################
f.seek(a_root_directory)
no_use8 = f.read(104)
image3_inode_number = f.read(4)
image3_a_inode_number = struct.unpack('<L',image3_inode_number)[0] ## 아이노드 번호
print('아이노드번호2 =',image3_a_inode_number)
image3_inode_rec_len = f.read(2) ##아이노드 레코드 길이
print('아이노드레코드길이2=',image3_inode_rec_len.hex())
image3_inode_file_len = f.read(1) ##아이노드 파일길이 / 결과값 0a
print('아이노드 파일길이2=',image3_inode_file_len.hex())
iamge3_inode_file_forder = f.read(1) ##아이노드 폴더인지 파일인지
file_len3 = int.from_bytes(image3_inode_file_len, byteorder='big')#inode_file_len은 원래 데이터 타입이 bytes인데 정수형으로 바꾸는 코드

if file_len3 % 4 == 0: #### 파일길이가 4로나눠지면 그대로 읽기 
    file_name3 = f.read(file_len3) 
else :
    q,r = divmod(file_len3,4) ### 예를들면 파일길이가 10이니깐 10/4 몫이 2. 2*4+4 = 12까지 읽기
    file_name3 = f.read(4*q + 4)
print(file_name3)

f.seek(c_inode_table)
no_use9=f.read((image3_a_inode_number-1)*256)
no_use10 = f.read(60)
inode15_data_pointer = f.read(4)
a_inode15_data_pointer = struct.unpack('<L',inode15_data_pointer)[0]
print(a_inode15_data_pointer)
real_inode15_data = (a_inode15_data_pointer)*block_size + super_block_start
print(real_inode15_data)

f.seek(real_inode15_data)
image3_signiture = f.read(5)
image3_file_size = f.read(3)
tmp3 = bytearray(image3_file_size) ###struct.unpack은 4바이트읽을수있는데 위에 read(3)이라서 안되서 바꿔줘야함
tmp3.append(0) ### 바이트어레이로 바꿔주고 append로 0추가해서 총 4바이트가 됨
image3_file_size = struct.unpack('<L',tmp3)[0]
print(image3_file_size)

f.seek(real_inode15_data)
copy_image3 = f.read(image3_file_size)

file3 = open('kakao_apeach.png','wb')
file3.write(copy_image3)
print()
######################################################################
f.seek(a_root_directory)
no_use11 = f.read(128)
image4_inode_number = f.read(4)
image4_a_inode_number = struct.unpack('<L',image4_inode_number)[0] ## 아이노드 번호
print('아이노드번호2 =',image4_a_inode_number)
image4_inode_rec_len = f.read(2) ##아이노드 레코드 길이
print('아이노드레코드길이2=',image4_inode_rec_len.hex())
image4_inode_file_len = f.read(1) ##아이노드 파일길이 / 결과값 0a
print('아이노드 파일길이2=',image4_inode_file_len.hex())
iamge4_inode_file_forder = f.read(1) ##아이노드 폴더인지 파일인지
file_len4 = int.from_bytes(image4_inode_file_len, byteorder='big')#inode_file_len은 원래 데이터 타입이 bytes인데 정수형으로 바꾸는 코드

if file_len4 % 4 == 0: #### 파일길이가 4로나눠지면 그대로 읽기 
    file_name4 = f.read(file_len4) 
else :
    q,r = divmod(file_len4,4) ### 예를들면 파일길이가 10이니깐 10/4 몫이 2. 2*4+4 = 12까지 읽기
    file_name4 = f.read(4*q + 4)
print(file_name4)

f.seek(c_inode_table)
no_use12=f.read((image4_a_inode_number-1)*256)
no_use13 = f.read(60)
inode16_data_pointer = f.read(4)
a_inode16_data_pointer = struct.unpack('<L',inode16_data_pointer)[0]
print(a_inode16_data_pointer)
real_inode16_data = (a_inode16_data_pointer)*block_size + super_block_start
print(real_inode16_data)

f.seek(real_inode16_data)
image4_signiture = f.read(5)
image4_file_size = f.read(3)
tmp4 = bytearray(image4_file_size) ###struct.unpack은 4바이트읽을수있는데 위에 read(3)이라서 안되서 바꿔줘야함
tmp4.append(0) ### 바이트어레이로 바꿔주고 append로 0추가해서 총 4바이트가 됨
image4_file_size = struct.unpack('<L',tmp4)[0]
print(image4_file_size)

f.seek(real_inode16_data)
copy_image4 = f.read(image4_file_size)

file4 = open('toystory.jpeg','wb')
file4.write(copy_image4)
print()
