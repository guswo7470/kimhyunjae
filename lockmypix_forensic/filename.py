from Crypto.Cipher import AES
import hashlib
import base64
import os
from Crypto.Util import Counter
import Crypto

# 파일 이름 복호화
def unpad(ct): 
    return ct[:-ct[-1]]


enc_filename = 'PduGKhtLK0Hz5z5RSuY8Hg'
pad_enc_filename = enc_filename + '=='
decoding_pad_enc_filename = base64.b64decode(pad_enc_filename)
key = b'202cb962ac59123\x00'
iv = key

cipher = AES.new(key, AES.MODE_CBC, iv)
dec = cipher.decrypt(decoding_pad_enc_filename)
unpad_dec = unpad(dec).decode()
print('filename = ', unpad_dec)


# PIN 또는 패스워드 복호화
with open('.ini.keyfile.ctr', 'rb+') as a:
    with open('PduGKhtLK0Hz5z5RSuY8Hg.6zu', 'rb+') as b:
        with open('20210412_233958.jpg', 'rb+') as c:
            
            enc_pw_byte = a.read(1)
            enc_img_byte = b.read(1)
            dec_img_byte = c.read(1)
            

            password = ''
            while enc_pw_byte:
                
                decode = ord(enc_pw_byte)^ord(enc_img_byte)^ord(dec_img_byte) # ord 함수 : 문자->숫자
                password += str(decode)

                enc_pw_byte = a.read(1)
                enc_img_byte = b.read(1)
                dec_img_byte = c.read(1)
         
            print(password) 


# 사진 및 동영상 복호화
f = open('PduGKhtLK0Hz5z5RSuY8Hg.6zu','rb+')
enc_file = f.read()

key2 = hashlib.sha1('0000'.encode()).digest()[:16]
iv2 = key2
c_iv = int.from_bytes(iv2,'big')
counter = Counter.new(128, initial_value=c_iv)
cipher2 = AES.new(key2, AES.MODE_CTR, counter=counter)
dec_file = cipher2.decrypt(enc_file)

f = open('20210412_233958.jpeg','wb+')
f.write(dec_file)