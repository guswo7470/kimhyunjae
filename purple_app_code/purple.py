import hashlib
import binascii
import base64
import os
from Crypto.Cipher import AES

#aes/cbc/pkcs5padding

userid = '2768BC76-1BBC-4D46-9577-BB6CD89608D5'
key = hashlib.sha256(userid.encode()).digest()
iv = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

msg = '9+yufGju7WnAZb9CzTmpEBlKi74gPKFlBKGQXNiAYAOi3zOG8IwJ65mcqpZnc8iG6xe5DgR+adGr8+kjqYDq7Hc7CYCz1PFEnA0DMx4afM2SebbwuSvVJI3Gz5OIWu0f'
decode_msg = base64.b64decode(msg)

cipher = AES.new(key,AES.MODE_CBC,iv)
dec = cipher.decrypt(decode_msg)

print('복호문=',dec)
print(dec.decode())