import caesar_lib as caesar
import os, sys

# === read message from a file ===============================
in_file_name = 'plaintext.txt'
print('current working directory: ', os.getcwd())
# --- changing working directory: os.chdir()
if not os.path.exists(in_file_name):
    print("file %s does not exist." %(in_file_name))
    sys.exit()
inFileObj = open(in_file_name)
msg = inFileObj.read()
inFileObj.close()

key = 3

cipher_msg = caesar.encrypt(msg,key)

# === write ciphertext message to a file

print("pt = ", msg[0:30],"...")
print("ct = ", cipher_msg[0:30],"...")
recovered = caesar.decrypt(cipher_msg, key)
print("dt = ", recovered[0:30],"...")

out_file_name = 'cipher_assignment.txt'

outFileObj = open(out_file_name, 'w')
outFileObj.write(cipher_msg)
outFileObj.close()

# === lower, split
low_plaintext = recovered.lower()
split_plaintext = list(low_plaintext)
low_cipher_msg = cipher_msg.lower()
split_cipher_msg = list(low_cipher_msg)

# === a~z

for alphabet in ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']:    
    s = split_plaintext.count(alphabet)
    print("plaintext alphabet = %s: %3d" %(alphabet, s))

print('\n')

for alphabet in ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']:    
    s = split_cipher_msg.count(alphabet)
    print("ciphertext alphabet = %s: %3d" %(alphabet, s))
