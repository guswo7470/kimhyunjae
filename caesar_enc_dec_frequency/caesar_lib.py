# caesar library
import caesar_lib as caesar

def encrypt(msg,key):
    upalphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    lowalphabet = 'abcdefghijklmnopqrstuvwxyz'
    ciphertext_msg = ''

    for ch in msg:
        if ch in upalphabet:
            idx = upalphabet.find(ch) # 문자를 숫자로 바꿔줌 (문자에다가 + key를 할 수 없어서)
            new_idx = (idx + key) % 26 # 0,1,2,... -> 3,4,5,...
            cipher_ch = upalphabet[new_idx] # 3,4,5,... -> D,E,F,...
            ciphertext_msg = ciphertext_msg + cipher_ch
        elif ch in lowalphabet:
            idx = lowalphabet.find(ch)
            new_idx = (idx + key) % 26
            cipher_ch = lowalphabet[new_idx]
            ciphertext_msg = ciphertext_msg + cipher_ch
        else:
            ciphertext_msg=ciphertext_msg + ch
    return ciphertext_msg

def decrypt(msg,key):
    upalphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    lowalphabet = 'abcdefghijklmnopqrstuvwxyz'
    ciphertext_msg = ''
    for ch in msg:
        if ch in upalphabet:
            idx = upalphabet.find(ch)
            new_idx = (idx - key) % 26
            cipher_ch = upalphabet[new_idx]
            ciphertext_msg = ciphertext_msg + cipher_ch
        elif ch in lowalphabet:
            idx = lowalphabet.find(ch)
            new_idx = (idx - key) % 26
            cipher_ch = lowalphabet[new_idx]
            ciphertext_msg = ciphertext_msg + cipher_ch
        else:
            ciphertext_msg=ciphertext_msg + ch
    return ciphertext_msg

#==============================================================================
def main():
    msg = "this is a message"
    key = 3
    cipher_msg = caesar.encrypt(msg,key)

    print("pt = ", msg)
    print("ct = ", cipher_msg)

    recovered = caesar.decrypt(cipher_msg, key)
    print("dt = ", recovered)

#=============================================================================
if __name__ == '__main__':     # 라이브러리 테스트용
    main()
#=============================================================================






