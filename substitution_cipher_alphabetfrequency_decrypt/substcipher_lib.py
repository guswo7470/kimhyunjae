# substitution cipher (치환암호)
import os, sys

Alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

def subst_encrypt(key,msg):
    result = ''
    InSet = Alphabet
    OutSet = key

    for ch in msg:
        if ch.upper() in InSet:
            idx = InSet.find(ch.upper())
            if ch in Alphabet:
                result += OutSet[idx].upper()

            else:
                result += OutSet[idx].lower()
        else:
            result+=ch

    return result

def subst_decrypt(key,msg):
    result = ''
    InSet = key
    OutSet = Alphabet

    for ch in msg:
        if ch.upper() in InSet:
            idx = InSet.find(ch.upper())
            if ch in Alphabet:
                result += OutSet[idx].upper()

            else:
                result += OutSet[idx].lower()
        else:
            result+=ch

    return result

# 파일 입력 / 출력
def ReadFile(in_file):

    if not os.path.exists(in_file):
        print('file %s does not exist.', in_file)
        sys.exit()
    # 입력파일에서 텍스트 읽기
    InFileObj = open(in_file)
    file_content = InFileObj.read()
    InFileObj.close()

    return file_content

def WriteFile(out_file, message):
    if os.path.exists(out_file):
        print('overwrite the file %s ? (y)es or (n)o' % (out_file))
        response = input('> ') # 사용자 입력 기다리기
        if not response.lower().startswith('y'): #'y' 로 시작하면 진행
            sys.exit()
    # 출력파일 쓰기
    OutFileObj = open(out_file, 'w')
    OutFileObj.write(message)
    OutFileObj.close()

    return 0
    
def main():
    #msg = "This is a sample message."
    msg = ReadFile('plaintext.txt')
    myKey = 'VWXABCDEIJKFGHLMQRSNOPTUYZ'
    cipher = subst_encrypt(myKey,msg)
    WriteFile('encrypted.txt',cipher)
    print("pt=", msg[:30])
    print("ct=", cipher[:30])
    print("key=",myKey)
    decryted = subst_decrypt(myKey,cipher)
    print("dt=",decryted[:30])

if __name__ == '__main__':
    main()