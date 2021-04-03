# 알파벳 빈도분석

import substcipher_lib as substcipher

ETAOIN = 'ETAOINSHRDLCUMWFGYPBVKJXQZ' #빈도분석 (영문특성)
LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

# 알파벳 빈도를 카운트
def getLetterCount(message):
    # dictionary {key:value}
    letterCount = {'A':0, 'B':0, 'C':0, 'D':0, 'E':0, 'F':0, 'G':0, 'H':0, 'I':0, 'J':0, 'K':0, 'L':0, 'M':0, 'N':0, 'O':0, 'P':0,
    'Q':0, 'R':0, 'S':0, 'T':0, 'U':0, 'V':0, 'W':0, 'X':0, 'Y':0, 'Z':0}

    for ch in message.upper():
        if ch in LETTERS:
            letterCount[ch] += 1 # 하나씩 증가
    return letterCount

# 정렬(sort)을 위한 함수 정의
def getItemZero(items):
    return items[0]

# 알파벳 출현 빈도 순서대로 쓰기
def getFreqOrder(message):
    letter2freq = getLetterCount(message)
    freq2letter = {} # {135: 'A', 521: 'B', ...}
    for ch in LETTERS:
        if letter2freq[ch] not in freq2letter:
            freq2letter[letter2freq[ch]] = [ch] # {135 : ['A'], ...}
        else:
            freq2letter[letter2freq[ch]].append(ch) # {135 : ['A','E'], ...}
 
    for freq in freq2letter:
            freq2letter[freq].sort(key = ETAOIN.find, reverse=False) # reverse = False : 작은게 앞에, true면 큰게 앞에 / {135 : ['E','A'], ...}
            freq2letter[freq] = ''.join(freq2letter[freq]) # {135 : 'EA' ...}
    
    freqPairs = list(freq2letter.items()) # [(135, 'A'), (521, 'B'), ...]
    #print("FreqPairs = ", freqPairs)
    freqPairs.sort(key=getItemZero, reverse=True) # [(521, 'B'), (135, 'A'), ...]
    freqOrder = []
    for freq_pair in freqPairs:     # freq_pair = (521, 'B')
        freqOrder.append(freq_pair[1]) # freq_pair[1] = 'B'

    return ''.join(freqOrder)

# freq_order= BNVIHLRSEFXAGMDOCYTWKPZJUQ
# ETAOIN = ETAOINSHRDLCUMWFGYPBVKJXQZ
# 빈도순서를 이용하여 암호키 예측하기
def Freq2Key(freq_order):
    temp_dict = {}
    i = 0
    for ch in freq_order:
        temp_dict[ETAOIN[i]] = ch   # temp_dict = {'E': 'B', 'T': 'N', ...}
        i += 1
    temp_list = list(temp_dict.items())     # temp_list = [ ('E','B'), ('T','N'), ...]
    temp_list.sort(key=getItemZero, reverse=False) # temp_list [('A':'X'), ...]
    temp_key_list = []
    for item in temp_list:
        temp_key_list.append(item[1]) # temp_key_list = [ 'X', 'C, ...]

    return ''.join(temp_key_list)


#in_file = 'plaintext.txt'
in_file = 'encrypted.txt'
msg = substcipher.ReadFile(in_file)
AlphaCount = getLetterCount(msg)
print(AlphaCount)
freq_order = getFreqOrder(msg)
print("freq_order=", freq_order)
key_guess = Freq2Key(freq_order)
print("key_guess=", key_guess)
################################################################
print("myKey= VWXABCDEIJKFGHLMQRSNOPTUYZ")
print("ETAOIN=", ETAOIN)
print("LETTERS=", LETTERS)
##################################################################
out_file = "freq_decrypted.txt"
freq_decrypt = substcipher.subst_decrypt(key_guess, msg)
substcipher.WriteFile(out_file, freq_decrypt)


''' 결과값
freq_order= BNVIHLRSEFXAGMDOCYTWKPZJUQ
key_guess=  VWAFBOCSHZPXMLITUERNGKDJYQ
myKey =     VWXABCDEIJKFGHLMQRSNOPTUYZ  어느정도는 맞췄음
ETAOIN=     ETAOINSHRDLCUMWFGYPBVKJXQZ
LETTERS=    ABCDEFGHIJKLMNOPQRSTUVWXYZ

'''