from struct import pack, unpack


class VariableByteCode:
    def __init__(self):
        pass

    @staticmethod
    def VB_encode_number(num):
        bytes_list = []
        while True:
            bytes_list.insert(0, num % 128)
            if num < 128:
                break
            num = num // 128
        bytes_list[-1] += 128
        return pack('%dB' % len(bytes_list), *bytes_list)

    def VB_encode(self, nums):
        bytes_list = []
        for number in nums:
            bytes_list.append(self.VB_encode_number(number))
        return b"".join(bytes_list)

    @staticmethod
    def VB_decode(bytestream):
        n = 0
        numbers = []
        bytestream = list(unpack('%dB' % len(bytestream), bytestream))
        zero_index = [i for i, ele in enumerate(bytestream) if ele == 0]
        for zero in zero_index:
            if bytestream[zero-1] >= 128:
                bytestream[zero] = 128
        for byte in bytestream:
            if byte < 128:
                n = 128 * n + byte
            else:
                n = 128 * n + (byte - 128)
                numbers.append(n)
                n = 0
        return numbers


if __name__ == '__main__':
    a = VariableByteCode()
    b = a.VB_encode([-3])
    c = a.VB_decode(b'\x88')

    print(b) # 압축
    print(c) # 압축해제

