import struct
import zlib
import olefile
from tika import parser


def hwp_zip(full_path):
    f = olefile.OleFileIO(full_path)
    dirs = f.listdir()
    if ["FileHeader"] not in dirs or ["\x05HwpSummaryInformation"] not in dirs:
        raise Exception("Not Valid Hwp.")
    header = f.openstream("FileHeader")
    header_data = header.read()
    is_compressed = (header_data[36] & 1) == 1
    nums = []
    for d in dirs:
        if d[0] == "BodyText":
            nums.append(int(d[1][len("Section"):]))
    sections = ["BodyText/Section" + str(x) for x in sorted(nums)]
    buf = ""
    for section in sections:
        bodytext = f.openstream(section)
        data = bodytext.read()
        if is_compressed:
            unpacked_data = zlib.decompress(data, -15)
        else:
            unpacked_data = data
        section_text = ""
        i = 0
        size = len(unpacked_data)
        while i < size:
            header = struct.unpack_from("<I", unpacked_data, i)[0]
            rec_type = header & 0x3ff
            rec_len = (header >> 20) & 0xfff
            if rec_type in [67]:
                rec_data = unpacked_data[i + 4:i + 4 + rec_len]
                section_text += rec_data.decode('utf-16le', errors='replace').upper()
                section_text += '\n'
            i += 4 + rec_len
        buf += section_text
    return buf

def tika_parse(full_path):
    headers = {"X-Tika-OCRLanguage": "eng", "X-Tika-OCRTimeout": "300"}
    parsed = parser.from_file(full_path, xmlContent=False,
                              requestOptions={'headers': headers, 'timeout': 300})
    act = parsed['content']
    if act != None:
        buf = str(act).upper()
    else:
        buf = ''

    return buf