import re
import sqlite3
import os
import struct
import zipfile
import zlib
from collections import OrderedDict
from timeit import default_timer as timer
from datetime import timedelta
import olefile
try:
    from xml.etree.cElementTree import XML
except ImportError:
    from xml.etree.ElementTree import XML
import docx
from tika import parser
import xlrd
import tika
tika.initVM()

def no_indexing(path, file):
    conn = sqlite3.connect(file)
    cur = conn.cursor()
    for i in range(40):
        for root, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                conn.execute('CREATE TABLE IF NOT EXISTS employee_data (path text, txt text)')
                cur.executemany('INSERT INTO employee_data  VALUES (?, ?)', [(full_path, full_path)])
    conn.commit()
    cur.close()

def indexing1(path, file): # db 파일 생성 후 indexing
    conn = sqlite3.connect(file)
    cur = conn.cursor()
    for i in range(40):
        for root, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                conn.execute('CREATE TABLE IF NOT EXISTS employee_data (path text, txt text)')
                cur.executemany('INSERT INTO employee_data  VALUES (?, ?)', [(full_path, full_path)])
    conn.commit()
    cur.execute('CREATE INDEX number ON employee_data (txt);')
    cur.close()

def indexing2(path, file): # 컬럼에 채울 때 마다 indexing
    conn = sqlite3.connect(file)
    cur = conn.cursor()
    for i in range(40):
        for root, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                try:
                    conn.execute('CREATE TABLE IF NOT EXISTS employee_data (path text, txt text)')
                    cur.executemany('INSERT INTO employee_data  VALUES (?, ?)', [(full_path, full_path)])
                    cur.execute('CREATE INDEX number ON employee_data (txt);')
                except:
                    None
    conn.commit()
    cur.close()

####################################################################################

def drive_scan(drive_list, save_file):
    if not os.path.exists(save_file):
        conn = sqlite3.connect(save_file)
        cur = conn.cursor()
        conn.execute('CREATE TABLE IF NOT EXISTS employee_data (path text, txt text)')

        for drive in drive_list:
            for root, dirs, files in os.walk(drive):
                for file in files:
                    full_path = os.path.join(root, file)
                    name, ext = os.path.splitext(full_path)
                    if file[0] == '$' or file[0] == '~': #파일 열어져있거나 삭제된거
                        continue
                    try:
                        if ext[1:] == 'xlsx':
                            print(full_path)
                            workbook = xlrd.open_workbook(full_path, on_demand=True)
                            index = 1
                            for sheet in workbook.sheet_names():
                                value_list = []
                                worksheet = workbook.sheet_by_name(sheet)
                                nrows = worksheet.nrows
                                ncols = worksheet.ncols
                                for row_num in range(nrows):
                                    for col_num in range(ncols):
                                        value = worksheet.cell_value(row_num, col_num)
                                        upper_value = str(value).upper() + '\n'
                                        value_list.append(upper_value)
                                value_list = list(OrderedDict.fromkeys(value_list))
                                txt = ''.join(value_list)
                                cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])
                                index += 1

                        elif ext[1:] == 'pptx':
                            print(full_path)
                            document = zipfile.ZipFile(full_path)
                            nums = []
                            value_list = []
                            for d in document.namelist():
                                if d.startswith("ppt/slides/slide"):
                                    nums.append(int(d[len("ppt/slides/slide"):-4]))
                            s_format = "ppt/slides/slide%s.xml"
                            slide_name_list = [s_format % x for x in sorted(nums)]
                            for slide in slide_name_list:
                                xml_content = document.read(slide)
                                tree = XML(xml_content)
                                if tree.tag == '{http://purl.oclc.org/ooxml/presentationml/main}sld':
                                    NAMESPACE = '{http://purl.oclc.org/ooxml/drawingml/main}'
                                else:
                                    NAMESPACE = '{http://schemas.openxmlformats.org/drawingml/2006/main}'
                                TEXT = NAMESPACE + 't'
                                for node in tree.iter(TEXT):
                                    value = str(node.text).upper()
                                    value_list.append(value)
                            value_list = list(OrderedDict.fromkeys(value_list))
                            txt = ','.join(value_list)
                            document.close()
                            cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                        elif ext[1:] == 'docx':
                            print(full_path)
                            templ = docx.Document(full_path)
                            result = []
                            for x, paragraph in enumerate(templ.paragraphs):
                                result.append(paragraph.text.upper())
                            result = list(OrderedDict.fromkeys(result))
                            txt = '\n'.join(result)
                            cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                        elif ext[1:] == 'hwp':
                            print(full_path)
                            try:
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
                                txt = ""
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
                                    txt += section_text
                                txt = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/ ,?_+%#@$!^&*()\n.-]', '', txt)
                                cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                            except OSError:
                                parsed = parser.from_file(full_path)
                                act = parsed['content']
                                if act != None:
                                    txt = act.strip().replace('\n', ' ').upper()
                                else:
                                    txt = ''
                                cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                        elif ext[1:] == 'ppt' or ext[1:] == 'xls' or ext[1:] == 'doc' or ext[1:] == 'pdf':
                            print(full_path)
                            headers = {"X-Tika-OCRLanguage": "eng", "X-Tika-OCRTimeout": "300"}
                            parsed = parser.from_file(full_path, xmlContent=False, requestOptions={'headers': headers, 'timeout': 300})
                            act = parsed['content']
                            if act != None:
                                txt = act.strip().upper()
                            else:
                                txt = ''

                            cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                        elif ext[1:] == 'txt':
                            print(full_path)
                            result = []
                            f = open(full_path, 'rb')
                            lines = f.readlines()
                            for line in lines:
                                sentence = str(line.decode('utf-8', errors='replace').upper()) + '\n'
                                remove = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/ ,?_+%#@$!^&*()\n.-]', '', sentence)
                                word_list = remove.split()
                                for word in word_list:
                                    result.append(word)
                                if len(result) >= 100000:
                                    value_list = list(OrderedDict.fromkeys(result))
                                    txt = ','.join(value_list)
                                    cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])
                                    result = []
                            value_list = list(OrderedDict.fromkeys(result))
                            txt = ','.join(value_list)
                            cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(full_path, txt)])

                        else:
                            continue

                    except:
                        print(full_path, '-------------> 손상된파일 or 암호파일')
                        continue

        conn.commit()
        cur.close()

def keyword_search(keyword, file):
    conn = sqlite3.connect(file)
    cur = conn.cursor()
    keyword = str(keyword).upper()
    cur.execute("SELECT path FROM employee_data WHERE txt LIKE ?", ('%'+keyword+'%',))
    all_path = []
    for i in cur.fetchall():
        all_path.append(i)
    all_path = list(OrderedDict.fromkeys(all_path))
    for v in all_path:
        print(v[0])

if __name__ == '__main__':
    start = timer()
    drive_list = [r'C:\Users\hjkim\OneDrive\바탕 화면\새 폴더']
    file = "D:\\drive5.db"
    drive_scan(drive_list, file)

    #keyword = 'asf1233@kookmin.ac.kr'
    #keyword_search(keyword, file)


    end = timer()
    print(timedelta(seconds=end - start))




