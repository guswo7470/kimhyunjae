import os
import re
import sqlite3
import struct
import traceback
import zipfile
import zlib
from collections import OrderedDict
from timeit import default_timer as timer
from datetime import timedelta

import olefile
from tika import parser


import docx



try:
    from xml.etree.cElementTree import XML
except ImportError:
    from xml.etree.ElementTree import XML
import xlrd
import tika
tika.initVM()

def make_table1(drive_list, save_file):
    conn = sqlite3.connect(save_file)
    cur = conn.cursor()
    conn.execute('CREATE TABLE IF NOT EXISTS employee_data (word text, path text)')

    entry_list = []
    word_list = []

    file_num = 0
    for drive in drive_list:
        for root, dirs, files in os.walk(drive):
            for file in files:
                full_path = os.path.join(root, file)
                ext = os.path.splitext(full_path)[1]

                if file[0] == '~' or file[0] == '$':
                    continue

                try:
                    if ext[1:] == 'txt':
                        print(full_path)
                        f = open(full_path, 'rb')
                        lines = f.readlines()
                        start = 0
                        for buf in lines:
                            buf = str(buf.decode('utf-8', errors='replace').upper())
                            dic = make_two_word(buf, file_num, start)
                            for entry in dic:
                                if entry[0] not in word_list:
                                    word_list.append(entry[0])
                                    entry_list.append(entry)

                                else:
                                    for i in entry_list:
                                        if i[0] == entry[0]:
                                            i[1] += entry[1]
                                            break
                                start += 1

                    elif ext[1:] == 'xlsx':
                        print(full_path)
                        workbook = xlrd.open_workbook(full_path, on_demand=True)
                        start = 0
                        for sheet in workbook.sheet_names():
                            worksheet = workbook.sheet_by_name(sheet)
                            nrows = worksheet.nrows
                            ncols = worksheet.ncols
                            for row_num in range(nrows):
                                for col_num in range(ncols):
                                    value = worksheet.cell_value(row_num, col_num)
                                    buf = str(value).upper()
                                    dic = make_two_word(buf, file_num, start)
                                    for entry in dic:
                                        if entry[0] not in word_list:
                                            word_list.append(entry[0])
                                            entry_list.append(entry)
                                        else:
                                            for i in entry_list:
                                                if i[0] == entry[0]:
                                                    i[1] += entry[1]
                                                    break
                                    start += 1

                    elif ext[1:] == 'pptx':
                        print(full_path)
                        document = zipfile.ZipFile(full_path)
                        nums = []
                        for d in document.namelist():
                            if d.startswith("ppt/slides/slide"):
                                nums.append(int(d[len("ppt/slides/slide"):-4]))
                        s_format = "ppt/slides/slide%s.xml"
                        slide_name_list = [s_format % x for x in sorted(nums)]
                        start = 0
                        for slide in slide_name_list:
                            xml_content = document.read(slide)
                            tree = XML(xml_content)
                            if tree.tag == '{http://purl.oclc.org/ooxml/presentationml/main}sld':
                                NAMESPACE = '{http://purl.oclc.org/ooxml/drawingml/main}'
                            else:
                                NAMESPACE = '{http://schemas.openxmlformats.org/drawingml/2006/main}'
                            TEXT = NAMESPACE + 't'
                            for node in tree.iter(TEXT):
                                buf = str(node.text).upper()
                                dic = make_two_word(buf, file_num, start)
                                for entry in dic:
                                    if entry[0] not in word_list:
                                        word_list.append(entry[0])
                                        entry_list.append(entry)
                                    else:
                                        for i in entry_list:
                                            if i[0] == entry[0]:
                                                i[1] += entry[1]
                                                break
                                start += 1
                        document.close()

                    elif ext[1:] == 'docx':
                        print(full_path)
                        document = docx.Document(full_path)
                        start = 0
                        for x, paragraph in enumerate(document.paragraphs):
                            buf = str(paragraph.text).upper()
                            dic = make_two_word(buf, file_num, start)
                            for entry in dic:
                                if entry[0] not in word_list:
                                    word_list.append(entry[0])
                                    entry_list.append(entry)
                                else:
                                    for i in entry_list:
                                        if i[0] == entry[0]:
                                            i[1] += entry[1]
                                            break
                            start += 1

                    elif ext[1:] == 'ppt' or ext[1:] == 'xls' or ext[1:] == 'doc' or ext[1:] == 'pdf' or ext[1:] == 'hwp':
                        print(full_path)
                        parsed = parser.from_file(full_path)
                        act = parsed['content']
                        start = 0
                        if act != None:
                            buf = act.strip().replace('\n', ' ').upper()
                        else:
                            buf = ''
                        dic = make_two_word(buf, file_num, start)
                        for entry in dic:
                            if entry[0] not in word_list:
                                word_list.append(entry[0])
                                entry_list.append(entry)
                            else:
                                for i in entry_list:
                                    if i[0] == entry[0]:
                                        i[1] += entry[1]
                                        break

                    else:
                        continue

                except:
                    print(traceback.print_exc())
                    continue

                file_num += 1

    for e in entry_list:
        word = e[0]
        path = ''.join(str(e[1]))[1:-1]
        cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(word, path)])
    conn.commit()
    cur.close()


def make_two_word(buf, file_num, start):
    buf = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/,?_+%#@$!^&*().-]', '', buf)
    size = len(buf) - 1
    if size != 0:
        for offset in range(size):
            two_word = buf[offset:offset+2]
            if len(two_word) == 2:
                entry_tup = [two_word, [(file_num, start)]]
                start += 1
                yield entry_tup



if __name__ == '__main__':
    start = timer()
    save_folder = r'D:\test.db'
    drive_list = [r'C:\Users\hjkim\OneDrive\바탕 화면\새 폴더']
    make_table1(drive_list, save_folder)

    end = timer()
    print(timedelta(seconds=end - start))
