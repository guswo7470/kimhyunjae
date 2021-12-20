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


'''index_set = {'처음': ['0 0', '0,4', '0 9'], '음처': ['0 1']}
if '처음' in index_set:
    index_set['처음'].append('1 5')
else:
    index_set['처음'] = ['1 5']'''

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

    file_num = 0
    dic3 = {}
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
                        location = 0
                        for buf in lines:
                            buf = str(buf.decode('utf-8', errors='replace').upper())
                            dic = make_two_word(buf, file_num, location)
                            for i in dic:
                                location += 1
                                for key, value in i.items():
                                    if key in dic3:
                                        dic3[key] += value

                                    else:
                                        dic3.update(i)


                    elif ext[1:] == 'xlsx':
                        print(full_path)
                        workbook = xlrd.open_workbook(full_path, on_demand=True)
                        location = 0
                        for sheet in workbook.sheet_names():
                            worksheet = workbook.sheet_by_name(sheet)
                            nrows = worksheet.nrows
                            ncols = worksheet.ncols
                            for row_num in range(nrows):
                                for col_num in range(ncols):
                                    value = worksheet.cell_value(row_num, col_num)
                                    buf = str(value).upper()
                                    dic = make_two_word(buf, file_num, location)
                                    for i in dic:
                                        location += 1
                                        for key,value in i.items():
                                            if key in dic3:
                                                dic3[key] += value
                                            else:
                                                dic3.update(i)

                    elif ext[1:] == 'pptx':
                        print(full_path)
                        document = zipfile.ZipFile(full_path)
                        nums = []
                        for d in document.namelist():
                            if d.startswith("ppt/slides/slide"):
                                nums.append(int(d[len("ppt/slides/slide"):-4]))
                        s_format = "ppt/slides/slide%s.xml"
                        slide_name_list = [s_format % x for x in sorted(nums)]
                        location = 0
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
                                dic = make_two_word(buf, file_num, location)
                                for i in dic:
                                    location += 1
                                    for key, value in i.items():
                                        if key in dic3:
                                            dic3[key] += value
                                        else:
                                            dic3.update(i)
                        document.close()

                    elif ext[1:] == 'docx':
                        print(full_path)
                        document = docx.Document(full_path)
                        location = 0
                        for x, paragraph in enumerate(document.paragraphs):
                            buf = str(paragraph.text).upper()
                            dic = make_two_word(buf, file_num, location)
                            for i in dic:
                                location += 1
                                for key, value in i.items():
                                    if key in dic3:
                                        dic3[key] += value
                                    else:
                                        dic3.update(i)

                    elif ext[1:] == 'ppt' or ext[1:] == 'xls' or ext[1:] == 'doc' or ext[1:] == 'pdf' or ext[1:] == 'hwp':
                        print(full_path)
                        parsed = parser.from_file(full_path)
                        act = parsed['content']
                        location = 0
                        if act != None:
                            buf = act.strip().replace('\n', ' ').upper()
                        else:
                            buf = ''
                        dic = make_two_word(buf, file_num, location)
                        for i in dic:
                            location += 1
                            for key, value in i.items():
                                if key in dic3:
                                    dic3[key] += value
                                else:
                                    dic3.update(i)

                    else:
                        continue

                except:
                    print(traceback.print_exc())
                    continue

                file_num += 1

    for word, path in dic3.items():
        cur.executemany('INSERT INTO employee_data VALUES (?, ?)', [(str(word), str(path))])
    conn.commit()
    cur.close()



def make_two_word(buf, file_num, location):
    buf = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/,?_+%#@$!^&*().-]', '', buf)
    size = len(buf) - 1
    dic = {}
    if size != 0:
        for offset in range(size):
            two_word = buf[offset:offset+2]
            if len(two_word) == 2:
                dic[two_word] = [str(file_num) + ' ' + str(location)+'\n']
                yield dic
                dic = {}
                location += 1




if __name__ == '__main__':
    start = timer()
    save_folder = r'D:\test2.db'
    drive_list = [r'C:\\']
    make_table1(drive_list, save_folder)

    end = timer()
    print(timedelta(seconds=end - start))
