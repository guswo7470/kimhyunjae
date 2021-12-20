import os
import re
import sqlite3
import zipfile
import sys
import xlrd
import CompressInt
from tika import parser
import docx
from timeit import default_timer as timer
from datetime import timedelta
try:
    from xml.etree.cElementTree import XML
except ImportError:
    from xml.etree.ElementTree import XML
import tika
import pickle
tika.initVM()


def normalize_text1(buf):
    buf = str(buf).upper()
    buf = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/,?_+%#@$!^&*().\r\n\t -]', '', buf)
    buf = buf.replace('\r', ' ')
    buf = buf.replace('\n', ' ')
    buf = buf.replace('\t', ' ')
    return buf

def make_dic(buf, insert_index):
    dic = {}
    dic[buf] = insert_index
    return dic

def make_db_file(path, save_folder):
    file_num = 1
    insert_index = 0
    db_index = 0
    path_db = os.path.join(save_folder, 'path_table.db')
    conn2 = sqlite3.connect(path_db)
    cur2 = conn2.cursor()
    cur2.execute("CREATE TABLE IF NOT EXISTS path (document_num int, path text)")
    cur2.execute("CREATE INDEX IF NOT EXISTS path_IDX ON path (document_num)")
    dic = {}

    for root, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            name, ext = os.path.splitext(full_path)
            if file[0] == '$' or file[0] == '~':
                continue

            try:
                if ext[1:] == 'ppt' or ext[1:] == 'xls' or ext[1:] == 'doc' or ext[1:] == 'pdf' or ext[1:] == 'hwp':
                    print(full_path, file_num)
                    parsed = parser.from_file(full_path)
                    act = parsed['content']
                    if act != None:
                        buf = act.strip()
                    else:
                        buf = ''
                    buf = normalize_text1(buf)
                    for i in range(len(buf)):
                        if insert_index % 1000000 == 0:
                            if insert_index != 0:
                                for key, value in dic.items():
                                    key = "'" + key + "'"
                                    query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
                                    cur.execute(query)
                                conn.commit()
                                conn.close()
                                dic = {}
                                insert_index += 1
                            db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                            conn = sqlite3.connect(db_path)
                            cur = conn.cursor()
                            cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location text)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                            db_index += 1
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:
                            compressed_start = CompressInt.VariableByteCode().VB_encode([i]).hex()
                            result = make_dic(index_set, compressed_start)
                            if index_set in dic:
                                if dic[index_set][-2:] == '00':
                                    dic[index_set] = dic[index_set] + str(
                                        CompressInt.VariableByteCode().VB_encode([file_num]).hex())
                                dic[index_set] += result[index_set]
                            else:
                                result[index_set] = str(
                                    CompressInt.VariableByteCode().VB_encode([file_num]).hex()) + compressed_start
                                dic.update(result)
                        insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-2:] != '00':
                            dic[k] = dic[k] + '00'

                elif ext[1:] == 'pptx':
                    print(full_path, file_num)
                    document = zipfile.ZipFile(full_path)
                    nums = []
                    for d in document.namelist():
                        if d.startswith("ppt/slides/slide"):
                            nums.append(int(d[len("ppt/slides/slide"):-4]))
                    s_format = "ppt/slides/slide%s.xml"
                    slide_name_list = [s_format % x for x in sorted(nums)]
                    buf = ''
                    for slide in slide_name_list:
                        xml_content = document.read(slide)
                        tree = XML(xml_content)
                        if tree.tag == '{http://purl.oclc.org/ooxml/presentationml/main}sld':
                            NAMESPACE = '{http://purl.oclc.org/ooxml/drawingml/main}'
                        else:
                            NAMESPACE = '{http://schemas.openxmlformats.org/drawingml/2006/main}'
                        TEXT = NAMESPACE + 't'
                        for node in tree.iter(TEXT):
                            buf += normalize_text1(node.text)
                    for i in range(len(buf)):
                        if insert_index % 1000000 == 0:
                            if insert_index != 0:
                                for key, value in dic.items():
                                    key = "'" + key + "'"
                                    query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
                                    cur.execute(query)
                                conn.commit()
                                conn.close()
                                dic = {}
                                insert_index += 1
                            db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                            conn = sqlite3.connect(db_path)
                            cur = conn.cursor()
                            cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location text)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                            db_index += 1
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:
                            compressed_start = CompressInt.VariableByteCode().VB_encode([i]).hex()
                            result = make_dic(index_set, compressed_start)
                            if index_set in dic:
                                if dic[index_set][-2:] == '00':
                                    dic[index_set] = dic[index_set] + str(
                                        CompressInt.VariableByteCode().VB_encode([file_num]).hex())
                                dic[index_set] += result[index_set]
                            else:
                                result[index_set] = str(
                                    CompressInt.VariableByteCode().VB_encode([file_num]).hex()) + compressed_start
                                dic.update(result)
                        insert_index += 1
                    document.close()
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-2:] != '00':
                            dic[k] = dic[k] + '00'

                elif ext[1:] == 'xlsx':
                    print(full_path, file_num)
                    workbook = xlrd.open_workbook(full_path, on_demand=True)
                    start = 1
                    for sheet in workbook.sheet_names():
                        worksheet = workbook.sheet_by_name(sheet)
                        nrows = worksheet.nrows
                        ncols = worksheet.ncols
                        for row_num in range(nrows):
                            for col_num in range(ncols):
                                value = worksheet.cell_value(row_num, col_num)
                                buf = normalize_text1(value)
                                for i in range(len(buf)):
                                    if insert_index % 1000000 == 0:
                                        if insert_index != 0:
                                            for key, value in dic.items():
                                                key = "'" + key + "'"
                                                query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
                                                cur.execute(query)
                                            conn.commit()
                                            conn.close()
                                            dic = {}
                                            insert_index += 1
                                        db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                        conn = sqlite3.connect(db_path)
                                        cur = conn.cursor()
                                        cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location text)")
                                        cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                        db_index += 1
                                    index_set = buf[i:i + 2]
                                    if index_set == '  ' or index_set == ' ':
                                        continue
                                    else:
                                        compressed_start = CompressInt.VariableByteCode().VB_encode([start]).hex()
                                        result = make_dic(index_set, compressed_start)
                                        if index_set in dic:
                                            if dic[index_set][-2:] == '00':
                                                dic[index_set] = dic[index_set] + str(
                                                    CompressInt.VariableByteCode().VB_encode([file_num]).hex())
                                            dic[index_set] += result[index_set]
                                        else:
                                            result[index_set] = str(CompressInt.VariableByteCode().VB_encode(
                                                [file_num]).hex()) + compressed_start
                                            dic.update(result)
                                        start += 1
                                    insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-2:] != '00':
                            dic[k] = dic[k] + '00'

                elif ext[1:] == 'docx':
                    print(full_path, file_num)
                    document = docx.Document(full_path)
                    start = 1
                    for x, paragraph in enumerate(document.paragraphs):
                        buf = normalize_text1(paragraph.text)
                        for i in range(len(buf)):
                            if insert_index % 1000000 == 0:
                                if insert_index != 0:
                                    for key, value in dic.items():
                                        key = "'" + key + "'"
                                        query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
                                        cur.execute(query)
                                    conn.commit()
                                    conn.close()
                                    dic = {}
                                    insert_index += 1
                                db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                conn = sqlite3.connect(db_path)
                                cur = conn.cursor()
                                cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location text)")
                                cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                db_index += 1
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                compressed_start = CompressInt.VariableByteCode().VB_encode([start]).hex()
                                result = make_dic(index_set, compressed_start)
                                if index_set in dic:
                                    if dic[index_set][-2:] == '00':
                                        dic[index_set] = dic[index_set] + str(
                                            CompressInt.VariableByteCode().VB_encode([file_num]).hex())
                                    dic[index_set] += result[index_set]
                                else:
                                    result[index_set] = str(CompressInt.VariableByteCode().VB_encode(
                                        [file_num]).hex()) + compressed_start
                                    dic.update(result)
                                start += 1
                            insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-2:] != '00':
                            dic[k] = dic[k] + '00'

                elif ext[1:] == 'txt':
                    print(full_path, file_num)
                    f = open(full_path, 'rb')
                    lines = f.readlines()
                    start = 1
                    for line in lines:
                        buf = line.decode('utf-8', errors='replace')
                        buf = normalize_text1(buf)
                        for i in range(len(buf)):
                            if insert_index % 1000000 == 0:
                                if insert_index != 0:
                                    for key, value in dic.items():
                                        key = "'" + key + "'"
                                        query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
                                        cur.execute(query)
                                    conn.commit()
                                    conn.close()
                                    dic = {}
                                insert_index += 1
                                db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                conn = sqlite3.connect(db_path)
                                cur = conn.cursor()
                                cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location blob)")
                                cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                db_index += 1
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                compressed_start = CompressInt.VariableByteCode().VB_encode([start]).hex()
                                result = make_dic(index_set, compressed_start)
                                if index_set in dic:
                                    if dic[index_set][-2:] == '00':
                                        dic[index_set] = dic[index_set] + str(CompressInt.VariableByteCode().VB_encode([file_num]).hex())
                                    dic[index_set] += result[index_set]
                                else:
                                    result[index_set] = str(CompressInt.VariableByteCode().VB_encode([file_num]).hex()) + compressed_start
                                    dic.update(result)
                                start += 1
                            insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-2:] != '00':
                            dic[k] = dic[k] + '00'

                file_num += 1

            except:
                continue

    for key, value in dic.items():
        key = "'" + key + "'"
        query = "INSERT OR REPLACE INTO word_set (word, location) VALUES (" + key + ", x'" + value + "')"
        cur.execute(query)

    conn.commit()
    cur.close()
    conn2.commit()
    cur2.close()
#INSERT OR REPLACE INTO word_set (word, location) VALUES (0, x'820b60890b609e00')

if __name__ == '__main__':
    path = r'C:\Users\hjkim\OneDrive\바탕 화면\새 폴더'
    save_folder = r'D:\한파일정수압축c드라이브'
    start = timer()

    make_db_file(path, save_folder)

    end = timer()
    print(timedelta(seconds=end - start))