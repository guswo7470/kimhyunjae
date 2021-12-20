import os
import re
import sqlite3
import zipfile
import sys

import xlrd
from tika import parser
import docx
from timeit import default_timer as timer
from datetime import timedelta
try:
    from xml.etree.cElementTree import XML
except ImportError:
    from xml.etree.ElementTree import XML
import tika
tika.initVM()


def word_ngrams(buf, n):
    output = []
    for i in range(len(buf)-n+1):
        output.append(buf[i:i+n])
    return output

def normalize_text1(buf):
    buf = str(buf).upper()
    buf = re.sub('[^A-Z0-9ㄱ-ㅣ가-힣:/,?_+%#@$!^&*().\r\n\t -]', '', buf)
    buf = buf.replace('\r', ' ')
    buf = buf.replace('\n', ' ')
    buf = buf.replace('\t', ' ')
    return buf

def make_db_file(path, save_folder):
    file_num = 0
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
                                conn.commit()
                                conn.close()
                                insert_index += 1
                            db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                            conn = sqlite3.connect(db_path)
                            cur = conn.cursor()
                            cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, document_num int, word_num int)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                            cur.execute("CREATE INDEX IF NOT EXISTS document_num_word_set_IDX ON word_set (document_num)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_num_word_set_IDX ON word_set (word_num)")
                            db_index += 1
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:

                            cur.execute("INSERT INTO word_set VALUES('" + index_set + "', " + str(file_num) + ", " + str(i) + ")")
                        insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")

                elif ext[1:] == 'txt':
                    print(full_path, file_num)
                    f = open(full_path, 'rb')
                    lines = f.readlines()
                    start = 0
                    for line in lines:
                        buf = line.decode('utf-8', errors='replace')
                        buf = normalize_text1(buf)
                        for i in range(len(buf)):
                            if insert_index % 1000000 == 0:
                                if insert_index != 0:
                                    conn.commit()
                                    conn.close()
                                    insert_index += 1
                                db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                conn = sqlite3.connect(db_path)
                                cur = conn.cursor()
                                cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, document_num int, word_num int)")
                                cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                cur.execute("CREATE INDEX IF NOT EXISTS document_num_word_set_IDX ON word_set (document_num)")
                                cur.execute("CREATE INDEX IF NOT EXISTS word_num_word_set_IDX ON word_set (word_num)")
                                db_index += 1
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                cur.execute("INSERT INTO word_set VALUES('" + index_set + "', " + str(file_num) + ", " + str(start) + ")")
                                start += 1
                            insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")

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
                                conn.commit()
                                conn.close()
                                insert_index += 1
                            db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                            conn = sqlite3.connect(db_path)
                            cur = conn.cursor()
                            cur.execute(
                                "CREATE TABLE IF NOT EXISTS word_set (word text, document_num int, word_num int)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                            cur.execute(
                                "CREATE INDEX IF NOT EXISTS document_num_word_set_IDX ON word_set (document_num)")
                            cur.execute("CREATE INDEX IF NOT EXISTS word_num_word_set_IDX ON word_set (word_num)")
                            db_index += 1
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:
                            cur.execute("INSERT INTO word_set VALUES('" + index_set + "', " + str(file_num) + ", " + str(i) + ")")
                        insert_index += 1
                    document.close()
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")

                elif ext[1:] == 'xlsx':
                    print(full_path, file_num)
                    workbook = xlrd.open_workbook(full_path, on_demand=True)
                    start = 0
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
                                            conn.commit()
                                            conn.close()
                                            insert_index += 1
                                        db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                        conn = sqlite3.connect(db_path)
                                        cur = conn.cursor()
                                        cur.execute(
                                            "CREATE TABLE IF NOT EXISTS word_set (word text, document_num int, word_num int)")
                                        cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                        cur.execute(
                                            "CREATE INDEX IF NOT EXISTS document_num_word_set_IDX ON word_set (document_num)")
                                        cur.execute(
                                            "CREATE INDEX IF NOT EXISTS word_num_word_set_IDX ON word_set (word_num)")
                                        db_index += 1
                                    index_set = buf[i:i + 2]
                                    if index_set == '  ' or index_set == ' ':
                                        continue
                                    else:
                                        cur.execute("INSERT INTO word_set VALUES('" + index_set + "', " + str(file_num) + ", " + str(start) + ")")
                                        start += 1
                                    insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")

                elif ext[1:] == 'docx':
                    print(full_path, file_num)
                    document = docx.Document(full_path)
                    start = 0
                    for x, paragraph in enumerate(document.paragraphs):
                        buf = normalize_text1(paragraph.text)
                        for i in range(len(buf)):
                            if insert_index % 1000000 == 0:
                                if insert_index != 0:
                                    conn.commit()
                                    conn.close()
                                    insert_index += 1
                                db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
                                conn = sqlite3.connect(db_path)
                                cur = conn.cursor()
                                cur.execute(
                                    "CREATE TABLE IF NOT EXISTS word_set (word text, document_num int, word_num int)")
                                cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")
                                cur.execute(
                                    "CREATE INDEX IF NOT EXISTS document_num_word_set_IDX ON word_set (document_num)")
                                cur.execute(
                                    "CREATE INDEX IF NOT EXISTS word_num_word_set_IDX ON word_set (word_num)")
                                db_index += 1
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                cur.execute("INSERT INTO word_set VALUES('" + index_set + "', " + str(file_num) + ", " + str(start) + ")")
                                start += 1
                            insert_index += 1
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")

                file_num += 1

            except:
                continue

    conn.commit()
    conn.close()
    conn2.commit()
    cur2.close()


def make_dic(doc_num, word_num):
    dic = {}
    dic[doc_num] = [word_num]
    return dic

def make_array(save_folder, word):
    result = []
    word_list = word_ngrams(word.upper(), 2)
    for word in word_list:
        dic = {}
        db_index = 0
        while True:
            db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
            if not os.path.exists(db_path):
                break
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            query = "SELECT * FROM word_set WHERE word = '" + word + "'"
            cur.execute(query)
            for word_set in cur.fetchall():
                entry = make_dic(word_set[1], word_set[2])
                for key, value in entry.items():
                    if key in dic:
                        dic[key] += value
                    else:
                        dic.update(entry)
            db_index += 1
        result.append(dic)
    return result


def compare_table(dic1, dic2):
    dic = {}
    for doc_num, word_num in dic1.items():
        if doc_num in dic2:
            new_word_num = []
            for a in word_num:
                new_word_num.append(a+1)
            word_num2 = dic2[doc_num]
            compare = list(set(new_word_num).intersection(word_num2))
            if compare != []:
                dic[doc_num] = compare
    return dic


def find_word(table_list):
    length_table = len(table_list)

    if length_table < 1:
        return

    elif length_table == 1:
        return table_list[0]

    elif length_table == 2:
        c = compare_table(table_list[0], table_list[1])
        return c

    else:
        c = compare_table(table_list[0], table_list[1])
        for i in range(length_table-2):
            d = compare_table(c, table_list[i+2])
            c = d
        return d

def find_path(save_folder, word):
    table_list = make_array(save_folder, word)
    word_set = find_word(table_list)
    db_path = os.path.join(save_folder, 'path_table.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for document_num, word_num in word_set.items():
        cur.execute("SELECT path FROM path WHERE document_num = '" + str(document_num) + "'")
        for i in cur.fetchall():
            yield i[0]



if __name__ == '__main__':
    path = 'X:\\'
    save_folder = r'D:\백만'
    start = timer()

    #make_db_file(path, save_folder)

    word = '손상된'
    '''for i in find_path(save_folder, word):
        print(i)'''


    end = timer()
    print(timedelta(seconds=end - start))




def binarySearch(array, target, left, right):
    middle_idx = (left+right)//2
    middle = array[middle_idx]
    if target == middle:
        print('answer {}'.format(target))
    elif middle > target:
        binarySearch(array, target,left,middle_idx-1)
    elif middle < target:
        binarySearch(array, target,middle_idx+1,right)
    else:
        return False

target = 25
m_list = [30, 94, 27, 92, 21, 37, 25, 47, 25, 53, 98, 19, 32, 32, 7]
length = len(m_list)
m_list.sort()
print(m_list)
left = 0
right = length - 1
binarySearch(m_list, target, left, right)