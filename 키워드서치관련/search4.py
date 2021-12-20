import os
import re
import sqlite3
import zipfile
from collections import OrderedDict

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

def make_dic(ngram, col1):
    dic = {}
    dic[ngram] = col1
    yield dic

def make_db_file(path, save_folder):
    file_num = 0
    insert_index = 0
    db_index = 0
    path_db = os.path.join(save_folder, 'path_table.db')
    conn2 = sqlite3.connect(path_db)
    cur2 = conn2.cursor()
    cur2.execute("CREATE TABLE IF NOT EXISTS path (document_num int, path text)")
    cur2.execute("CREATE INDEX IF NOT EXISTS path_IDX ON path (document_num)")

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
                        if insert_index % 20000000 == 0:
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
                            if insert_index % 20000000 == 0:
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
                        if insert_index % 20000000 == 0:
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
                                    if insert_index % 20000000 == 0:
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
                            if insert_index % 20000000 == 0:
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


def get_word_set(save_folder, word, document_num=None, word_num=None):
    db_index = 0
    while True:
        db_path = os.path.join(save_folder, 'index_' + str(db_index) + '.db')
        if not os.path.exists(db_path):
            break
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        query = "SELECT * FROM word_set WHERE word = '" + word + "'"
        if document_num is not None:
            query += " and document_num = " + str(document_num)
        if word_num is not None:
            query += " and word_num = " + str(word_num)
        cur.execute(query)
        for word_set in cur.fetchall():
            yield word_set
        db_index += 1
        cur.close()


def find_word_call(save_folder, find_text, document_num=None, word_num=None):
    if len(find_text) < 2 and document_num is not None:
        yield [find_text, document_num, word_num]

    find_text = str(find_text).upper()
    for word_set in get_word_set(save_folder, find_text[:2], document_num, word_num):
        for result in find_word_call(save_folder, find_text[1:], word_set[1], word_set[2] + 1):
            yield result


def find_word(save_folder, word):
    for ret_set in find_word_call(save_folder, word):
        ret_set[0] = word
        ret_set[2] -= (len(word) - 1)
        yield ret_set

def find_path(save_folder, word):
    result = []

    for a in find_word(save_folder, word):
        path_table_db = os.path.join(save_folder, 'path_table.db')
        conn = sqlite3.connect(path_table_db)
        cur = conn.cursor()

        query = "SELECT path FROM path WHERE document_num = '" + str(a[1]) + "'"
        cur.execute(query)

        for i in cur.fetchall():
            result.append(i[0])

    result = OrderedDict.fromkeys(result)
    for a in result:
        yield a


if __name__ == '__main__':
    path = 'C:\\'
    save_folder = 'D:\\2천만'
    start = timer()
    #make_db_file(path, save_folder)

    for i in find_path(save_folder, '김현재'):
        print(i)


    end = timer()
    print(timedelta(seconds=end - start))




