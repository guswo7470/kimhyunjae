import os
import chardet
import sqlite3
import zipfile
import xlrd
import CompressInt
from collections import OrderedDict
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


def normalize_text1(buf):
    buf = str(buf).upper()
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
    index_path = os.path.join(save_folder, 'index_table.db')
    conn = sqlite3.connect(index_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS word_set (word text, location blob)")
    cur.execute("CREATE INDEX IF NOT EXISTS word_word_set_IDX ON word_set (word)")

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
                    print(full_path)
                    parsed = parser.from_file(full_path)
                    act = parsed['content']
                    start = 1
                    if act != None:
                        buf = act.strip()
                    else:
                        buf = ''
                    buf = normalize_text1(buf)
                    for i in range(len(buf)):
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:
                            compressed_start = CompressInt.VariableByteCode().VB_encode([start])
                            compressed_file_num = CompressInt.VariableByteCode().VB_encode([file_num])
                            result = make_dic(index_set, compressed_start)
                            if index_set in dic:
                                if index_set in dic:
                                    if dic[index_set][-1:] == b'\x00':
                                        dic[index_set] += compressed_file_num
                                    dic[index_set] += compressed_start
                            else:
                                result[index_set] = compressed_file_num + compressed_start
                                dic.update(result)
                        start += 1
                        if len(dic[index_set]) > 100000:
                            cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (index_set, dic[index_set]))
                            conn.commit()
                            dic[index_set] = compressed_file_num
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-1:] != b'\x00':
                            dic[k] = dic[k] + b'\x00'
                    file_num += 1

                elif ext[1:] == 'pptx':
                    print(full_path)
                    document = zipfile.ZipFile(full_path)
                    nums = []
                    start = 1
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
                        index_set = buf[i:i + 2]
                        if index_set == '  ' or index_set == ' ':
                            continue
                        else:
                            compressed_start = CompressInt.VariableByteCode().VB_encode([start])
                            compressed_file_num = CompressInt.VariableByteCode().VB_encode([file_num])
                            result = make_dic(index_set, compressed_start)
                            if index_set in dic:
                                if index_set in dic:
                                    if dic[index_set][-1:] == b'\x00':
                                        dic[index_set] += compressed_file_num
                                    dic[index_set] += compressed_start
                            else:
                                result[index_set] = compressed_file_num + compressed_start
                                dic.update(result)
                        start += 1
                        if len(dic[index_set]) > 100000:
                            cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (index_set, dic[index_set]))
                            conn.commit()
                            dic[index_set] = compressed_file_num
                    document.close()
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-1:] != b'\x00':
                            dic[k] = dic[k] + b'\x00'
                    file_num += 1

                elif ext[1:] == 'xlsx':
                    print(full_path)
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
                                    index_set = buf[i:i + 2]
                                    if index_set == '  ' or index_set == ' ':
                                        continue
                                    else:
                                        compressed_start = CompressInt.VariableByteCode().VB_encode([start])
                                        compressed_file_num = CompressInt.VariableByteCode().VB_encode([file_num])
                                        result = make_dic(index_set, compressed_start)
                                        if index_set in dic:
                                            if dic[index_set][-1:] == b'\x00':
                                                dic[index_set] += compressed_file_num
                                            dic[index_set] += compressed_start
                                        else:
                                            result[
                                                index_set] = compressed_file_num + compressed_start
                                            dic.update(result)
                                    start += 1
                                    if len(dic[index_set]) > 100000:
                                        cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (index_set, dic[index_set]))
                                        conn.commit()
                                        dic[index_set] = compressed_file_num
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-1:] != b'\x00':
                            dic[k] = dic[k] + b'\x00'
                    file_num += 1

                elif ext[1:] == 'docx':
                    print(full_path)
                    document = docx.Document(full_path)
                    start = 1
                    for x, paragraph in enumerate(document.paragraphs):
                        buf = normalize_text1(paragraph.text)
                        for i in range(len(buf)):
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                compressed_start = CompressInt.VariableByteCode().VB_encode([start])
                                compressed_file_num = CompressInt.VariableByteCode().VB_encode([file_num])
                                result = make_dic(index_set, compressed_start)
                                if index_set in dic:
                                    if dic[index_set][-1:] == b'\x00':
                                        dic[index_set] += compressed_file_num
                                    dic[index_set] += compressed_start
                                else:
                                    result[
                                        index_set] = compressed_file_num + compressed_start
                                    dic.update(result)
                            start += 1
                            if len(dic[index_set]) > 100000:
                                cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (index_set, dic[index_set]))
                                conn.commit()
                                dic[index_set] = compressed_file_num
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-1:] != b'\x00':
                            dic[k] = dic[k] + b'\x00'
                    file_num += 1

                elif ext[1:] == 'txt':
                    print(full_path)
                    check_f = open(full_path, 'rb')
                    check = check_f.readline()
                    detect = chardet.detect(check)
                    encoding = detect['encoding']
                    if encoding == 'ascii' or encoding == 'Windows-1254':
                        encoding = 'utf-8'
                    check_f.close()
                    f = open(full_path, 'r', encoding=encoding)
                    lines = f.readlines()
                    start = 1
                    for line in lines:
                        buf = normalize_text1(line)
                        for i in range(len(buf)):
                            index_set = buf[i:i + 2]
                            if index_set == '  ' or index_set == ' ':
                                continue
                            else:
                                compressed_start = CompressInt.VariableByteCode().VB_encode([start])
                                compressed_file_num = CompressInt.VariableByteCode().VB_encode([file_num])
                                result = make_dic(index_set, compressed_start)
                                if index_set in dic:
                                    if dic[index_set][-1:] == b'\x00':
                                        dic[index_set] += compressed_file_num
                                    dic[index_set] += compressed_start
                                else:
                                    result[index_set] = compressed_file_num + compressed_start
                                    dic.update(result)
                            start += 1
                            if len(dic[index_set]) > 100000:
                                cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (index_set, dic[index_set]))
                                conn.commit()
                                dic[index_set] = compressed_file_num
                    cur2.execute("INSERT INTO path VALUES('" + str(file_num) + "', '" + str(full_path) + "')")
                    for k, v in dic.items():
                        if dic[k][-1:] != b'\x00':
                            dic[k] = dic[k] + b'\x00'
                    file_num += 1
                    f.close()

            except PermissionError:
                continue
            except UnicodeDecodeError:
                continue


    for key, value in dic.items():
        cur.execute("INSERT INTO word_set (word, location) VALUES (?, ?)", (key, value))
    conn.commit()
    cur.close()
    conn2.commit()
    cur2.close()


def word_ngrams(buf, n):
    output = []
    for i in range(len(buf)-n+1):
        output.append(buf[i:i+n])
    return output


def compress_concate(save_folder, word):
    table_list = []
    word_list = word_ngrams(word, 2)
    db_path = os.path.join(save_folder, 'index_table.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for w in word_list:
        cur.execute("SELECT location FROM word_set WHERE word = ?", (w,))
        result = []
        for location in cur.fetchall():
            location = CompressInt.VariableByteCode().VB_decode(location[0])
            if result == []:
                result += location
            else:
                if result[-1] != 0:
                    result = result + location[1:]
        table_list.append(result)
    return table_list


def make_table_entry(save_folder, word):
    table_list = []
    for location in compress_concate(save_folder, word):
        result = {}
        zero_index = [i for i, ele in enumerate(location) if ele == 0]
        p = 0
        for zero in zero_index:
            dic = make_dic(location[p], location[p+1:zero])
            result.update(dic)
            p = zero+1
            if zero == zero_index[-1]:
                break
        table_list.append(result)
    return table_list


def make_input(dic):
    for k, v_list in dic.items():
        for v in v_list:
            yield k, v


def binary_search(arr, target, low=None, high=None):
    try:
        low, high = low or 0, high or len(arr) - 1
        if low > high:
            return None
        mid = (low + high) // 2
        if arr[mid] > target:
            return binary_search(arr, target, low, mid)
        if arr[mid] == target:
            return target
        if arr[mid] < target:
            return binary_search(arr, target, mid + 1, high)
    except RecursionError:
        return None
    except IndexError:
        return None


def find_wordset(save_folder, word):
    table_list = make_table_entry(save_folder, word)
    if len(table_list) >= 2:
        for i_k, i_v in make_input(table_list[0]):
            result = []
            for dic in table_list[1:]:
                if i_k in dic:
                    v = i_v + 1
                    search_result = binary_search(sorted(dic[i_k]), v, 0, len(dic[i_k]))
                    if search_result is not None and search_result != -1:
                        result.append((i_k, i_v))
                        i_v = v
                    else:
                        break
                else:
                    break
            yield result

    elif len(table_list) == 1:
        for i_k, i_v in make_input(table_list[0]):
            yield [(i_k, i_v)]


def find_path(save_folder, word):
    word = word.upper()
    db_path = os.path.join(save_folder, 'path_table.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if len(word) >= 2:
        word_list = word_ngrams(word, 2)
        for wordset in find_wordset(save_folder, word):
            if len(wordset) == len(word_list) - 1 or len(word_list) == 1:
                document_num = wordset[0][0]
                cur.execute("SELECT path FROM path WHERE document_num = '" + str(document_num) + "'")
                for path in cur.fetchall():
                    yield word, path[0], wordset[0]

    elif len(word) == 1:
        db_path2 = os.path.join(save_folder, 'index_table.db')
        conn2 = sqlite3.connect(db_path2)
        cur2 = conn2.cursor()
        cur2.execute("SELECT word FROM word_set WHERE word LIKE ?", ('%'+word+'%',))
        word_list = []
        for two_word in cur2.fetchall():
            word_list.append(two_word[0])
        word_list = OrderedDict.fromkeys(word_list)
        for w in word_list:
            for wordset in find_wordset(save_folder, w):
                document_num = wordset[0][0]
                cur.execute("SELECT path FROM path WHERE document_num = '" + str(document_num) + "'")
                for path in cur.fetchall():
                    yield w, path[0], wordset[0]


if __name__ == '__main__':
    path = 'C:\\'
    save_folder = 'D:\\비교'
    start = timer()

    make_db_file(path, save_folder)

    word = '김'
    #SELECT word FROM word_set WHERE word like '%김%'
    '''for i in find_path(save_folder, word):
        #print(i)
        None'''


    end = timer()
    print(timedelta(seconds=end - start))