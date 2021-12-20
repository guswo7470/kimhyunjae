import os
import sqlite3
from collections import OrderedDict
import CompressInt


def word_ngrams(buf, n):
    output = []
    for i in range(len(buf)-n+1):
        output.append(buf[i:i+n])
    return output


def make_dic(buf, insert_index):
    dic = {buf: insert_index}
    return dic


def compress_concate(save_folder, word):
    table_list = []
    word_list = word_ngrams(word, 2)
    db_path = os.path.join(save_folder, 'index.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for w in word_list:
        cur.execute("SELECT location FROM word_set WHERE word = ?", (w,))
        result = []
        for location in cur.fetchall():
            location = CompressInt.VariableByteCode().VB_decode(location[0])
            if not result:
                result += location
            else:
                if result[-1] != 0:
                    result = result + location[1:]
                else:
                    result = result + location
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
    db_path = os.path.join(save_folder, 'path.db')
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if len(word) >= 2:
        word_list = word_ngrams(word, 2)
        for wordset in find_wordset(save_folder, word):
            if len(wordset) == len(word_list) - 1 or len(word_list) == 1:
                document_num = wordset[0][0]
                cur.execute("SELECT path FROM path WHERE document_num = '" + str(document_num) + "'")
                for path in cur.fetchall():
                    yield path[0], wordset[0][1]

    elif len(word) == 1:
        db_path2 = os.path.join(save_folder, 'index.db')
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
                    yield path[0], wordset[0]