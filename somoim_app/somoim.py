#made by sumin and dreamtree
import hashlib
import binascii
import base64
import os
from Crypto.Cipher import AES
from Crypto.Hash import HMAC, SHA256, SHA384, SHA512
import sqlite3 
import shutil


def openDB():
    conn=sqlite3.connect('sqliteGroupChats.db')
    cursor=conn.cursor()
    cursor.execute("SELECT msg_id,msg FROM group_chats")
    rows=cursor.fetchall()
    conn.close()
    return rows

def decryptmsg(msg,key,iv):
    msg = str(msg)
    decode=base64.b64decode(msg)
    decryptor=AES.new(key,AES.MODE_CBC,iv)
    result=decryptor.decrypt(decode)
    result=result[:-1*result[-1]] #패딩제거
    return result.decode('utf-8')


def makekeyandiv():
    iv_int = 0
    #text='@9cnl1dir_a6[B@35ad1a1'
    text='@9cnl1dir_a6[B@c5ae829'
    key=hashlib.sha256(text.encode('utf-8')).digest()
    iv='fldsjfodasjifuds'.encode()
    return key,iv


def makeDecryptedDB(name,msglist):
    shutil.copy('sqliteGroupChats.db',name)
    conn=sqlite3.connect(name)
    cursor=conn.cursor()
    
    for i in msglist:
        #i[0] = msg_id
        #i[1] = msg
        # print (i)
        # print ("update group_chats set msg = {} WHERE msg_id = {}".format(i[1],i[0]))
        cmd="update group_chats set msg =? WHERE msg_id =?"
        cursor.execute(cmd,(i[1],i[0]))
    conn.commit()
    conn.close()

if __name__=="__main__":
    data=openDB()
    
    msglist = list()
    for d in data:
        key,iv=makekeyandiv()
        msglist.append((d[0],decryptmsg(d[1],key,iv)))
    
    makeDecryptedDB('sqliteGroupChats-decrypted.db',msglist)

