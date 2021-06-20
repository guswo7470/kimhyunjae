import hashlib
import sqlite3

UIN = '60059450'.encode()
IMEI = '351564855412227'.encode()
Account = 'wxid_4sx4zhxvmvta22'.encode()

# En*.db
En_key =  IMEI + UIN
key1 = hashlib.md5(En_key).hexdigest()
real_En_key = key1[:7]
print('En*.db key = ', real_En_key)

# FTS5IndexMicroMsg_encrypt.db
FTS_key = UIN + IMEI + Account
key2 = hashlib.md5(FTS_key).hexdigest()
real_FTS_key = key2[:7]
print('FTS5IndexMicroMsg_encrypt.db key = ', real_FTS_key)

# MicroMsgPriority.db
Micro_key = UIN + Account + IMEI
key3 = hashlib.md5(Micro_key).hexdigest()
real_Micro_key = key3[:7]
print('MicroMsgPriority.db key = ', real_Micro_key)