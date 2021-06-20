
from pysqlcipher3 import dbapi2 as sqlite

# En*.db decrypt
conn = sqlite.connect("EnMicroMsg.db")
c = conn.cursor()
c.execute("PRAGMA key = '083f78e';")
c.execute("PRAGMA cipher_use_hmac = OFF;")
c.execute("PRAGMA cipher_page_size = 1024;")
c.execute("PRAGMA kdf_iter = 4000;")
c.execute("ATTACH DATABASE 'Decrypt_En.db' AS db KEY '';")
c.execute("SELECT sqlcipher_export( 'db' );" )
c.execute("DETACH DATABASE db;")
c.close()

