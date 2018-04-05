import os, sys
import checkfile, json, pymssql, pymysql, threading, time, itertools, vigenere, csv
from pprint import pprint
from getpass import getpass

# (C) Copyright 2018 Heru Arief Wijaya (http://belajararief.com/) untuk INDONESIA.

done = False

def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rPush Data ' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')

def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec) 
        func()  
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t

status = False
def selesai():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if status:
            break
        sys.stdout.write('\r  ' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')

print ("Selamat datang di Cek UPDATE SIMDA BMD 2.0.7.10 ---- v1.0.0 by @hoaaah")
print ("Aplikasi ini akan melakukan pengecekan terhadap permasalahan pada DB BMD sebelum dilakukan updating ke versi 2.0.7.10.")
key = {
    'key1': 'donnoWhatToDo',
    'key2': 'yesManPaleLo'
}

sourceServer, sourceUsername, sourcePassword, sourceDb = input("Server: "), input("Username: "), getpass(), input("Source Database: ")


print("""------Memeriksa Koneksi-------""")
try:
    sourceConn = pymssql.connect(sourceServer,  sourceUsername, sourcePassword, sourceDb)
except ConnectionError:
    print("""
    [x] Data Source Connection failed, check again your config.json
    """)
finally:
    print("""
    """)
    if (sourceConn):
        print("""       [v] Source Connection Valid""")

# write to CSV first
writeToCsv = csv.writer(open("output.csv", "w"))


t = threading.Thread(target = selesai)
t.start()


print("""
------Memeriksa Permasalahan 1-------
Permasalahan 1 yaitu ID Pmeda di Riwayat tidak memiliki induk pada KIB Induk""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBAR a LEFT JOIN Ta_KIB_A b ON a.IDPemda = b.IDPemda WHERE b.IDPemda IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBBR a LEFT JOIN Ta_KIB_B b ON a.IDPemda = b.IDPemda WHERE b.IDPemda IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBCR a LEFT JOIN Ta_KIB_C b ON a.IDPemda = b.IDPemda WHERE b.IDPemda IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBDR a LEFT JOIN Ta_KIB_D b ON a.IDPemda = b.IDPemda WHERE b.IDPemda IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBER a LEFT JOIN Ta_KIB_E b ON a.IDPemda = b.IDPemda WHERE b.IDPemda IS NULL
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if(result == 0) :
    print("------Permasalahan 1 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 1: Solusi data riwayat yang tidak ada induk akan dihapus.'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 2-------
Permasalahan 2 yaitu terdapat no register ganda untuk suatu jenis aset""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, jumlah
    FROM
    (
        SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, COUNT(IDPemda) AS jumlah
        FROM Ta_KIB_A
        GROUP BY Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register
    ) a WHERE jumlah > 1
    UNION ALL
    SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, jumlah
    FROM
    (
        SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, COUNT(IDPemda) AS jumlah
        FROM Ta_KIB_B 
        GROUP BY Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register
    ) a WHERE jumlah > 1
    UNION ALL 
    SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, jumlah
    FROM
    (
        SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, COUNT(IDPemda) AS jumlah
        FROM Ta_KIB_C
        GROUP BY Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register
    ) a WHERE jumlah > 1
    UNION ALL 
    SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, jumlah
    FROM
    (
        SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, COUNT(IDPemda) AS jumlah
        FROM Ta_KIB_D
        GROUP BY Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register
    ) a WHERE jumlah > 1
    UNION ALL
    SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, jumlah
    FROM
    (
        SELECT Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register, COUNT(IDPemda) AS jumlah
        FROM Ta_KIB_E 
        GROUP BY Kd_Bidang, Kd_Unit, Kd_Sub, Kd_UPB, Kd_Aset1, Kd_Aset2, Kd_Aset3, Kd_Aset4, Kd_Aset5, No_Register
    ) a WHERE jumlah > 1
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 2 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 2: Solusi perbaikan fungsi perhitungan no register.'])
    writeToCsv.writerow(['Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'jumlah'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 3-------
Permasalahan 3 yaitu Tanggal Perolehan di KIB Berbeda dengan di KIB Riwayat""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT
    a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBAR a LEFT JOIN Ta_KIB_A b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan != b.Tgl_Perolehan
    UNION ALL
    SELECT
    a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBBR a LEFT JOIN Ta_KIB_B b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan != b.Tgl_Perolehan
    UNION ALL
    SELECT
    a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBCR a LEFT JOIN Ta_KIB_C b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan != b.Tgl_Perolehan
    UNION ALL
    SELECT
    a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBDR a LEFT JOIN Ta_KIB_D b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan != b.Tgl_Perolehan
    UNION ALL
    SELECT
    a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBER a LEFT JOIN Ta_KIB_E b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan != b.Tgl_Perolehan
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 3 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 3: Solusi penyesuaian tanggal perolehan di riwayat mengikuti perolehan di KIB Induk'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 4-------
Permasalahan 4 yaitu Tanggal Pembukuan Berbeda dengan Tanggal Dokumen Pindah (untuk aset yang pindah skpd)""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBAR a WHERE Kd_Riwayat = 3 AND Tgl_Dokumen != Tgl_Pembukuan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBBR a WHERE Kd_Riwayat = 3 AND Tgl_Dokumen != Tgl_Pembukuan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBCR a WHERE Kd_Riwayat = 3 AND Tgl_Dokumen != Tgl_Pembukuan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBDR a WHERE Kd_Riwayat = 3 AND Tgl_Dokumen != Tgl_Pembukuan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBER a WHERE Kd_Riwayat = 3 AND Tgl_Dokumen != Tgl_Pembukuan
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 4 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 4: Solusi penyesuaian tanggal pembukuan dengan tanggal dokumen pindah.'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 5-------
Permasalahan 5 yaitu Aset dirubah kondisi RB lalu indah skpd, kondisinya kembali ke BAIK""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, 
    a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, 
    a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBBR a
    LEFT JOIN Ta_KIB_B b ON a.IDPemda = b.IDPemda
    LEFT JOIN 
    (
        SELECT * FROM Ta_KIBBR d WHERE d.Kd_Riwayat = 1
    ) c ON a.IDPemda = c.IDPemda AND (a.No_Urut - c.No_Urut) = 1
    WHERE a.Kd_Riwayat = 3 AND ((a.Kondisi = 1 AND b.Kondisi = 3) OR (a.Kondisi = 1 AND c.Kondisi = 3))
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, 
    a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, 
    a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBCR a
    LEFT JOIN Ta_KIB_C b ON a.IDPemda = b.IDPemda
    LEFT JOIN 
    (
        SELECT * FROM Ta_KIBCR d WHERE d.Kd_Riwayat = 1
    ) c ON a.IDPemda = c.IDPemda AND (a.No_Urut - c.No_Urut) = 1
    WHERE a.Kd_Riwayat = 3 AND ((a.Kondisi = 1 AND b.Kondisi = 3) OR (a.Kondisi = 1 AND c.Kondisi = 3))
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, 
    a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, 
    a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBDR a
    LEFT JOIN Ta_KIB_D b ON a.IDPemda = b.IDPemda
    LEFT JOIN 
    (
        SELECT * FROM Ta_KIBDR d WHERE d.Kd_Riwayat = 1
    ) c ON a.IDPemda = c.IDPemda AND (a.No_Urut - c.No_Urut) = 1
    WHERE a.Kd_Riwayat = 3 AND ((a.Kondisi = 1 AND b.Kondisi = 3) OR (a.Kondisi = 1 AND c.Kondisi = 3))
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, 
    a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, 
    a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBER a
    LEFT JOIN Ta_KIB_E b ON a.IDPemda = b.IDPemda
    LEFT JOIN 
    (
        SELECT * FROM Ta_KIBER d WHERE d.Kd_Riwayat = 1
    ) c ON a.IDPemda = c.IDPemda AND (a.No_Urut - c.No_Urut) = 1
    WHERE a.Kd_Riwayat = 3 AND ((a.Kondisi = 1 AND b.Kondisi = 3) OR (a.Kondisi = 1 AND c.Kondisi = 3))
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 5 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 5: Solusi Penyesuaian kondisi di KIB dan riwayat.'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 6-------
SKIPPED""")

print("""
------Memeriksa Permasalahan 7-------
Permasalahan 7 yaitu Tanggal Pembukuan lebih kecil dari tanggal perolehan""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIB_A a WHERE Tgl_Pembukuan < Tgl_Perolehan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIB_B a WHERE Tgl_Pembukuan < Tgl_Perolehan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIB_C a WHERE Tgl_Pembukuan < Tgl_Perolehan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIB_D a WHERE Tgl_Pembukuan < Tgl_Perolehan
    UNION ALL
    SELECT a.IDPemda, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIB_E a WHERE Tgl_Pembukuan < Tgl_Perolehan
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 7 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 7: Solusi penyesuaian sesuaikan tanggal pembukuan minimum sama dengan tanggal perolehan'])
    writeToCsv.writerow(['IDPemda', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 8-------
Permasalahan 8 yaitu Tanggal Pembukuan lebih kecil dari tanggal perolehan""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBAR a LEFT JOIN Ta_KIB_A b ON a.IDPemda = b.IDPemda WHERE a.Kd_KA != b.Kd_KA
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBBR a LEFT JOIN Ta_KIB_B b ON a.IDPemda = b.IDPemda WHERE a.Kd_KA != b.Kd_KA
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBCR a LEFT JOIN Ta_KIB_C b ON a.IDPemda = b.IDPemda WHERE a.Kd_KA != b.Kd_KA
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBDR a LEFT JOIN Ta_KIB_D b ON a.IDPemda = b.IDPemda WHERE a.Kd_KA != b.Kd_KA
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga
    FROM Ta_KIBER a LEFT JOIN Ta_KIB_E b ON a.IDPemda = b.IDPemda WHERE a.Kd_KA != b.Kd_KA
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 8 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 8: Solusi Penyesuaian Kode KA riwayat mengikuti kode KA induk'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga'])
    writeToCsv.writerows(sourceRow)

print("""
------Memeriksa Permasalahan 9-------
Permasalahan 9 yaitu Terdapat data Tanggal Pembukuan, Tanggal Perolehan, dan Masa Manfaat yang tidak diisi baik pada KIB Induk maupun KIB Riwayat""")

sourceCursor = sourceConn.cursor()
sourceCursor.execute("""
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, 0 AS Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' WHEN b.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Induk NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' WHEN b.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Induk NULL' END AS Ket_Pembukuan,
    NULL AS Ket_MM
    FROM Ta_KIBAR a LEFT JOIN Ta_KIB_A b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR b.Tgl_Perolehan IS NULL OR b.Tgl_Pembukuan IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, a.Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' WHEN b.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Induk NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' WHEN b.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Induk NULL' END AS Ket_Pembukuan,
    CASE WHEN a.Masa_Manfaat IS NULL THEN 'MM Riwayat NULL' WHEN b.Masa_Manfaat IS NULL THEN 'MM Induk NULL' END AS Ket_MM
    FROM Ta_KIBBR a LEFT JOIN Ta_KIB_B b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR b.Tgl_Perolehan IS NULL OR b.Tgl_Pembukuan IS NULL OR a.Masa_Manfaat IS NULL OR b.Masa_Manfaat IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, a.Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' WHEN b.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Induk NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' WHEN b.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Induk NULL' END AS Ket_Pembukuan,
    CASE WHEN a.Masa_Manfaat IS NULL THEN 'MM Riwayat NULL' WHEN b.Masa_Manfaat IS NULL THEN 'MM Induk NULL' END AS Ket_MM
    FROM Ta_KIBCR a LEFT JOIN Ta_KIB_C b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR b.Tgl_Perolehan IS NULL OR b.Tgl_Pembukuan IS NULL OR a.Masa_Manfaat IS NULL OR b.Masa_Manfaat IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, a.Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' WHEN b.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Induk NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' WHEN b.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Induk NULL' END AS Ket_Pembukuan,
    CASE WHEN a.Masa_Manfaat IS NULL THEN 'MM Riwayat NULL' WHEN b.Masa_Manfaat IS NULL THEN 'MM Induk NULL' END AS Ket_MM
    FROM Ta_KIBDR a LEFT JOIN Ta_KIB_D b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR b.Tgl_Perolehan IS NULL OR b.Tgl_Pembukuan IS NULL OR a.Masa_Manfaat IS NULL OR b.Masa_Manfaat IS NULL
    UNION ALL
    SELECT a.IDPemda, a.Kd_Id, a.No_Urut, a.Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, a.Tgl_Dokumen, a.No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, a.Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' WHEN b.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Induk NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' WHEN b.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Induk NULL' END AS Ket_Pembukuan,
    CASE WHEN a.Masa_Manfaat IS NULL THEN 'MM Riwayat NULL' WHEN b.Masa_Manfaat IS NULL THEN 'MM Induk NULL' END AS Ket_MM
    FROM Ta_KIBER a LEFT JOIN Ta_KIB_E b ON a.IDPemda = b.IDPemda WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR b.Tgl_Perolehan IS NULL OR b.Tgl_Pembukuan IS NULL OR a.Masa_Manfaat IS NULL OR b.Masa_Manfaat IS NULL
    UNION ALL
    SELECT a.IDPemda, 0 AS Kd_Id, 0 AS No_Urut, 0 AS Kd_Riwayat, a.Kd_Prov, a.Kd_Kab_Kota, a.Kd_Bidang, a.Kd_Unit, a.Kd_Sub, a.Kd_UPB, a.Kd_Aset1, a.Kd_Aset2, a.Kd_Aset3, a.Kd_Aset4, a.Kd_Aset5, a.No_Register, a.Kd_Pemilik, NULL AS Tgl_Dokumen, NULL AS No_Dokumen, a.Tgl_perolehan, a.Tgl_Pembukuan, a.harga, a.Masa_Manfaat,
    CASE WHEN a.Tgl_Perolehan IS NULL THEN 'Tgl_Perolehan Riwayat NULL' END AS Ket_Perolehan,
    CASE WHEN a.Tgl_Pembukuan IS NULL THEN 'Tgl_Pembukuan Riwayat NULL' END AS Ket_Pembukuan,
    CASE WHEN a.Masa_Manfaat IS NULL THEN 'MM Riwayat NULL' END AS Ket_MM
    FROM Ta_Lainnya a WHERE a.Tgl_Perolehan IS NULL OR a.Tgl_Pembukuan IS NULL OR a.Masa_Manfaat IS NULL
""")
sourceRow = sourceCursor.fetchall()
result = sourceCursor.rowcount
if (result == 0) :
    print("------Permasalahan 9 Passed-------")
else:
    print("------Terdapat ",result," permasalahan-------")
    writeToCsv.writerow(['Permasalahan 9: Solusi isi data Tanggal Pembukuan, Tanggal Perolehan, dan Masa Manfaat sesuai dengan yang seharusnya terisi'])
    writeToCsv.writerow(['IDPemda', 'Kd_Id', 'No_Urut', 'Kd_Riwayat', 'Kd_Prov', 'Kd_Kab_Kota', 'Kd_Bidang', 'Kd_Unit', 'Kd_Sub', 'Kd_UPB', 'Kd_Aset1', 'Kd_Aset2', 'Kd_Aset3', 'Kd_Aset4', 'Kd_Aset5', 'No_Register', 'Kd_Pemilik', 'Tgl_Dokumen', 'No_Dokumen', 'Tgl_perolehan', 'Tgl_Pembukuan', 'harga', 'Masa_Manfaat', 'Ket_Perolehan', 'Ket_Pembukuan', 'Ket_MM'])
    writeToCsv.writerows(sourceRow)


done = True

print("Setup selesai, anda dapat menutup aplikasi ini. Terimakasih telah menggunakan aplikasi ini. Silakan buka file output.csv untuk melihat file-file bermasalah dan solusinya.")

status = False
# t = threading.Thread(target = selesai)
# t.start()