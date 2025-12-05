import pandas as pd
import numpy as np
import sys
import re
import os

print("="*70)
print("   ULTIMATE PROCESSOR V2 (FIXED): SEKOLAH & KAMPUS")
print("   Fix: Mengatasi error kolom ganda (AttributeError)")
print("="*70)

print("\nPilih Mode Pengolahan Data:")
print(" [1] Data SEKOLAH (SD, SMP, SMA, SMK)")
print(" [2] Data PERGURUAN TINGGI (Kampus)")
mode = input(">> Masukkan angka (1 atau 2): ")

if mode == '1':
    input_file = 'hasil_data_sekolah_lengkap.csv' 
    
    f1_lengkap = '1_SEKOLAH_FULL_DATA.csv'       
    f2_bersih  = '2_SEKOLAH_BERSIH_UTAMA.csv'    
    f3_bali    = '3_SEKOLAH_BALI_BERSIH.csv'     
    f4_luar_sd = '4_LUAR_BALI_SD_BERSIH.csv'     
    f5_luar_smp= '5_LUAR_BALI_SMP_BERSIH.csv'    
    f6_luar_sma= '6_LUAR_BALI_SMA_SMK_BERSIH.csv'
    
    col_jenjang_asli = 'jenjang'
    col_jenjang_baru = 'jenjang'
    print("\n[MODE: SEKOLAH] Target: 6 File Output")

elif mode == '2':
    input_file = 'hasil_data_kampus_lengkap.csv' 
    
    f1_lengkap = '1_KAMPUS_FULL_DATA.csv'        
    f2_bersih  = '2_KAMPUS_BERSIH_UTAMA.csv'     
    f3_bali    = '3_KAMPUS_BALI_BERSIH.csv'      
    f4_luar    = '4_KAMPUS_LUAR_BALI_BERSIH.csv' 
    
    col_jenjang_asli = 'jenjang'
    col_jenjang_baru = 'bentuk_pendidikan'
    print("\n[MODE: KAMPUS] Target: 4 File Output")

else:
    print("[ERROR] Pilihan salah.")
    sys.exit()

def format_whatsapp(nomor):
    if pd.isna(nomor) or str(nomor).strip() in ['', '-', 'nan']: return np.nan
    bersih = re.sub(r'\D', '', str(nomor))
    if len(bersih) < 6: return np.nan 
    if bersih.startswith('0'): bersih = '62' + bersih[1:]
    elif bersih.startswith('8'): bersih = '62' + bersih
    return f"https://wa.me/{bersih}"

def validasi_email(email):
    if pd.isna(email): return np.nan
    s = str(email).strip()
    if len(s) < 5 or "@" not in s or s == '-' or ' ' in s: return np.nan
    return s

def sort_sekolah(teks):
    t = str(teks).upper()
    if 'SD' in t: return 'SD'
    if 'SMP' in t: return 'SMP'
    if 'SMA' in t: return 'SMA'
    if 'SMK' in t: return 'SMK'
    return 'LAINNYA'

def sort_kampus(teks):
    t = str(teks).upper()
    if 'AKADEMI KOMUNITAS' in t: return 'AKADEMI KOMUNITAS'
    if 'AKADEMI' in t: return 'AKADEMI'
    if 'POLITEKNIK' in t: return 'POLITEKNIK'
    if 'SEKOLAH TINGGI' in t: return 'SEKOLAH TINGGI'
    if 'INSTITUT' in t: return 'INSTITUT'
    if 'UNIVERSITAS' in t: return 'UNIVERSITAS'
    return 'LAINNYA'

print(f"[-] Membaca input: {input_file}...")
try:
    if not os.path.exists(input_file):
        alt_file = 'data_kampus_lengkap.csv'
        if mode == '2' and os.path.exists(alt_file):
             input_file = alt_file
             print(f"    [INFO] Mengalihkan ke file: {alt_file}")
        else:
            print(f"[ERROR] File '{input_file}' tidak ditemukan.")
            sys.exit()
            
    df = pd.read_csv(input_file, sep=None, engine='python', dtype=str, on_bad_lines='skip')
    
    df = df.loc[:, ~df.columns.duplicated()]
    
    print(f"    >> Total Data Mentah: {len(df):,} baris.")
except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit()

if col_jenjang_asli in df.columns and col_jenjang_asli != col_jenjang_baru:
    if col_jenjang_baru in df.columns:
        df.drop(columns=[col_jenjang_asli], inplace=True)
    else:
        df.rename(columns={col_jenjang_asli: col_jenjang_baru}, inplace=True)

print("\n[-] Tahap 1: Membersihkan Teks (Hapus :: dan PROV)...")
cols_text = ['nama_sekolah', 'provinsi', col_jenjang_baru]
for col in cols_text:
    if col in df.columns:
        df[col] = df[col].astype(str).str.upper().str.strip()
        df[col] = df[col].str.replace(':', '', regex=False).str.strip()

if 'email' in df.columns:
    df['email'] = df['email'].astype(str).str.replace(':', '', regex=False).str.strip().str.lower()

if 'provinsi' in df.columns:
    df['provinsi'] = df['provinsi'].str.replace(r'\s+', ' ', regex=True)
    kata_dibuang = ['PROPINSI.', 'PROPINSI', 'PROV.', 'PROV', 'DAERAH ISTIMEWA', 'DKI']
    for kata in kata_dibuang:
        df['provinsi'] = df['provinsi'].str.replace(kata, '', regex=False).str.strip()

print("\n[-] Tahap 2: Validasi Kontak...")
if 'nomor_telepon' in df.columns:
    df['nomor_telepon'] = df['nomor_telepon'].apply(format_whatsapp)
if 'email' in df.columns:
    df['email'] = df['email'].apply(validasi_email)

df_valid = df.dropna(subset=['nomor_telepon', 'email'], how='all').copy()

cols_fill = ['nomor_telepon', 'email']
for c in cols_fill:
    if c in df_valid.columns:
        df_valid[c] = df_valid[c].fillna('-')

print(f"    >> Sisa Data Valid: {len(df_valid):,} baris.")

print("\n[-] Tahap 3: Menyimpan FILE 1 (LENGKAP)...")
df_valid.to_csv(f1_lengkap, index=False)
print(f"    [OK] File 1 (Semua Kolom) : {f1_lengkap}")

print("\n[-] Tahap 4: Sortir & Pangkas Kolom...")

if col_jenjang_baru in df_valid.columns:
    if mode == '1': 
        df_valid['sort_key'] = df_valid[col_jenjang_baru].apply(sort_sekolah)
        urutan = ['SD', 'SMP', 'SMA', 'SMK', 'LAINNYA']
    else: 
        df_valid['sort_key'] = df_valid[col_jenjang_baru].apply(sort_kampus)
        urutan = ['AKADEMI KOMUNITAS', 'AKADEMI', 'POLITEKNIK', 'SEKOLAH TINGGI', 'INSTITUT', 'UNIVERSITAS', 'LAINNYA']
        
    df_valid['sort_key'] = pd.Categorical(df_valid['sort_key'], categories=urutan, ordered=True)
    df_valid = df_valid.sort_values(by=['sort_key', 'nama_sekolah'])

kolom_target = ['nama_sekolah', 'provinsi', col_jenjang_baru, 'nomor_telepon', 'email']
cols_final = [c for c in kolom_target if c in df_valid.columns]
df_final = df_valid[cols_final]

print("\n[-] Tahap 5: Menyimpan FILE 2 (BERSIH)...")
df_final.to_csv(f2_bersih, index=False)
print(f"    [OK] File 2 (5 Kolom)     : {f2_bersih}")

print("\n[-] Tahap 6: Memecah Wilayah...")

if 'provinsi' in df_final.columns:
    df_bali = df_final[df_final['provinsi'].str.contains('BALI', na=False)]
    df_bali.to_csv(f3_bali, index=False)
    print(f"    [OK] File 3 (Bali Only)   : {f3_bali} ({len(df_bali):,} data)")

    df_luar = df_final[~df_final['provinsi'].str.contains('BALI', na=False)]
    
    if mode == '1':
        df_sd = df_luar[df_luar[col_jenjang_baru].str.contains('SD', na=False)]
        df_sd.to_csv(f4_luar_sd, index=False)
        print(f"    [OK] File 4 (Luar SD)     : {f4_luar_sd} ({len(df_sd):,} data)")
        
        df_smp = df_luar[df_luar[col_jenjang_baru].str.contains('SMP', na=False)]
        df_smp.to_csv(f5_luar_smp, index=False)
        print(f"    [OK] File 5 (Luar SMP)    : {f5_luar_smp} ({len(df_smp):,} data)")
        
        df_sma = df_luar[df_luar[col_jenjang_baru].str.contains('SMA|SMK', na=False)]
        df_sma.to_csv(f6_luar_sma, index=False)
        print(f"    [OK] File 6 (Luar SMA/SMK): {f6_luar_sma} ({len(df_sma):,} data)")
        
    else:
        df_luar.to_csv(f4_luar, index=False)
        print(f"    [OK] File 4 (Luar All)    : {f4_luar} ({len(df_luar):,} data)")

else:
    print("[ERROR] Kolom provinsi tidak ditemukan.")

print("\n" + "="*70)
print("   SELESAI. Semua file tersimpan sesuai permintaan.")
print("="*70)