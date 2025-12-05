import requests
import pandas as pd
import time
import sys
import os
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("="*60)
print("   ULTIMATE MINER: SEKOLAH & KAMPUS (AUTO-RESUME)")
print("="*60)

print("\nApa yang ingin Anda tambang hari ini?")
print(" [1] Data SEKOLAH (Input: npsn_sekolah.csv)")
print(" [2] Data KAMPUS  (Input: list_npsn_kampus.csv)")
mode = input(">> Masukkan angka (1 atau 2): ")

if mode == '1':
    input_file  = 'npsn_sekolah.csv'      
    output_file = 'HASIL_DATA_SEKOLAH_MENTAH.csv'
    kolom_npsn_input = 'npsn'
    print("\n[MODE: SEKOLAH] Target: 200rb++ data.")
    
elif mode == '2':
    input_file  = 'npsn_kampus.csv'  
    output_file = 'HASIL_DATA_KAMPUS_MENTAH.csv'
    kolom_npsn_input = 'npsn'
    print("\n[MODE: KAMPUS] Target: 4rb++ data.")

else:
    print("[ERROR] Pilihan tidak valid.")
    sys.exit()

base_url = "https://referensi.data.kemendikdasmen.go.id/pendidikan/npsn/"

session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
})

print(f"[-] Membaca daftar antrean: {input_file}...")
try:
    df_input = pd.read_csv(input_file, dtype=str, on_bad_lines='skip')
    
    if kolom_npsn_input in df_input.columns:
        all_npsn = df_input[kolom_npsn_input].tolist()
    else:
        all_npsn = df_input.iloc[:, 0].tolist() 
        
    print(f"    >> Total Antrean: {len(all_npsn):,} target.")

except FileNotFoundError:
    print(f"[ERROR] File '{input_file}' tidak ditemukan.")
    print("        Pastikan Anda sudah punya file bibitnya.")
    sys.exit()

processed_npsn = set()

if os.path.exists(output_file):
    print(f"[-] Mendeteksi file hasil lama: {output_file}")
    try:
        df_existing = pd.read_csv(output_file, dtype=str, on_bad_lines='skip')
        if 'npsn' in df_existing.columns:
            processed_npsn = set(df_existing['npsn'].tolist())
            print(f"    >> Ditemukan {len(processed_npsn):,} data yang SUDAH selesai.")
            print("    >> Melanjutkan sisanya...")
    except:
        print("    >> File output rusak/kosong, mulai dari 0.")

antrean_final = []
for n in all_npsn:
    n_bersih = str(n).split(';')[0].split(',')[0].strip()
    if n_bersih not in processed_npsn and len(n_bersih) > 4:
        antrean_final.append(n_bersih)

total_job = len(antrean_final)
print(f"[-] SISA TARGET BARU: {total_job:,}")

if total_job == 0:
    print("[SELESAI] Semua data sudah diambil! Tidak ada tugas tersisa.")
    sys.exit()

hasil_buffer = [] 
counter = 0

print("[-] Memulai Mining... (Tekan Ctrl+C untuk Pause)")

try:
    for npsn in antrean_final:
        counter += 1
        
        try:
            url_target = f"{base_url}{npsn}"
            resp = session.get(url_target, timeout=25, verify=False)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                item_data = {
                    'npsn': npsn,
                    'nama_sekolah': 'Tidak Ditemukan', 
                    'provinsi': '',
                    'jenjang': '', 
                    'bentuk_pendidikan': '', 
                    'status': '',
                    'alamat': '',
                    'nomor_telepon': '',
                    'email': '',
                    'website': ''
                }
                
                rows = soup.find_all('tr')
                found = False
                
                for row in rows:
                    text_row = row.get_text(separator=":", strip=True)
                    if ":" in text_row:
                        parts = text_row.split(":", 1)
                        if len(parts) >= 2:
                            label = parts[0].strip().lower()
                            isi = parts[1].strip()
                            
                            if label == 'nama':
                                item_data['nama_sekolah'] = isi
                                found = True
                            elif 'propinsi' in label or 'provinsi' in label:
                                item_data['provinsi'] = isi
                            elif 'bentuk pendidikan' in label: 
                                item_data['bentuk_pendidikan'] = isi
                                item_data['jenjang'] = isi 
                            elif 'status sekolah' in label or 'status' == label:
                                item_data['status'] = isi
                            elif 'alamat' in label:
                                item_data['alamat'] = isi
                            elif 'telepon' in label:
                                item_data['nomor_telepon'] = isi
                            elif 'email' in label:
                                item_data['email'] = isi
                            elif 'website' in label:
                                item_data['website'] = isi
                
                if found and item_data['nama_sekolah'] != 'Tidak Ditemukan':
                    hasil_buffer.append(item_data)
                    print(f"[{counter}/{total_job}] OK: {item_data['nama_sekolah']}")
                else:
                    print(f"[{counter}/{total_job}] SKIP: Data kosong ({npsn})")

            elif resp.status_code == 404:
                print(f"[{counter}/{total_job}] 404: NPSN Salah ({npsn})")
            else:
                print(f"[{counter}/{total_job}] ERR: {resp.status_code}")
                
        except Exception as e:
            print(f"[{counter}/{total_job}] ERROR: {e}")

        time.sleep(0.8)

        if len(hasil_buffer) >= 20: 
            df_chunk = pd.DataFrame(hasil_buffer)
            
            write_header = not os.path.exists(output_file)
            df_chunk.to_csv(output_file, mode='a', header=write_header, index=False)
            
            print(f"    >> {len(hasil_buffer)} data disimpan ke {output_file}...")
            hasil_buffer = [] 
except KeyboardInterrupt:
    print("\n[PAUSED] Berhenti sementara. Data di buffer akan disimpan.")

if len(hasil_buffer) > 0:
    df_chunk = pd.DataFrame(hasil_buffer)
    write_header = not os.path.exists(output_file)
    df_chunk.to_csv(output_file, mode='a', header=write_header, index=False)
    print(f"[SELESAI] Sisa {len(hasil_buffer)} data tersimpan.")

print(f"Total sesi ini: {counter} data diproses.")