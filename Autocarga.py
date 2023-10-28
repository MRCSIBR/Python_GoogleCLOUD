import schedule
import time
import subprocess

def job():
    print("Task is running...")
    # Run the other Python script as a subprocess
    subprocess.run(["python3", "GCP_2.py"])

# Correr el script cada dia a 10:30 AM
schedule.every().day.at("10:30").do(job)

# Correr el script cada minuto
#schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

# Para correr en segundo plano: python3 autocarga.py &
