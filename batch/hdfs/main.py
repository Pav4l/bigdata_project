import csv
import random
import os

filename = "big_file.csv"
target_size_mb = 500
target_size_bytes = target_size_mb * 1024 * 1024

with open(filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["id", "value"])

    i = 0
    while True:
        writer.writerow([i, random.randint(100000, 999999)])
        i += 1

        if i % 10000 == 0:
            size = os.path.getsize(filename)
            if size >= target_size_bytes:
                print(f"Файл достиг {size / (1024 * 1024):.2f} МБ после {i} строк.")
                break

print("Файл big_file.csv успешно создан.")
