import subprocess

hdfs_path = "/user/hdfs/testfile/big_file.csv"
local_path_in_container = "/tmp/big_file.csv"
local_output_path = "C:/novodata/github/bigdata_project/batch/hdfs/big_file_host.csv"
# Удаляем старый файл в контейнере (если существует)
subprocess.run([
    "docker", "exec", "hdfs-datanode1-1", "rm", "-f", local_path_in_container
], check=True)

# Копируем файл из HDFS внутрь контейнера
subprocess.run([
    "docker", "exec", "hdfs-datanode1-1", "hdfs", "dfs", "-get", hdfs_path, local_path_in_container
], check=True)

# Копируем файл из контейнера на хост
subprocess.run([
    "docker", "cp", f"hdfs-datanode1-1:{local_path_in_container}", local_output_path
], check=True)

# Читаем файл
with open(local_output_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        print(line.strip())
        if i >= 10:
            break

# Подтверждение
# if os.path.exists(local_output_path):
#     print(f" Файл успешно скопирован в: {local_output_path}")
