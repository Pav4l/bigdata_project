from clickhouse_connect import get_client
from datetime import date
import time
import smtplib
from email.mime.text import MIMEText


target_table = 'customer.sales'
today = date.today()
recipient_email = 'pashtet_7@mail.ru'
smtp_host = 'smtp.mail.ru'
smtp_port = 587
smtp_user = 'pashtet_7@mail.ru'
smtp_token = 'secret'


client = get_client(host='localhost', port=8123, username='user', password='strongpassword')
data_found = False

for attempt in range(1, 5):
    print(f"\n🔄 Попытка {attempt} из 4")

    result = client.query(f"""
        SELECT count() FROM customer.imports
        WHERE table_name = '{target_table}'
          AND last_import_date = toDate('{today}')
    """).result_rows[0][0]

    if result > 0:
        print(f"✅ Данные за {today} в {target_table} присутствуют.")
        data_found = True
        break
    else:
        print(f"❌ Данных за {today} в {target_table} нет.")
        if attempt < 4:
            print("⏳ Ждём 1 час до следующей попытки...")
            time.sleep(1)  # ждать 1 час

if not data_found:
    subject = f"[ALERT] Нет данных в {target_table} за {today}"
    body = f"Данных в таблице {target_table} за {today} так и не появилось после 4 попыток проверки."

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient_email

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_token)
            server.send_message(msg)
        print(f"📧 Уведомление отправлено на {recipient_email}")
    except Exception as e:
        print(f"❌ Ошибка при отправке письма: {e}")
