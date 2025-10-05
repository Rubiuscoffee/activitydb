# mailer.py
import os, smtplib, mysql.connector
from email.mime.text import MIMEText
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # opcional si usas .env
DB_HOST = os.getenv("DB_HOST","127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT","3306"))
DB_USER = os.getenv("DB_USERNAME","root")
DB_PASS = os.getenv("DB_PASSWORD","")
DB_NAME = os.getenv("DB_DATABASE","hotel_reservas")
DB_CA = os.getenv("DB_CA")

SMTP_HOST = os.getenv("SMTP_HOST","smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT","587"))
SMTP_USER = os.getenv("SMTP_USER","tu_correo@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS","tu_password_app")  # usa App Password

def send_mail(to_email, subject, body):
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = SMTP_USER
    msg["To"] = to_email
    msg["Subject"] = subject
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

# Configurar SSL si se proporciona el certificado CA
ssl_config = {}
if DB_CA:
    ssl_config = {
        'ssl_ca': DB_CA,
        'ssl_verify_cert': True,
        'ssl_verify_identity': False
    }

cnx = mysql.connector.connect(
    host=DB_HOST, 
    port=DB_PORT,
    user=DB_USER, 
    password=DB_PASS, 
    database=DB_NAME,
    ssl_disabled=False if DB_CA else True,
    **ssl_config
)
cnx.autocommit = False
cur = cnx.cursor(dictionary=True)

cur.execute("SELECT * FROM email_outbox WHERE status='PENDING' ORDER BY id LIMIT 50 FOR UPDATE")
rows = cur.fetchall()
for row in rows:
    try:
        send_mail(row["to_email"], row["subject"], row["body"])
        cur.execute("UPDATE email_outbox SET status='SENT', sent_at=%s, last_error=NULL WHERE id=%s",
                    (datetime.utcnow(), row["id"]))
        print(f"[OK] Sent email_outbox.id={row['id']}")
    except Exception as e:
        cur.execute("UPDATE email_outbox SET status='ERROR', last_error=%s WHERE id=%s",
                    (str(e), row["id"]))
        print(f"[ERR] email_outbox.id={row['id']} -> {e}")
cnx.commit()
cur.close(); cnx.close()
