import smtplib
from email.mime.text import MIMEText
#mailto_list=["262950629zjy@gmail.com"]
#mail_host="smtp.mail.yahoo.com"
#mail_user="zhu_john@yahoo.cn"
#mail_pass="3832165"
#mail_postfix="yahoo.cn"

def send_mail(mailto, sub, content):
	mailto_list = [mailto]
	mail_host = "smtp.163.com"
	mail_user = "sfi_china_web@163.com"
	mail_pass = "sfi-server"
	me = "<"+mail_user+">"
	msg = MIMEText(content,_subtype='plain',_charset='utf-8')
	msg['Subject'] = sub
	msg['From'] = me
	msg['To'] = ";".join(mailto_list)
	try:
		server = smtplib.SMTP()
		server.connect(mail_host,25)
		server.starttls()
		server.login(mail_user,mail_pass)
		server.sendmail(me,mailto_list,msg.as_string())
		server.close()
		return True
	except Exception, e:
		print str(e)
		return False
