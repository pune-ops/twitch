import subprocess
import logging
import os
import sys
import argparse
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def call_process(command):
    try:
        print(command)
        proc= subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        print(stdout,stderr)
        ret = proc.returncode
        print(ret)
        return ret
    except Exception as e:
        print(e)


def send_mail(send_from, send_to, subject, text, server="localhost"):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def check_s3_files(timeperiod):
    project = 'twitch'
    s3bucket = 'comscore-endpoint-production'
    s3prefix = timeperiod[0:4] + "/" + timeperiod[4:6] + "/" + timeperiod[6:8] + "/" + timeperiod[9:11]
    minCount = '800'
    print("Checking the twitch data for {0} ".format(args.time_period))
    command = "\\\\csiadtpl02\\C$\\cs_bin\\TwitchCustomScanner\\TwitchCustomScanner.exe"+" "+"--project"+" "+project +" "+"--timeperiod"+" "+timeperiod+" "+"--s3bucket"+" "+s3bucket+" "+"--s3prefix"+" "+s3prefix+" "+"--minCount"+" "+minCount
    return call_process(command)


def update_file(list):
    with open("\\\\csiadtpl02\\d$\\ganesh\\twitch.txt", "w") as file:
        for i in list:
            print("updating file for the hour".format(i.rstrip()) +" "+ i)
            file.write(i + "\n")

def main():
    try:
        success = 1
        while True:
            with open("\\\\csiadtpl02\\d$\\ganesh\\twitch.txt","r+") as file:
                hours_list = [args.time_period]
                print(hours_list)
                content = file.readlines()
                for i in content:
                    success = check_s3_files(i)
                    if success == 0:
                        print("Twitch data for the hour {} is available completely".format(i.rstrip()))
                        if not os.path.exists("\\\\csiadtpl02\\d$\\ganesh\\twitch\\{}.txt".format(i)):
                            with open("\\\\csiadtpl02\\d$\\ganesh\\twitch\\{}.txt".format(i.rstrip()),'w'): pass
                    else:
                        print("Hour file is not available adding for next scan" + i)
                        hours_list.append(i.rstrip())
                    time.sleep(50)
                hours_list = set(hours_list)
                update_file(hours_list)
                print(success)
                break

    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        sys.exit(success)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--time_period', required=True, help='Status to check')
    #parser.add_argument('--prefix', required=False, help='Start date')
    parser.add_argument('--mail', required=True, help='receivers', nargs='+', default=None)
    parser.add_argument('--log_level', help='Logging level.', required=False, default="INFO")

    try:
        args = parser.parse_args()
        main()
    except Exception as e:
        print(e)

