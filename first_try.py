import subprocess
import logging
import sys
import argparse
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def call_process(command):
    try:
        proc= subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        logger.error(stdout,stderr)
        ret = proc.returncode
        print("Exit code of subprocess" + ret)
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

def check_s3_files():
    project = 'twitch'
    timeperiod = args.time_period  # format 20200218_01
    s3bucket = 'comscore-endpoint-production'
    s3prefix = args.prefix  # format 2020 / 02 / 18 / 01
    minCount = '800'
    print("Checking the twitch data for {0} ".format(args.time_period))
    command = "\\\\csiadtpl02\\C$\\cs_bin\\TwitchCustomScanner\\TwitchCustomScanner.exe"+" "+"--project"+" "+project +" "+"--timeperiod"+" "+timeperiod+" "+"--s3bucket"+" "+s3bucket+" "+"--s3prefix"+" "+s3prefix+" "+"--minCount"+" "+minCount
    return call_process(command)

def main():
    try:
        logger.info("Checking the twitch data for ")
        success = 1
        time_out = time.time() + 300 # no for 2 days 172800 time in seconds
        while True:
            if success == 0:
                logger.info("All the files are available for time period")
                break
            elif time.time() > time_out:
                break
            success = check_s3_files()
            logger.info("Sleeping for the 3 Hours")
            time.sleep(50) # 10800 sleep time for 3 hours
        if success !=0:
            send_mail('gswami@comscore.com',
                      'gswami@comscore.com',
                      'Twitch data is missing for {0}'.format(args.time_period),
                      'Twitch data is missing for {0} since two days is not arrived'.format(args.time_period),
                      'smtp.office.comscore.com')
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        sys.exit(success)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--time_period', required=True, help='Status to check')
    parser.add_argument('--prefix', required=True, help='Start date')
    parser.add_argument('--mail', required=True, help='receivers', nargs='+', default=None)
    parser.add_argument('--log_level', help='Logging level.', required=False, default="INFO")


    try:
        args = parser.parse_args()
    except Exception as e:
        print(e)


    logging.basicConfig(
        stream=sys.stdout,
        format="%(levelname) -5s %(asctime)s %(module)s:%(lineno)s - %(message)s",
        level=logging.getLevelName(args.log_level),
    )
    global logger
    logger = logging.getLogger(__name__)

    for handler in logging.root.handlers:
        handler.addFilter(logging.Filter('__main__'))

    main()
