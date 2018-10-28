import fdb
import imapy
from imapy.query_builder import Q
from datetime import datetime
import sys, os 

def get_all_unseen(box):
    q = Q()
    emails = box.folder('INBOX').emails(
        q.subject('Re: ').unseen()
    )
    return emails

def write_error(con, email, message, logger):
    if "date" not in email:
        return
    date_received = datetime.strptime(email["date"], "%a, %d %b %Y %H:%M:%S %z")
    date_read = datetime.now()
    log_info = message
    email_from = email["from_whom"]
    email_from_adress = email["from_email"]
    theme = email["subject"]
    body = email["text"]
    cursor = con.cursor()
    cursor.execute("insert into EMAIL_LOG (time_received, time_read, log_info, email_from, email_from_address, theme, body) \
                   values (?, ?, ?, ?, ?, ?, ?)", (date_received, date_read, log_info, email_from, email_from_adress, theme, body))
    con.commit()
    print("Error:",  message, file=logger)

def read_configuration(filepath):
    conf = {}
    with open(filepath) as f:
        lines = f.read().splitlines()
        lines = list(filter(lambda x: x.strip()[0] != "#", lines))
        lines = list(map(lambda l: map(lambda x: x.strip(), l.split("=")), lines))
        for line in lines:
            k, v = line
            conf[k] = v
    return conf

def parse_emails(con, box, config, logger):
    unseen = get_all_unseen(box)
    for email in unseen:
        email.mark("Seen")
        try:
            text = email["text"][0]["text"]
        except:
            write_error(con, email, "Can't get text from mail body", logger)
        email["text"] = text
        status = text.strip()[0]
        start_info, end_info = text.find(config["start_code_sequence"]), text.rfind(config["end_code_sequence"])
        if (start_info == -1 or end_info == -1):
            write_error(con, email, "Invalid protocol: start or end code sequence is missing", logger)
            continue
        codes = text[start_info+len(config["start_code_sequence"]):end_info]
        codes = codes.strip().split()
        if (status.isdigit() and int(status) in [0,1]):
            is_code_valid = list(map(str.isdigit, filter(lambda x: len(x)!=0, codes)))
            is_code_valid = (len(is_code_valid) > 0) and all(is_code_valid)
            if is_code_valid:
                codes = list(map(int, codes))
                cur = con.cursor()
                for code in codes:
                    result = cur.execute("EXECUTE PROCEDURE EMAIL_READING (?, ?)", (int(code), int(status)))
                    con.commit()
                print("SUCCESS", file=logger)
            else:
                write_error(con, email, "Invalid SCH_CODES codes in mail body", logger)
        else:
            write_error(con, email, "Invalid status in mail body", logger)
    if (len(unseen) == 0):
        print("No new emails to read.", file=logger)
        
def main():
    log_name = "logfile.log"
    logger = open(log_name, "w")    
    args = sys.argv
    if len(args) != 2:
        print("Invalid number of arguments", file=logger)
        return
    
    conf = args[1]
    if (not os.path.isfile(conf)):
        print("Argument is not a valid file path", file=logger)
        return
    
    try:
        conf = read_configuration(conf)
    except:
        print("Invalid configuration file", file=logger)
        return

    con, box = None, None
    try:
        con = fdb.connect(dsn=conf["db_location"], user=conf["db_lgn"], password=conf["db_pwd"]) 
    except Exception as e:
        print(str(e), file=logger)
        print("Can't establish database connection", file=logger)
        return

    try:
        box = imapy.connect(host=conf["imap_location"], username=conf["imap_lgn"], password=conf["imap_pwd"], ssl=True)
    except Exception as e:
        print(str(e), file=logger)
        print("Can't establish connection to imap server", file=logger)
        return    
    
    try:
        parse_emails(con, box, conf, logger)
        box.logout()
        con.close()
    except Exception as e:
        print(str(e), file=logger)
        print("Something unknown and invalid happened. Sorry.", file=logger)
    logger.close()
main()

