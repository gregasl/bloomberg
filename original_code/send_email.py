import win32com.client

TO_ADDRESS = "michael.farren@aslcap.com" # 'guocheng.xia@aslcap.com'

def email(error_type, error_number):
    olMailItem = 0x0
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = error_type
    newMail.Body = 'Hello,\n\nThere is an error. Error code: ' + str(error_number)
    #newMail.To = "michael.farren@aslcap.com" # "tech@aslcap.com"
    newMail.To = "tech@aslcap.com"
    newMail.Send()
    del newMail
    del obj




