import requests

URL_SCELETON = 'http://app.thefacesoffacebook.com/php/select_many_fbid.php?'

def main():
    header = '0 fbusers2 99162169'
    data = {'data':header}
    response = requests.post(URL_SCELETON, data=data)
    htmlTextApproved = response.text
    splitted = htmlTextApproved.split(' ')

if __name__ == "__main__":
    main()