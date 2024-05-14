import requests
from bs4 import BeautifulSoup

response = requests.get("https://bds.com.vn/mua-ban-nha-dat-ha-noi-page2000")
soup = BeautifulSoup(response.content, "html.parser")
list_link = soup.findAll("div", "item-nhadat")
link = list_link[1].find("a", "title-item-nhadat")
print(link.get("href"))

LIST_LINK_PRODUCT = []

for i in range(2000):
    try:
        response = requests.get("https://bds.com.vn/mua-ban-nha-dat-ha-noi-page" + str(i + 1))
        soup = BeautifulSoup(response.content, "html.parser")
        list_link = soup.findAll('div', 'item-nhadat')          
        for item in list_link:
            link = item.find('a', 'title-item-nhadat').get('href')
            LIST_LINK_PRODUCT.append(
                {
                    'link': link
                }
            )
        print('####Done page' + str(i + 1))
    except Exception as e:
        print('####Error get link ' + str(i + 1))
        print(e)