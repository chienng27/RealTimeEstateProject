from playwright.async_api import async_playwright
import asyncio
import json
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import re

pattern_price = r"(\d+(\.\d+)?)"
URL_base = 'https://bds68.com.vn/'
Location = 'Hồ Chí Minh'
pattern = r',\s*([^,]+),\s*[^,]+$'
N = 10000

def extract_location(text):
    match = re.search(pattern, text)
    if match:
        result = match.group(1)
        return result
    return "Không tìm thấy thông tin"

def change_page_number(url, num):
    base_url, query = url.split('?')
    new_query = f'pg={num}'
    new_url = f'{base_url}?{new_query}'
    return new_url

def extract_picture(soup_temp):
    pictures = []
    for img in soup_temp.find_all('img', class_='swiper-lazy swiper-lazy-loaded'):
        if img.get('src'):
            pictures.append(img['src'])
    
    for img in soup_temp.find_all('img', class_='swiper-lazy'):
        if img.get('data-src'):
            pictures.append(img['data-src'])
    return pictures

async def run(playwright, producer):
    print('Launching browser...')
    count = 0
    num = 60
    browser = await playwright.chromium.launch(headless=True)
    try:
        page = await browser.new_page()
        print('Navigating to bds68.com.vn...')
        await page.goto(URL_base)
        print("Connected to URL")

        # Click to "Thuê"
        span = page.locator("span[class='slider']")
        await span.click()
        print("Đã click cho thuê")
        
        # Select location 
        filter_1 = page.locator('span[id="spProvHeader"]') 
        await filter_1.click()
        print('Đã click vào filter')

        list_box = page.locator('ul[id="dropdown_location"]')
        ho_chi_minh_item = list_box.locator('li', has_text=Location)
        await ho_chi_minh_item.click()
        print(f'{Location} Clicked')

        enter = page.locator("input[class='search-btn w-100']")
        await enter.click()
        print("Enter Clicked")
        await page.wait_for_load_state("load")
        await page.wait_for_timeout(500)
        page_link = page.url + '?pg=1'# Lấy URL hiện tại
        print(f'Initial page URL: {page_link}')
        content = await page.inner_html('div[id="ser-grid-container"]')
        soup = BeautifulSoup(content, "html.parser")

        while count < N:
            for div in soup.find_all("div", class_="prop-box-item-contain Nope"):
                data = {}
                prop_items = div.find_all('span', class_='prop-item-info')
                if len(prop_items) >= 2:
                    match = re.search(pattern_price, prop_items[0].text.strip())
                    if match:
                        price = float(match.group(1))
                    else:
                        price = 0
                    area = float(prop_items[1].text.replace("m²","").strip())
                else:
                    print("Cannot find information")
                    continue

                address = div.find('p').text.strip()
                ward = extract_location(address)

                link = URL_base + div.find('a')['href']
                
                category = div.find('div', class_='prop-grid-category')
                cate = category.find('span').text.strip() if category else 'None'
                
                moreinfo = div.find('div', class_='prop-grid-moreinfo')
                lenmif = moreinfo.find_all('span')
                bed = bath = 0
                if len(lenmif) == 2:
                    bed = lenmif[0].text.strip()
                    bath = lenmif[1].text.strip()
                elif len(lenmif) == 1:
                    if moreinfo.find('i', class_='fa fa-bed'):
                        bed = lenmif[0].text.strip()
                    else:
                        bath = lenmif[0].text.strip()

                try:
                    await page.goto(link)
                    await page.wait_for_load_state('load')
                    content_temp = await page.inner_html('div[class="sildePhoto"]')
                    soup_temp = BeautifulSoup(content_temp, "html.parser")
                    pic = extract_picture(soup_temp)
                except Exception as e:
                    print(f"Error navigating to {link}: {e}")
                    continue

                data = {
                    "ward" : ward,
                    "price": price,
                    "area": area,
                    "address": address,
                    "category": cate,
                    "bed": bed,
                    "bath": bath,
                    "link": link,
                    "src_pic": pic
                }

                # Serialize data and send to Kafka
                try:
                    producer.send('real-estate-data', value=json.dumps(data).encode('utf-8'))
                    print(f"Sent data to Kafka: {data}")
                except Exception as e:
                    print(f"Error sending data to Kafka: {e}")
                count += 1
                
                await asyncio.sleep(2)

            if count < N:
                num += 1
                page_link = change_page_number(page_link, num)
                print(f'Navigating to page URL: {page_link}')
                await page.goto(page_link)
                await page.wait_for_load_state("load")
                content = await page.inner_html('div[id="ser-grid-container"]')
                soup = BeautifulSoup(content, "html.parser")

    finally:
        await browser.close()

async def main():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    async with async_playwright() as playwright:
        await run(playwright, producer)

if __name__ == '__main__':
    asyncio.run(main())



