import asyncio
import ssl
import random
import aiohttp
from pydantic import BaseModel
from faker import Faker
from parsel import Selector
from typing import Optional, List, AsyncIterator, Any
from tenacity import retry, wait_fixed, stop_after_attempt
import datetime
import requests
from asyncer import asyncify

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "core"))

from core.engine import AioEngine

fake = Faker()


class MonitoringInput(BaseModel):
    Quantity: Optional[int] = 0   # 数量
    MinPrice: Optional[float] = 0.0  # 产品单价
    ProductName: Optional[str] = ""  # 产品名称
    Platform: Optional[str] = ""  # 所属平台
    ShipsFromCountryName: Optional[str] = ""  # 发货地
    DaysUntilDispatch: Optional[str] = ""  # 发货时间


class MonitoringInsertFormData(BaseModel):
    OrderTotal: Optional[float] = 0.0   # 总额
    PayUrl: Optional[str] = ""   # 产品链接
    MonitoringInputs: Optional[List[MonitoringInput]] = []
    Remark: Optional[str] = ""  # 备注信息


class Monitor(AioEngine):
    platform = "Element14"

    async def get_proxy(self) -> str:
        """
        获取代理
        :return:
        """

        url = "http://192.168.1.165:2333/get_proxy?num=1&type=qg-dtdx"

        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:

                servers = await resp.json()
                return servers[0] if servers else None

    async def scmMonitoringProductList(self):
        """
        获取爬虫监控数据
        :return:
        """

        url = "https://scm.jotrin.cn:446/api/ScmMonitoringProduct/getList?platform=Element14&priority=0&priority=1"

        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp_json = await resp.json()
                if resp_json:
                    return resp_json
                else:
                    raise Exception(f"/api/ScmMonitoringProduct/getList 返回为None")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def scmMonitoringInsert(self, form_data: dict):
        """
        上传数据
        :param form_data:
        :return:
        """

        url = "https://scm.jotrin.cn:446/api/Monitoring/insert"
        # url = "http://192.168.1.10:8030/api/Monitoring/insert"

        headers = {
            "User-Agent": fake.user_agent()
        }

        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=form_data, headers=headers) as resp:
                resp_txt = await resp.text()

                if resp.status == 200:
                    self.logger.info(f"[监控]>> 上传成功 {form_data}\n{resp_txt}")
                    return True
                else:
                    self.logger.error(f"[监控]>> 上传失败 {resp_txt}")
                    return False

    def sync_api_search(self, url: str):
        """
        同步搜索接口，应为异步请求异常所以改为同步
        :param url:
        :return:
        """

        proxy_url = "http://192.168.1.165:2333/get_proxy?num=1&type=qg-dtdx"
        proxy_resp = requests.get(proxy_url).json()
        server = proxy_resp[0]
        proxies = {
            "http": server,
            "https": server,
        }

        headers = {
            "authority": "cn.element14.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }

        resp = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        return resp.text

    @retry(stop=stop_after_attempt(6), wait=wait_fixed(1))
    async def sync_to_async_api_search(self, url: str):
        """
        搜索接口
        :param url:
        :return:
        """

        html = await asyncify(self.sync_api_search)(url)
        return html


    @retry(stop=stop_after_attempt(6), wait=wait_fixed(1))
    async def api_search(self, url: str):
        """
        搜索接口
        :param url:
        :return:
        """


        headers = {
            "authority": "cn.element14.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }

        server = await self.get_proxy()

        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, proxy=server) as resp:
                resp_txt = await resp.text()
                return resp_txt

    async def publish_tasks(self) -> AsyncIterator:

        product_list = await self.scmMonitoringProductList()
        product_list = product_list["data"]

        for product_data in product_list:
            if self.platform not in product_data["platform"]:
                continue

            productName = product_data["productName"]
            # productName = "MCHC11F1CFNE4"  # 列表 停产
            # productName = "ADG439FBRZ"  # 有货 C8051F336-GMR MK64FN1M0VLL12

            url = f"https://cn.element14.com/search?st={productName}&gs=true"

            self.logger.info(f"[监控]>> {productName} -> {url}")
            yield [url, productName]
            # break   # *

    async def process(self, task_future: Any):

        [url, pn] = task_future

        # try:
        html = await self.sync_to_async_api_search(url)
        response = Selector(html)

        # 查看是否有搜索结果
        no_search_result = response.xpath("//table[@id='noSearchResults']//td[2]//text()").getall()
        if no_search_result:
            self.logger.warning(f"[监控]>> 未搜索到任何结果 {pn}")
            return

        skulinks = response.xpath(f"//tr[contains(@class, 'productRow')]/td/a[@title='{pn.upper()}']/@href").getall()
        if not skulinks:
            skulinks = response.xpath(
                f"//a[contains(@class, 'skulink') and contains(string(), '{pn.upper()}')]/@href").getall()

        if skulinks:  # 列表
            for skulink in skulinks:
                self.logger.info(f"[监控]>> {pn} -> {skulink}")
                yield self.yield_step(self.process, [skulink, pn])
        else:  # 详情

            if response.xpath("//th[contains(string(), '库存编号')]/following-sibling::td/text()").get():
                self.logger.warning(f"特殊页面，数据不全，重试 - {url}")
                yield self.yield_step(self.process, [url, pn])
                return

            # page_pn = response.xpath("//h1[@class='pdpMainPartNumber']/b/text()").get()
            page_pn = response.xpath("//dd[@class='ManufacturerPartNumber']/span/text()").get()
            if not page_pn or page_pn.strip() != pn.upper():
                print(html)
                self.logger.warning(f"无精准匹配型号 {page_pn} / {pn.upper()} - {url}")
                return

            # 库存编号
            platform_pn = response.xpath("//dd[@class='ManufacturerOrderCode']/span/text()").get()

            # 库存，是否有货
            stock = response.xpath("//span[@class='availTxtMsg' and contains(text(), '有货')]/text()").get()

            availability_list_strongs = response.xpath("//div[@class='availabilityList']/strong")

            l_pri = []
            trs = response.xpath("//table[@id='pdpTableProductDetailPrice']/tbody/tr")
            for tr in trs:
                td1 = tr.xpath("./td[1]/text()").get()
                if not td1:
                    continue
                td1 = td1.strip()

                td2_vatExcl = tr.xpath("./td[2]/span[contains(@class, 'vatExcl')]/text()").get()
                td2_vatExcl = td2_vatExcl.strip()
                td2_vatIncl = tr.xpath("./td[2]/span[contains(@class, 'vatIncl')]/text()").get()
                td2_vatIncl = td2_vatIncl.strip()

                l_pri.append([td1, [td2_vatExcl, td2_vatIncl]])

            #
            form_data = MonitoringInsertFormData()

            if not stock or len(l_pri) == 0:
                self.logger.warning(f"[监控]>> 无现货或其他原因，跳过监控 {pn}")
                return

            for availability_list_strong in availability_list_strongs:

                area_stock = availability_list_strong.xpath("./text()").get()
                area_stock = int(area_stock.replace(",", "").replace(" ", ""))

                area_desc = availability_list_strong.xpath("./following-sibling::text()[1]").get()
                if area_desc is None:
                    area_desc = ""
                area_desc = area_desc.strip()

                # 最低价 匹配
                min_price = 0
                for l_p_i in range(len(l_pri)):
                    n = int(l_pri[l_p_i][0].replace("+", ""))
                    pri = float(l_pri[l_p_i][1][1].replace("CNY", ""))

                    if l_p_i == 0:
                        min_price = pri
                        continue
                    elif area_stock >= n:
                        min_price = pri
                    elif area_stock < n:
                        break

                form_data.OrderTotal = round(area_stock * min_price, 2)

                form_data.PayUrl = url

                form_data.Remark = f"库存编号：{platform_pn}"

                ipt = MonitoringInput()

                ipt.MinPrice = min_price

                ipt.Quantity = area_stock  # 地区库存

                ipt.ProductName = pn

                ipt.Platform = "E络盟"

                area_desc = area_desc.split("，")

                ipt.ShipsFromCountryName = area_desc[0].strip().split()[0]

                ipt.DaysUntilDispatch = area_desc[1]

                form_data.MonitoringInputs = [ipt]

                # print(f"{form_data.dict()=}")
                # 上传
                await self.scmMonitoringInsert(form_data.model_dump())

        # except Exception as e:
        #     self.logger.error(f"[异常]>> {pn} - {e}")


if __name__ == "__main__":
    monitor = Monitor()
    monitor.run()








