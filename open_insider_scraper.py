import os
import json
import time
import pandas as pd


# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)

import logging

# Suppress logging from Selenium and other related modules
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("http").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)


class OpenInsiderScraper:
    def __init__(self, debug: bool = True) -> None:
        self.debug = debug
        self.url = "http://openinsider.com/screener?s={}&o=&pl=&ph=&ll=&lh=&fd=0&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=1000&page=1"
        self.chrome_driver_path = self._get_chrome_driver_path()
        self.base_export_path = self._get_data_export_path()
        self.tickers_folder_path = f"{self.base_export_path}\\Tickers"
        os.makedirs(self.base_export_path, exist_ok=True)
        os.makedirs(self.tickers_folder_path, exist_ok=True)

        """ -- Chromedriver options -- """
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("--disable-gpu")

    def _get_data_export_path(self):
        try:
            internal_path = f"{os.getcwd()}\\config.json"
            with open(internal_path, "r") as file:
                data = json.load(file)
            return data["data_export_path"]
        except FileNotFoundError:
            external_path = f"{os.getcwd()}\\OpenInsiderScraper\\config.json"
            with open(external_path, "r") as file:
                data = json.load(file)
            return data["data_export_path"]

    """-----------------------------------"""
    """----------------------------------- Browser Operations -----------------------------------"""

    def _get_chrome_driver_path(self):
        try:
            internal_path = f"{os.getcwd()}\\config.json"
            with open(internal_path, "r") as file:
                data = json.load(file)
            return data["chrome_driver_path"]
        except FileNotFoundError:
            external_path = f"{os.getcwd()}\\OpenInsiderScraper\\config.json"
            with open(external_path, "r") as file:
                data = json.load(file)
            return data["chrome_driver_path"]

    def _create_browser(self, url=None):
        """
        :param url: The website to visit.
        :return: None
        """
        service = Service(executable_path=self.chrome_driver_path)
        self.browser = webdriver.Chrome(service=service, options=self.chrome_options)
        # Default browser route
        if url == None:
            self.browser.get(url=self.sec_annual_url)
        # External browser route
        else:
            self.browser.get(url=url)

    def _clean_close(self) -> None:
        self.browser.close()
        self.browser.quit()

    def _read_data(
        self, xpath: str, wait: bool = False, _wait_time: int = 5, tag: str = ""
    ) -> str:
        """
        :param xpath: Path to the web element.
        :param wait: Boolean to determine if selenium should wait until the element is located.
        :param wait_time: Integer that represents how many seconds selenium should wait, if wait is True.
        :return: (str) Text of the element.
        """

        if wait:
            try:
                data = (
                    WebDriverWait(self.browser, _wait_time)
                    .until(EC.presence_of_element_located((By.XPATH, xpath)))
                    .text
                )
            except TimeoutException:
                print(f"[Failed Xpath] {xpath}")
                if tag != "":
                    print(f"[Tag]: {tag}")
                raise NoSuchElementException("Element not found")
            except NoSuchElementException:
                print(f"[Failed Xpath] {xpath}")
                return "N\A"
        else:
            try:
                data = self.browser.find_element("xpath", xpath).text
            except NoSuchElementException:
                data = "N\A"
        # Return the text of the element found.
        return data

    def _click_button(
        self,
        xpath: str,
        wait: bool = False,
        _wait_time: int = 5,
        scroll: bool = False,
        tag: str = "",
    ) -> None:
        """
        :param xpath: Path to the web element.
        :param wait: Boolean to determine if selenium should wait until the element is located.
        :param wait_time: Integer that represents how many seconds selenium should wait, if wait is True.
        :return: None. Because this function clicks the button but does not return any information about the button or any related web elements.
        """

        if wait:
            try:
                element = WebDriverWait(self.browser, _wait_time).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                # If the webdriver needs to scroll before clicking the element.
                if scroll:
                    self.browser.execute_script("arguments[0].click();", element)
                element.click()
            except TimeoutException:
                print(f"[Failed Xpath] {xpath}")
                if tag != "":
                    print(f"[Tag]: {tag}")
                raise NoSuchElementException("Element not found")
        else:
            element = self.browser.find_element("xpath", xpath)
            if scroll:
                self.browser.execute_script("arguments[0].click();", element)
            element.click()

    """----------------------------------------------------------------------- Insider Trades -----------------------------------------------------------------------"""

    def get_insider_trades(self, ticker: str, update: bool = False):
        ticker = ticker.upper()
        path = f"{self.tickers_folder_path}\\{ticker}.csv"

        try:

            if update:
                df = self._scrape_insider_trades(ticker)
                df.to_csv(path)
                return df
            else:
                df = pd.read_csv(path)
                df.set_index("filing_date", inplace=True)
                return df
        except FileNotFoundError:
            df = self._scrape_insider_trades(ticker)
            df.to_csv(path)
            return df

    def _scrape_insider_trades(self, ticker: str):
        # Xpaths

        data = {
            "url": f"{self.url.format(ticker)}",
            "filing_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[2]/div/a",
                "format": False,
            },
            "trade_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[3]/div",
                "format": False,
            },
            "insider_name": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[5]/a",
                "format": False,
            },
            "title": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[6]",
                "format": True,
            },
            "trade_type": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[7]",
                "format": False,
            },
            "price": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[8]",
                "format": True,
            },
            "quantity": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[9]",
                "format": True,
            },
            "ownership": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[10]",
                "format": True,
            },
            "ownership_change": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[11]",
                "format": True,
            },
            "value": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[12]",
                "format": True,
            },
        }

        df = self._scrape_table(data)
        return df

    """----------------------------------------------------------------------- Cluster Buys -----------------------------------------------------------------------"""

    def get_cluster_buys(self, update: bool = False):
        path = f"{self.base_export_path}\\cluster_buys.csv"

        try:
            if update:
                df = self._scrape_cluster_buys()
                df.to_csv(path)
                return df
            else:
                df = pd.read_csv(path)
                df.set_index("filing_date", inplace=True)
                return df
        except FileNotFoundError:
            df = self._scrape_cluster_buys()
            df.to_csv(path)
            return df

    def _scrape_cluster_buys(self):

        data = {
            "url": "http://openinsider.com/latest-cluster-buys",
            "filing_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[2]/div/a",
                "format": False,
            },
            "trade_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[3]/div",
                "format": False,
            },
            "ticker": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[4]/b/a",
                "format": False,
            },
            "company_name": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[5]/a",
                "format": True,
            },
            "industry": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[6]/a",
                "format": False,
            },
            "insider_name": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[7]",
                "format": False,
            },
            "price": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[9]",
                "format": False,
            },
            "quantity": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[10]",
                "format": True,
            },
            "ownership": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[11]",
                "format": True,
            },
            "ownership_change": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[12]",
                "format": True,
            },
            "value": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[13]",
                "format": True,
            },
        }

        df = self._scrape_table(data)
        return df

    """----------------------------------------------------------------------- Penny Stock Buys -----------------------------------------------------------------------"""

    def get_penny_stock_buys(self, update: bool = False):
        path = f"{self.base_export_path}\\penny_stock_buys.csv"

        try:
            if update:
                df = self._scrape_penny_stock_buys()
                df.to_csv(path)
                return df
            else:
                df = pd.read_csv(path)
                df.set_index("filing_date", inplace=True)
                return df
        except FileNotFoundError:
            df = self._scrape_penny_stock_buys()
            df.to_csv(path)
            return df

    def _scrape_penny_stock_buys(self):
        # Xpaths
        data = {
            "url": "http://openinsider.com/latest-penny-stock-buys",
            "filing_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[2]/div/a",
                "format": False,
            },
            "trade_date": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[3]/div",
                "format": False,
            },
            "ticker": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[4]/b/a",
                "format": False,
            },
            "company_name": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[5]/a",
                "format": True,
            },
            "industry": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[6]/a",
                "format": False,
            },
            "insider_name": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[7]",
                "format": False,
            },
            "price": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[9]",
                "format": False,
            },
            "quantity": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[10]",
                "format": True,
            },
            "ownership": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[11]",
                "format": True,
            },
            "ownership_change": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[12]",
                "format": True,
            },
            "value": {
                "xpath": "/html/body/div[2]/table/tbody/tr[{}]/td[13]",
                "format": True,
            },
        }

        df = self._scrape_table(data)
        return df

    def _scrape_table(self, instructions: dict):
        self._create_browser(instructions["url"])

        # Delete from instruction to not interfere with loop comprehension later.
        del instructions["url"]

        scraping = True
        rows = []
        row_index = 1
        while scraping:
            row = {}
            for k, v in instructions.items():
                data = self._read_data(v["xpath"].format(row_index))
                if self.debug:
                    print(f"[Data]: {data}")
                if data == "N\\A":
                    scraping = False
                else:
                    if v["format"]:
                        data = self._format_value(data)
                    row[k] = data

            if row != {}:
                rows.append(row)
            else:
                scraping = False
            row_index += 1
        self._clean_close()
        df = pd.DataFrame(rows)
        df.set_index("filing_date", inplace=True)
        return df

    def _format_value(self, value):
        positive = True
        # Remove "+" or "-" symbol. Keeps track if value is negative.
        if value[0] == "+" or value[0] == "-":
            if value[0] == "-":
                positive = False
                value = value[1:]
            elif value[0] == "+":
                positive = True
                value = value[1:]
        # Remove "$"" sign
        if value[0] == "$":
            value = value[1:]
        # Remove "%" symbol.
        if value[-1] == "%":
            value = value[:-1]
        # Remove commas.
        if "," in value:
            value = value.replace(",", "")
        # Attempt to fonvert to "final_type".

        try:
            value = float(value)
        except ValueError:
            return value

        if not positive:

            value *= -1
        return value


if __name__ == "__main__":

    oi = OpenInsiderScraper()
    # insider_trades = oi.get_insider_trades("AAPL")
    # cluster_buys = oi.get_cluster_buys()
    # penny_stocks = oi.get_penny_stock_buys()
