"""Provider for free proxies.

During init, scrapes the available proxies from proxyscrape.com.
"""

from copy import deepcopy
from random import randint
from typing import List, Optional

import aiohttp
import pandas as pd
import requests
import tqdm.asyncio


# Base class for ProxyProvider
class ProxyProvider:
    # Dictionary of proxies from country -> [] list of proxies. If the proxy provider
    # does not specify country information, the country key is "". The list of proxies
    # is fully qualified, ie. contain the protocol (and user/password if necessary.)
    proxies: dict[str, list[str]] = {}
    bad_proxies: set[str] = set()
    default_retries = 5

    def _get_good_proxy_list(self, country: Optional[str] = None) -> List[str]:
        proxy_list: list[str] = []
        if country is None:
            proxy_list = [
                proxy for proxy_list in self.proxies.values() for proxy in proxy_list
            ]
        elif country in self.proxies:
            proxy_list = self.proxies[country]

        return list(filter(lambda p: p not in self.bad_proxies, proxy_list))

    @staticmethod
    async def _is_good_proxy(session, proxy) -> tuple[str, bool]:
        try:
            async with session.head("https://httpbin.org/ip?json", proxy=proxy) as resp:
                return proxy, resp.status == 200
        except Exception:
            return proxy, False

    async def check_proxies(self, timeout=30, retries=5) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self.bad_proxies.clear()
        all_proxies = self._get_good_proxy_list()
        for _ in range(retries):
            async with aiohttp.ClientSession(timeout=timeout) as session:
                ret = await tqdm.asyncio.tqdm.gather(
                    *[self._is_good_proxy(session, proxy) for proxy in all_proxies]
                )

            all_proxies.clear()
            for proxy, status in ret:
                if status is False:
                    self.bad_proxies.add(proxy)
                else:
                    all_proxies.append(proxy)
            print("Num bad proxies so far: ", len(self.bad_proxies))

    def get_one_proxy(self, country: Optional[str] = None) -> str | None:
        good_proxies = self._get_good_proxy_list(country=country)
        if len(good_proxies) > 0:
            return good_proxies[randint(0, len(good_proxies) - 1)]
        return None


class ProxyProviderFromList(ProxyProvider):
    def __init__(self, proxy_list: list[str]):
        self.proxies["*"] = deepcopy(proxy_list)


class ProxyProviderFromDict(ProxyProvider):
    def __init__(self, proxy_dict: dict[str, list[str]]):
        self.proxies = deepcopy(proxy_dict)


class ProxyProviderFromProxyscrape(ProxyProvider):
    def __init__(self):
        # Init with language independent list.
        resp = requests.get(
            "https://api.proxyscrape.com/v2/?request=displayproxies"
            + "&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
        )
        self.proxies["*"] = [f"http://{p}" for p in resp.text.strip().split("\r\n")]
        print(self.proxies)


class ProxyProviderFromWebshare(ProxyProvider):
    def __init__(self, APIKEY: str):
        response = requests.get(
            "https://proxy.webshare.io/api/proxy/list/",
            headers={"Authorization": f"Token {APIKEY}"},
        )
        df_proxies = pd.json_normalize(response.json()["results"])
        df_proxies["proxy_string"] = df_proxies.apply(
            lambda row: (
                f"http://{row['username']}:{row['password']}@"
                + f"{row['proxy_address']}:{row['ports.http']}"
            ),
            axis=1,
        )
        print(f"Got {df_proxies.shape[0]} proxies from webshare.")
        self.proxies = deepcopy(
            df_proxies[["country_code", "proxy_string"]]
            .groupby("country_code")
            .agg(list)
            .to_dict()["proxy_string"]
        )


class ProxyProviderFromIPRoyal(ProxyProvider):
    def __init__(self, APIKEY: str, order_id: int) -> None:
        super().__init__()

        response = requests.get(
            "https://dashboard.iproyal.com/api/servers/proxies/reseller/"
            + f"{order_id}/credentials",
            headers={
                "X-Access-Token": f"Bearer {APIKEY}",
                "Content-Type": "application/json",
            },
        )

        df_proxies = pd.json_normalize(response.json()["data"])
        df_proxies["proxy_string"] = df_proxies.apply(
            lambda row: f"http://{row['username']}:{row['password']}@{row['ip']}:12323",
            axis=1,
        )
        print(f"Got {df_proxies.shape[0]} proxies from IPRoyal.")
        self.proxies["*"] = deepcopy(list(df_proxies["proxy_string"].values))
