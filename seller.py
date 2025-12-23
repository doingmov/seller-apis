import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина Ozon.

    Args:
        last_id (str): ID последнего товара для постраничного запроса.
        client_id (str): ID клиента Ozon.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Словарь с информацией о товарах.

    Examples:
        >>> get_product_list("", "123", "token")
        {'items': [...], 'total': 10, 'last_id': 'abc'}

        >>> get_product_list(None, "123", "token")
        Traceback (TypeError)
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Возвращает список артикулов товаров магазина.

    Args:
        client_id (str): ID клиента Ozon.
        seller_token (str): API-ключ продавца.

    Returns:
        list: Список строк с offer_id каждого товара.

    Examples:
        >>> get_offer_ids("123", "token")
        ['offer1', 'offer2']

        >>> get_offer_ids("", "")
        Traceback (requests.exceptions.HTTPError)
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на Ozon.

    Args:
        prices (list): Список словарей с ценами для каждого товара.
        client_id (str): ID клиента Ozon.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Ответ API с результатами обновления.

    Examples:
        >>> update_price([{'offer_id': '123', 'price': '5990'}], "123", "token")
        {'updated': 1}

        >>> update_price([], "123", "token")
        Traceback (requests.exceptions.HTTPError)
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на Ozon.

    Args:
        stocks (list): Список словарей с остатками для каждого товара.
        client_id (str): ID клиента Ozon.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Ответ API с результатами обновления.

    Examples:
        >>> update_stocks([{'offer_id': '123', 'stock': 10}], "123", "token")
        {'updated': 1}

        >>> update_stocks([], "123", "token")
        Traceback (requests.exceptions.HTTPError)
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл с остатками часов Casio и возвращает их как список.

    Returns:
        list: Список словарей с информацией о каждом часе.

    Examples:
        >>> download_stock()
        [{'Код': '123', 'Количество': '5', 'Цена': "5'990.00 руб."}]

        >>> download_stock()
        Traceback (requests.exceptions.ConnectionError)
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену из текстового формата в число без разделителей.

    Функция удаляет все символы, кроме цифр, и возвращает строку с числом.
    Например, строка "5'990.00 руб." превращается в "5990".

    Args:
        price (str): Цена в виде строки, которая может содержать пробелы,
        валюту и дробные части.

    Returns:
        str: Цена в виде числа в виде строки (без пробелов и валюты).

    Examples:
        Correct usage:
            >>> price_conversion("5'990.00 руб.")
            '5990'
            >>> price_conversion("12 345,00 руб.")
            '12345'

        Incorrect usage (может вернуть пустую строку или некорректное значение):
            >>> price_conversion(None)
            Traceback (TypeError)
            >>> price_conversion(5990)
            Traceback (AttributeError)
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разбивает список на части по n элементов.

    Args:
        lst (list): Исходный список.
        n (int): Размер частей.

    Returns:
        generator: Генератор списков длиной до n элементов.

    Examples:
        >>> list(divide([1,2,3,4], 2))
        [[1,2],[3,4]]

        >>> list(divide([], 2))
        [[]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
