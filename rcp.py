import asyncio
import websockets
import json
from tabulate import tabulate
import ccxt
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.Formatter.converter = time.gmtime

async def signal_producer(config):
    logger = logging.getLogger("signal_producer")
    api_server = config.get("api_server")
    exchange = config.get("exchange")
    dry_run = config.get("dry_run", False)
    fee = config.get("fee", 0.001)
    while True:
        try:
            uri = (
                "ws://" + 
                api_server["listen_ip_address"] + ":" + 
                str(api_server["listen_port"]) +
                "/api/v1/message/ws?token=" +
                api_server["ws_token"]
            )
            while True:
                try:        
                    async with websockets.connect(uri) as websocket: #ping_interval=None
                        rcp_request = {
                            "type": "subscribe",
                            "data": ["entry_fill", "exit_fill"] #["entry", "exit"]
                        }

                        await websocket.send(json.dumps(rcp_request))
                        logger.info(f'Connected to {api_server["listen_ip_address"]}:{api_server["listen_port"]} - Subscribed to "entry_fill" and "exit_fill" messages')

                        Exchange = getattr(ccxt, exchange["name"])({
                            "apiKey": exchange["key"],
                            "secret": exchange["secret"],
                            # "options": {
                            #     "createMarketBuyOrderRequiresPrice": False,
                            # },
                        })

                        while True:
                            rcp_response = await websocket.recv()
                            rcp_data = json.loads(rcp_response)                            
                            rcp_type = rcp_data.get("type")

                            # Create the rcp tabulated table
                            rcp_table = tabulate(
                                rcp_data.items(),
                                headers=["Field", "Value"],
                                tablefmt="orgtbl",
                                stralign="center",
                            )
                            rcp_title = f'{rcp_data.get("exchange")}/{rcp_type}'.center(len(rcp_table.splitlines()[0]), "=")
                            rcp_table = f'{rcp_title}\n{rcp_table}\n{"=" * len(rcp_table.splitlines()[0])}'
                            print(rcp_table)
                            
                            if not dry_run:
                                pair = rcp_data.get("pair")
                                amount = rcp_data.get("amount") #+0.3%
                                stake_amount = rcp_data.get("stake_amount")
                                try:
                                    if rcp_type == "entry" or rcp_type == "entry_fill":
                                        order = Exchange.createOrder(pair, "market", "buy", amount, rcp_data.get("open_rate"))
                                    elif rcp_type == "entry" or rcp_type == "exit_fill":
                                        retry_limit = 10  # Set the number of retries as needed
                                        while retry_limit > 0:
                                            try:
                                                order = Exchange.createMarketSellOrder(pair, amount)
                                            except Exception as e:
                                                if "Insufficient assets" in str(e) or "PlaceMultiOrdersNormalUser" in str(e) or "The current system is busy, please try again later" in str(e):
                                                    logger.error(
                                                        f'{e} - pair: {pair} | amount: {amount} | stake_amount: {stake_amount} | '
                                                        f'{api_server["listen_ip_address"]}:{api_server["listen_port"]} {rcp_type} failed'
                                                    )
                                                    # Reduce the amount and stake_amount 
                                                    amount = amount * (1 - 0.002)
                                                    stake_amount = stake_amount * (1 - 0.002)
                                                    logger.info(f'Reducing amount to {amount} due to "{e}"')
                                                    await asyncio.sleep(0.0001)
                                                    retry_limit -= 1  # Decrement the retry limit
                                                else:
                                                    logger.error(
                                                        f'{e} - pair: {pair} | amount: {amount} | stake_amount: {stake_amount} | '
                                                        f'{api_server["listen_ip_address"]}:{api_server["listen_port"]} {rcp_type} failed'
                                                    )
                                                    break  # Exit the retry loop if not due to "Insufficient assets"
                                            else:
                                                break # If the order is successful, break out of the retry loop
                                    # Extract relevant data for tabulate
                                    order_data = {
                                        "trade_id": order["info"]["orderId"],
                                        "timeInForce": order["timeInForce"],
                                        "stopLossPrice": order["stopLossPrice"],
                                        "takeProfitPrice": order["takeProfitPrice"],
                                        "exchange": exchange["name"],
                                        "pair": order["symbol"],
                                        "direction": order["side"],
                                        "open_rate": order["price"],
                                        "order_type": order["type"],
                                        "stake_amount": order["info"]["cummulativeQuoteQty"],
                                        "cost": order["cost"],
                                        "fee": float(order["amount"]) * fee * float(order["price"]),
                                        "amount": order["amount"],
                                        "filled": order["filled"],
                                        "open_date": order["datetime"],
                                    }
                                    # Create the buy_order tabulated table
                                    order_table = tabulate(order_data.items(),
                                        headers=["Field", "Value"],
                                        tablefmt="orgtbl",
                                        stralign="center",
                                    )
                                    order_title = f'{exchange["name"]}/{rcp_type}'.center(len(order_table.splitlines()[0]), "=")
                                    order_table = f'{order_title}\n{order_table}\n{"=" * len(order_table.splitlines()[0])}'
                                    print(order_table)
                                except Exception as e:
                                    logger.error(
                                        f'{e} - pair: {pair} | amount: {amount} | stake_amount: {stake_amount} | '
                                        f'{api_server["listen_ip_address"]}:{api_server["listen_port"]} {rcp_type} failed'
                                    )
                            else:
                                logger.info(f'No {rcp_type} for {exchange["name"]} - {api_server["listen_ip_address"]}:{api_server["listen_port"]} is dry_run')
                            balance = Exchange.fetchBalance()
                            # Create a list of dictionaries suitable for tabulate
                            balance_data = [
                                {"Asset": asset, "Free": balance["free"][asset], "Used": balance["used"][asset], "Total": balance["total"][asset]}
                                for asset in balance["free"]
                                if (
                                    balance["free"][asset] != 0
                                    or balance["used"][asset] != 0
                                    or balance["total"][asset] != 0
                                )
                            ]
                            # Create the balance tabulated table
                            balance_table = tabulate(
                                balance_data,
                                headers="keys",
                                tablefmt="orgtbl",
                                stralign="center",
                            )
                            balance_title = "Balance".center(len(balance_table.splitlines()[0]), "=")
                            balance_table = f'{balance_title}\n{balance_table}\n{"=" * len(balance_table.splitlines()[0])}'                   
                            print(balance_table, "\n")
                # except websockets.exceptions.ConnectionClosedError as e:
                #     logger.error(f'{e} - {api_server["listen_ip_address"]}:{api_server["listen_port"]} disconnected')
                #     await asyncio.sleep(0.0001)
                except Exception as e:
                    logger.error(f'{e} - {api_server["listen_ip_address"]}:{api_server["listen_port"]} disconnected')
                    await asyncio.sleep(0.0001)
        except Exception as e:
            logger.error(f"{e} - signal_producer closed")
            logger.info("Reopening signal_producer")
            await asyncio.sleep(0.0001)

# Define the uri_configs dictionary
NFINGK = {
    "api_server": {
        "listen_ip_address": "deepplexus.com",
        "listen_port": 9101,
        "ws_token": "J7kNyiKxsuxeWiQl0JoHJJdZpESmsUML3A",
    },
    "exchange": {
        "name": "bingx",
        "key": "MUYodOEgQtdJQU96Ll8gfH65dra7zxVQWqdahDHFHfahrhdsdfs54654pPo4qjbzwpENUrjn3nPSQ",
        "secret": "AOVjsvWfDgDWkUPKwTWyDfSlJ5sv6541FGSD6gs65ss44zxcSFcds6a5sfasfdf54f5d1s5Tfa2q7KVKw"
    },
}
NFIV7K = {
    "api_server": {
        "listen_ip_address": "deepplexus.com",
        "listen_port": 9102,
        "ws_token": "J7kNyiKxsuxeWiQl0JoHJJdZpESmsUML3A",
    },
    "exchange": {
        "name": "bingx",
        "key": "MUYodOEgQtdJQU96Ll8gfH65dra7zxVQWqdahDHFHfahrhdsdfs54654pPo4qjbzwpENUrjn3nPSQ",
        "secret": "AOVjsvWfDgDWkUPKwTWyDfSlJ5sv6541FGSD6gs65ss44zxcSFcds6a5sfasfdf54f5d1s5Tfa2q7KVKw"
    },
}
NFIX2K = {
    "api_server": {
        "listen_ip_address": "deepplexus.com",
        "listen_port": 9104,
        "ws_token": "J7kNyiKxsuxeWiQl0JoHJJdZpESmsUML3A",
    },
    "exchange": {
        "name": "bingx",
        "key": "MUYodOEgQtdJQU96Ll8gfH65dra7zxVQWqdahDHFHfahrhdsdfs54654pPo4qjbzwpENUrjn3nPSQ",
        "secret": "AOVjsvWfDgDWkUPKwTWyDfSlJ5sv6541FGSD6gs65ss44zxcSFcds6a5sfasfdf54f5d1s5Tfa2q7KVKw"
    },
    "dry_run": True,
}
EMA1K = {
    "api_server": {
        "listen_ip_address": "deepplexus.com",
        "listen_port": 9105,
        "ws_token": "J7kNyiKxsuxeWiQl0JoHJJdZpESmsUML3A",
    },
    "exchange": {
        "name": "bingx",
        "key": "MUYodOEgQtdJQU96Ll8gfH65dra7zxVQWqdahDHFHfahrhdsdfs54654pPo4qjbzwpENUrjn3nPSQ",
        "secret": "AOVjsvWfDgDWkUPKwTWyDfSlJ5sv6541FGSD6gs65ss44zxcSFcds6a5sfasfdf54f5d1s5Tfa2q7KVKw"
    },
    "dry_run": True,
}

configs = [NFINGK, NFIV7K, NFIX2K]

async def main():
    while True:
        # Create a list of tasks for each configuration
        tasks = [signal_producer(conf) for conf in configs]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
