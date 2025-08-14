import requests
from time import sleep
import datetime
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib.patches import Rectangle
import mysql.connector
import io
from PIL import Image, ImageChops

WEBHOOK_URL1 = "https://discord.com/api/webhooks/1405326694331584675/PrJd9JOCa3MFL9Se4-b45akl5FltHBz84u2BOQY_FAM1swpuzU1pAD04Zp7upvJ6CoLj"
WEBHOOK_URL2 = "https://discord.com/api/webhooks/1405326799197569208/WZjEIGI3_ZB16xUdM248GNhycd37KIZlnHzpxCYGmpJLbTrgCYWKJEISKL2wbDbXxz47"
WEBHOOK_URL3 = "https://discord.com/api/webhooks/1405326928847704084/jZ6SSUBuYRxgg6E2P-QKehLX5I6vXGg9cP_QnUUKn3xnz3xfYew2E1KGtqjoWMeIn4eS"
WEBHOOK_SHITTY = "https://discord.com/api/webhooks/1405320769478725812/923wagmJ0lpHPLmbettrPk5SZa2AEnSDlBPXoBFfmQCf3ye-m8Tpy2uEErqbDtZDUM1h"

url = f"https://data.solanatracker.io/"
headers = {
    "x-api-key": "aea2e858-9e73-4074-b39f-2a32c8a3d5d5"
}

config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'guilherme12',
    'database': 'coin-snipper'
}

### db connnection

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("[db] connected")
except mysql.connector.Error as err:
    print(f"[db] error: {err}")

def insert_address(address):
        try:
            cursor.execute("INSERT IGNORE INTO coin (address) VALUES (%s)", (address,))
            conn.commit()
            return cursor.rowcount
        except mysql.connector.Error as err:
            print(f"[db] error: {err}")

### get kols

traders = requests.get(url+f"top-traders/all", headers=headers).json()
sleep(1)

token_addresses = set()

exclude_address = "So11111111111111111111111111111111111111112"

wallet_win_percentage = {item["wallet"]: item["summary"]["winPercentage"] for item in traders["wallets"]}

for wallet, win in wallet_win_percentage.items():
    coins_traded = requests.get(url+f"wallet/{wallet}/trades", headers=headers).json()
    sleep(1)
    for trade in coins_traded["trades"]:
        from_addr = trade["from"]["address"]
        to_addr = trade["to"]["address"]
        
        if from_addr != exclude_address and win>67.5:
            token_addresses.add(from_addr)
        if to_addr != exclude_address and win>67.5:
            token_addresses.add(to_addr)

### data

token_address_list = list(token_addresses)

for token_address in token_address_list:

    try:
        if insert_address(token_address):
            print(f"[log] ca: {token_address}")
            response = requests.get(url+f"tokens/{token_address}", headers=headers)
            sleep(1)
            response_json = response.json()

            created_at_ms = response_json["pools"][0]["createdAt"]
            creation_data = datetime.datetime.fromtimestamp(created_at_ms / 1000)
            formated_data = creation_data.strftime("%d %B %Y %H:%M")

            volume = response_json["pools"][0]["txns"]["volume"]/(response_json["pools"][0]["price"]["usd"]/response_json["pools"][0]["price"]["quote"])

            if volume >=7.5 and volume<=90: 

                try:
                    website = response_json["token"]["strictSocials"]["website"]
                except (KeyError, TypeError):
                    website = "null"

                try:
                    twitter = response_json["token"]["strictSocials"]["twitter"]
                except (KeyError, TypeError):
                    twitter = "null"

                ### photo

                chart = requests.get(url+f"chart/{token_address}", headers=headers).json()
                sleep(1)

                trades = chart["oclhv"]
                df = pd.DataFrame(trades)

                df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
                df['time_pt'] = df['time'].dt.tz_convert('Europe/Lisbon')

                total_supply = response_json["pools"][0]["tokenSupply"]

                zero_vol_streak = 0
                end_idx = len(df) - 1
                for i, vol in enumerate(df['volume']):
                    if vol == 0:
                        zero_vol_streak += 1
                        if zero_vol_streak >= 10:
                            end_idx = i - 10
                            break
                    else:
                        zero_vol_streak = 0

                df = df.loc[:end_idx].reset_index(drop=True)

                duration_minutes = (df['time_pt'].iloc[-1] - df['time_pt'].iloc[0]).total_seconds() / 60

                if zero_vol_streak <= 10 and duration_minutes<10:
                    base_interval = pd.Timedelta(milliseconds=200)
                else:
                    base_interval = pd.Timedelta(minutes=1)

                start_time = df['time_pt'].iloc[0]
                first_candle_end = start_time + base_interval / 2
                intervals = []
                for t in df['time_pt']:
                    if t <= first_candle_end:
                        intervals.append(start_time)
                    else:
                        intervals.append(t.floor(base_interval))
                df['interval'] = intervals

                agg = df.groupby('interval').agg(
                    open_price=('open', 'first'),
                    close_price=('close', 'last'),
                    high_price=('high', 'max'),
                    low_price=('low', 'min'),
                    volume=('volume', 'sum')
                ).reset_index()

                agg['market_cap_open'] = agg['open_price'] * total_supply
                agg['market_cap_close'] = agg['close_price'] * total_supply
                agg['market_cap_high'] = agg['high_price'] * total_supply
                agg['market_cap_low'] = agg['low_price'] * total_supply

                agg = agg[agg['market_cap_high'] > 3000].reset_index(drop=True)
                agg['x'] = range(len(agg))

                plt.style.use('dark_background')
                fig, ax = plt.subplots(figsize=(17, 8))
                ax.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)

                up_color, down_color = 'green', 'red'
                width = 0.6

                for idx, row in agg.iterrows():
                    x = row['x']
                    color = up_color if row['market_cap_close'] >= row['market_cap_open'] else down_color
                    ax.plot([x, x], [row['market_cap_low'], row['market_cap_high']], color='lightgray', linewidth=1)
                    rect = Rectangle(
                        (x - width/2, min(row['market_cap_open'], row['market_cap_close'])),
                        width,
                        max(abs(row['market_cap_close'] - row['market_cap_open']), 0.1),
                        facecolor=color,
                        edgecolor=color
                    )
                    ax.add_patch(rect)

                if zero_vol_streak <= 10 and duration_minutes<10:
                    step = max(len(agg)//60, 2)
                    time_fmt = '%H:%M:%S'
                else:
                    step = 10
                    time_fmt = '%H:%M'

                xticks = agg['x'][::step]
                xticklabels = agg['interval'].dt.strftime(time_fmt)[::step]
                ax.set_xticks(xticks)
                ax.set_xticklabels(xticklabels, rotation=45)

                y_margin = (agg['market_cap_high'].max() - agg['market_cap_low'].min()) * 0.1
                ax.set_ylim(agg['market_cap_low'].min() - y_margin, agg['market_cap_high'].max() + y_margin)

                max_val, min_val = agg['market_cap_high'].max(), agg['market_cap_low'].min()
                ax.text(0.01, 0.99, f'---- High: {max_val:,.0f}\n---- Low: {min_val:,.0f}', transform=ax.transAxes,
                        fontsize=10, verticalalignment='top', color='white',
                        bbox=dict(facecolor='black', alpha=0.5, edgecolor='white', boxstyle='round,pad=0.5'))

                ax.axhline(max_val, color='red', linestyle='--', linewidth=1)
                ax.axhline(min_val, color='green', linestyle='--', linewidth=1)

                ax.set_title(f'{response_json["token"]["name"]}', fontsize=14, fontweight='bold', pad=20)
                ax.set_ylabel('Marketcap', fontsize=11)

                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
                buf.seek(0)

                ### last release

                ### final message

                data = {
                    "embeds": [
                        {
                            "description": f'**`{formated_data}`**',
                            "color": 0x00ff00,
                            "thumbnail": {
                                "url": response_json["token"]["image"]
                            },
                            "fields": [
                                {"name": "⠀", "value": f'''symbol: ${response_json["token"]["symbol"]}
                                                                    name: {response_json["token"]["name"]}
                                                                    volume: {volume:.2f} sol
                                                                    mint: `{response_json["token"]["mint"]}`
                                                                    twitter: {twitter}
                                                                    website: {website}''', "inline": False}
                            ],
                        }
                    ]
                }

                files = {
                    "file": ("grafico.png", buf, "image/png")
                }

                payload = {
                    "embeds": data["embeds"]
                }

                if volume>=7.5 and volume<=40: webhook = WEBHOOK_SHITTY
                if volume>40 and volume<=55: webhook = WEBHOOK_URL3
                if volume>55 and volume<=70: webhook = WEBHOOK_URL2
                if volume>70 and volume<=90: webhook = WEBHOOK_URL1

                requests.post(webhook, data={"payload_json": json.dumps(payload)}, files=files)

                buf.close()
                plt.close(fig)
    except Exception as e:
        print(f"error: {e}")