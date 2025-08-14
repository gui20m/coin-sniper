import requests
from time import sleep
import datetime
import boto3
import numpy as np
from skimage.metrics import structural_similarity as ssim
import cv2
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib.patches import Rectangle
from PIL import Image, ImageChops, UnidentifiedImageError
import mysql.connector
import io
import os
from io import BytesIO
from PIL import Image, ImageChops
from zoneinfo import ZoneInfo

WEBHOOK_URL1 = os.getenv("API_1")
WEBHOOK_URL2 = os.getenv("API_2")
WEBHOOK_URL3 = os.getenv("API_3")
WEBHOOK_SHITTY = os.getenv("API_4")

url = f"https://data.solanatracker.io/"
headers = {"x-api-key": os.getenv("API_5")}

config = {
    'host': os.getenv("host"),
    'port': os.getenv("port"),
    'user': 'root',
    'password': os.getenv("PASSW"),
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
        cursor.execute("INSERT IGNORE INTO coin (address) VALUES (%s)",
                       (address, ))
        conn.commit()
        return cursor.rowcount
    except mysql.connector.Error as err:
        print(f"[db] error: {err}")


### get kols

traders = requests.get(url + f"top-traders/all", headers=headers).json()
sleep(1)

token_addresses = set()

exclude_address = "So11111111111111111111111111111111111111112"

wallet_win_percentage = {
    item["wallet"]: item["summary"]["winPercentage"]
    for item in traders["wallets"]
}

for wallet, win in wallet_win_percentage.items():
    coins_traded = requests.get(url + f"wallet/{wallet}/trades",
                                headers=headers).json()
    sleep(1)
    for trade in coins_traded["trades"]:
        from_addr = trade["from"]["address"]
        to_addr = trade["to"]["address"]

        if from_addr != exclude_address and win > 65.5:
            token_addresses.add(from_addr)
        if to_addr != exclude_address and win > 65.5:
            token_addresses.add(to_addr)

### data

token_address_list = list(token_addresses)

for token_address in token_address_list:

    try:
        if insert_address(token_address):
            response = requests.get(url + f"tokens/{token_address}",
                                    headers=headers)
            sleep(1)
            response_json = response.json()

            created_at_ms = response_json["pools"][0]["createdAt"]
            creation_data = datetime.datetime.fromtimestamp(created_at_ms /
                                                            1000, tz=ZoneInfo("Europe/Lisbon"))
            formated_data = creation_data.strftime("%d %B %Y %H:%M")

            volume = response_json["pools"][0]["txns"]["volume"] / (
                response_json["pools"][0]["price"]["usd"] /
                response_json["pools"][0]["price"]["quote"])

            print(f"[log] ca: {token_address}, vol:{volume:.2f}")

            if volume >= 7.5 and volume <= 90:

                try:
                    website = response_json["token"]["strictSocials"][
                        "website"]
                except (KeyError, TypeError):
                    website = "null"

                try:
                    twitter = response_json["token"]["strictSocials"][
                        "twitter"]
                except (KeyError, TypeError):
                    twitter = "null"

                ### photo

                chart = requests.get(url + f"chart/{token_address}",
                                     headers=headers).json()
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

                duration_minutes = (df['time_pt'].iloc[-1] -
                                    df['time_pt'].iloc[0]).total_seconds() / 60

                if zero_vol_streak <= 10 and duration_minutes < 10:
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

                agg = df.groupby('interval').agg(open_price=('open', 'first'),
                                                 close_price=('close', 'last'),
                                                 high_price=('high', 'max'),
                                                 low_price=('low', 'min'),
                                                 volume=('volume',
                                                         'sum')).reset_index()

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
                    color = up_color if row['market_cap_close'] >= row[
                        'market_cap_open'] else down_color
                    ax.plot([x, x],
                            [row['market_cap_low'], row['market_cap_high']],
                            color='lightgray',
                            linewidth=1)
                    rect = Rectangle(
                        (x - width / 2,
                         min(row['market_cap_open'], row['market_cap_close'])),
                        width,
                        max(
                            abs(row['market_cap_close'] -
                                row['market_cap_open']), 0.1),
                        facecolor=color,
                        edgecolor=color)
                    ax.add_patch(rect)

                if zero_vol_streak <= 10 and duration_minutes < 10:
                    step = max(len(agg) // 60, 2)
                    time_fmt = '%H:%M:%S'
                else:
                    step = 10
                    time_fmt = '%H:%M'

                xticks = agg['x'][::step]
                xticklabels = agg['interval'].dt.strftime(time_fmt)[::step]
                ax.set_xticks(xticks)
                ax.set_xticklabels(xticklabels, rotation=45)

                y_margin = (agg['market_cap_high'].max() -
                            agg['market_cap_low'].min()) * 0.1
                ax.set_ylim(agg['market_cap_low'].min() - y_margin,
                            agg['market_cap_high'].max() + y_margin)

                max_val, min_val = agg['market_cap_high'].max(
                ), agg['market_cap_low'].min()
                ax.text(0.01,
                        0.99,
                        f'---- High: {max_val:,.0f}\n---- Low: {min_val:,.0f}',
                        transform=ax.transAxes,
                        fontsize=10,
                        verticalalignment='top',
                        color='white',
                        bbox=dict(facecolor='black',
                                  alpha=0.5,
                                  edgecolor='white',
                                  boxstyle='round,pad=0.5'))

                ax.axhline(max_val, color='red', linestyle='--', linewidth=1)
                ax.axhline(min_val, color='green', linestyle='--', linewidth=1)

                ax.set_title(f'{response_json["token"]["name"]}',
                             fontsize=14,
                             fontweight='bold',
                             pad=20)
                ax.set_ylabel('Marketcap', fontsize=11)

                buf = io.BytesIO()
                fig.savefig(buf,
                            format='png',
                            dpi=150,
                            bbox_inches='tight',
                            facecolor=fig.get_facecolor())
                buf.seek(0)


                ### last release

                def download_image(url):
                    """Baixa a imagem da URL e converte para array OpenCV"""
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        image_array = np.frombuffer(response.content, np.uint8)
                        image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
                        return image
                    except Exception:
                        return None

                def compare_images(img1, img2, threshold=0.99):
                    """Compara duas imagens usando SSIM"""
                    if img1 is None or img2 is None:
                        return False

                    height = min(img1.shape[0], img2.shape[0])
                    width = min(img1.shape[1], img2.shape[1])
                    img1_resized = cv2.resize(img1, (width, height))
                    img2_resized = cv2.resize(img2, (width, height))

                    gray1 = cv2.cvtColor(img1_resized, cv2.COLOR_BGR2GRAY)
                    gray2 = cv2.cvtColor(img2_resized, cv2.COLOR_BGR2GRAY)

                    similarity, _ = ssim(gray1, gray2, full=True)
                    return similarity >= threshold

                params = {
                    "query": response_json["token"]["name"],
                    "symbol": response_json["token"]["symbol"]
                }

                try:
                    response = requests.get(url + "search", headers=headers, params=params, timeout=10)
                    response.raise_for_status()
                    all_releases = response.json()
                except requests.RequestException:
                    all_releases = {"data": []}

                image_createdAt = {}
                for item in all_releases.get('data', []):
                    try:
                        image_createdAt[item['image']] = item['createdAt']
                    except KeyError:
                        continue

                base_image_url = response_json["token"]["image"]
                base_image = download_image(base_image_url)

                if base_image is None:
                    print("failed to download base image.")
                else:
                    matching_images = {}
                    for img_url, createdAt in image_createdAt.items():
                        img = download_image(img_url)
                        if img is None:
                            continue
                        if compare_images(base_image, img):
                            matching_images[img_url] = createdAt

                    if matching_images:
                        try:
                            latest_url = max(matching_images, key=lambda k: matching_images[k])
                            latest_ts = matching_images[latest_url]
                            latest_date = datetime.datetime.fromtimestamp(latest_ts / 1000, tz=ZoneInfo("Europe/Lisbon"))
                        except Exception as e:
                            latest_date = None
                            print(f"failed to fetch image: {e}")
                    else:
                        latest_date = None
                        print("no matching image.")

                last_release_str = latest_date.strftime("%d %B %Y %H:%M") if latest_date else "null"
                
                ### final message

                data = {
                    "embeds": [{
                        "description":
                        f'**`{formated_data}`**',
                        "color":
                        0x00ff00,
                        "thumbnail": {
                            "url": response_json["token"]["image"]
                        },
                        "fields": [{
                            "name": "⠀",
                            "value":
                            f'''symbol: ${response_json["token"]["symbol"]}
                                                                    name: {response_json["token"]["name"]}
                                                                    volume: {volume:.2f} sol
                                                                    mint: `{response_json["token"]["mint"]}`
                                                                    twitter: {twitter}
                                                                    website: {website}
                                                                    last release: `{last_release_str}`
                                                                    ''',
                            "inline": False
                        }],
                    }]
                }

                files = {"file": ("grafico.png", buf, "image/png")}

                payload = {"embeds": data["embeds"]}

                if volume >= 7.5 and volume <= 40: webhook = WEBHOOK_SHITTY
                if volume > 40 and volume <= 55: webhook = WEBHOOK_URL3
                if volume > 55 and volume <= 70: webhook = WEBHOOK_URL2
                if volume > 70 and volume <= 90: webhook = WEBHOOK_URL1

                requests.post(webhook,
                              data={"payload_json": json.dumps(payload)},
                              files=files)
                sleep(1)

                buf.close()
                plt.close(fig)
    except Exception as e:
        print(f"error: {e}")
