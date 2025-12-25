import datetime
import telebot
import os
from metrics_calculator import calculate_metrics
import yaml
import aiogram

# with open("secrets.yaml", "rb") as f:
#     secrets = yaml.safe_load(f)

today = datetime.datetime.now().strftime("%Y-%m-%d")

token = os.environ["TELEGRAM_TOKEN2"]
pg_user = os.environ["PG_USER2"]
pg_password = os.environ["PG_PASSWORD"]
pg_host = os.environ["PG_HOST"]

# token = secrets["TELEGRAM_TOKEN"]
# pg_user = secrets["PG_USER"]
# pg_password = secrets["PG_PASSWORD"]
# pg_host = secrets["PG_HOST"]

(
    message,
    image_trial,
    image_conversion,
    image_cash_in,
    image_ltr,
    image_inspection,
    image_unique,
    image_avg_views_per_users,
    image_new_paying_users,
) = calculate_metrics(
    current_date=today, db_host=pg_host, db_user=pg_user, db_password=pg_password
)


bot = telebot.TeleBot(token)

bot.send_message("1759890204", message)

images = ['trial_history', 'payment_conv', 'cash_history', 'duration_completion','nusers_history','CAC_LTV']
for image_name in images:
    image_path = f'{image_name}.png'  
    image = open(image_path, 'rb')
    bot.send_photo("1759890204", image)

bot.send_photo("1759890204", image_trial)
bot.send_photo("1759890204", image_conversion)
bot.send_photo("1759890204", image_cash_in)
bot.send_photo("1759890204", image_ltr)
bot.send_photo("1759890204", image_inspection)
bot.send_photo("1759890204", image_unique)
bot.send_photo("1759890204", image_avg_views_per_users)
bot.send_photo("1759890204", image_new_paying_users)
