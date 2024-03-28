import json
import time
from datetime import datetime
from collections import Counter
import numbers

from mpi4py import MPI




def analyze(twt):
    date_time_str = twt.get("doc").get("data").get("created_at")
    date_time = datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    uniq_hour = (date_time.date(), date_time.hour)
    date = date_time.date()

    hour_cnt[uniq_hour] += 1
    day_cnt[date_time.date()] += 1

    sentiment = twt.get("doc").get("data").get("sentiment", 0) # if "sentiment" doesn't exist, set 0

    if not isinstance(sentiment, numbers.Number):
        # print(hour_happy.get(uniq_hour, 0))
        # print("Non-numeric sentiment:", sentiment)
        sentiment = sentiment.get("score")

    hour_happy[uniq_hour] = hour_happy.get(uniq_hour, 0) + sentiment
    day_happy[date] = day_happy.get(date, 0) + sentiment






def format_hour(act_hour):
    if act_hour == 0:
        display_hour = "12am"
    elif act_hour < 12:
        display_hour = f"{act_hour}am"
    elif act_hour == 12:
        display_hour = "12pm"
    else:
        display_hour = f"{act_hour - 12}pm"
    return display_hour


def format_day(act_date):
    day_suffix = ["th", "st", "nd", "rd"] + ["th"] * 16 + ["st", "nd", "rd"] + ["th"] * 7 + ["st"]
    formatted_date = act_date.strftime(f"%d{day_suffix[act_date.day - 1]} %B")
    return formatted_date

def format_sentiment(sentiment):
    if sentiment > 0:
        return f"+{sentiment:.2f}"
    # elif sentiment < 0:
    #     return f"{sentiment_day:.2f}"
    else:
        return f"{sentiment:.2f}"


if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    hour_happy = {}
    day_happy = {}
    hour_cnt = Counter()
    day_cnt = Counter()

    begin = time.time()

    with open("./twitter-50mb.json", 'r', encoding='utf-8') as twt_file:
        twt_file.readline()  # skip first line "{"total_rows":185875709,"offset":0,"rows":["

        id = 0  # id for processor allocation

        while True:
            twt_str = twt_file.readline()
            if not twt_str:
                break

            elif twt_str.endswith(",\n"):  # twt_str == '...,\n'
                id += 1

                twt_str = twt_str[:-2]
                # print(twt_str)
                twt_json = json.loads(twt_str)
                analyze(twt_json)

            # elif twt_str[1] != '\"':
            #     break

    # most active hour:
    act_day_hour, hour_count = hour_cnt.most_common(1)[0]
    act_date, act_hour = act_day_hour

    act_hour1 = format_hour(act_hour)
    act_hour2 = format_hour(act_hour + 1)

    formatted_date = format_day(act_date)
    print(f"Most active hour: {act_hour1}-{act_hour2} on {formatted_date} had the most tweets (#{hour_count})")

    # most active day:
    act_day, day_count = day_cnt.most_common(1)[0]
    formatted_day = format_day(act_day)
    # print(f"Day with highest count: {act_day}, Count: {day_count}")  # 2021-06-21
    print(f"Most active day: {formatted_day} had the most tweets (#{day_count})")  # 21st June

    # happiest hour:
    happiest_day_hour = max(hour_happy, key=hour_happy.get)
    sentiment_hour = hour_happy[happiest_day_hour]
    happiest_date, happiest_hour = happiest_day_hour
    # print(f"Happiest hour: {happiest_day_hour}, Sentiment Sum: {hour_happy[happiest_day_hour]}")
    happiest_hour1 = format_hour(happiest_hour)
    happiest_hour2 = format_hour(happiest_hour + 1)

    formatted_happiest_date = format_day(happiest_date)
    formatted_sentiment_hour = format_sentiment(sentiment_hour)
    print(
        f"Happiest hour: {happiest_hour1}-{happiest_hour2} on {formatted_happiest_date} with an overall sentiment score of {formatted_sentiment_hour}")

    # happiest day:
    happiest_day = max(day_happy, key=day_happy.get)
    formatted_happiest_day = format_day(happiest_day)
    sentiment_day = day_happy[happiest_day]
    # print(f"Happiest day: {happiest_day}, Sentiment Sum: {day_happy[happiest_day]}")
    formatted_sentiment_day = format_sentiment(sentiment_day)
    print(
        f"Happiest day: {formatted_happiest_day} was the happiest day with an overall sentiment score of {formatted_sentiment_day}")

    time = time.time() - begin
    print(f"Time taken: {time:.2f} seconds")

