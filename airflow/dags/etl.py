from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pandas as pd
import requests

# Load api key
with open("credentials.txt", "r") as file:
    api_key = file.read()

# Default setting for a DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 8),
    "email": ["prayagshah07@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# Converting from kelvin to farenheit
def kelvin_to_farenheit(temp):
    return (temp - 273.15) * 9 / 5 + 32


# Extract, transform and load the weather data
def etl_weather_data():
    combined_df = pd.DataFrame()
    names_of_city = [
        "Halifax",
        "Portland",
        "Seattle",
        "Windsor",
        "Truro",
        "Boston",
        "New York",
    ]

    base_url = "https://api.openweathermap.org"

    for city in names_of_city:
        end_point = "/data/2.5/weather?q=" + city + "&APPID=" + api_key
        full_url = base_url + end_point

        r = requests.get(full_url)
        data = r.json()
        print(data)

        # Getting the weather data into json format
        city = data["name"]
        weather_description = data["weather"][0]["description"]
        temp_farehneit = kelvin_to_farenheit(data["main"]["temp"])
        feels_like_farehneit = kelvin_to_farenheit(data["main"]["feels_like"])
        min_temp_farehneit = kelvin_to_farenheit(data["main"]["temp_min"])
        max_temp_farehneit = kelvin_to_farenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
        sunrise_time = datetime.utcfromtimestamp(
            data["sys"]["sunrise"] + data["timezone"]
        )
        sunset_time = datetime.utcfromtimestamp(
            data["sys"]["sunset"] + data["timezone"]
        )

        # Creating a dictionary to store the weather data
        weather_data = {
            "city": city,
            "weather_description": weather_description,
            "temp_farehneit": temp_farehneit,
            "feels_like_farehneit": feels_like_farehneit,
            "min_temp_farehneit": min_temp_farehneit,
            "max_temp_farehneit": max_temp_farehneit,
            "pressure": pressure,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "time_of_record": time_of_record,
            "sunrise_time": sunrise_time,
            "sunset_time": sunset_time,
        }

        weather_data_list = [weather_data]
        weather_df = pd.DataFrame(weather_data_list)
        combined_df = pd.concat([combined_df, weather_df], ignore_index=True)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = "current_weather_data_" + dt_string
    combined_df.to_csv(f"{dt_string}.csv", index=False)
    output_file = f"/home/ubuntu/{dt_string}.csv"
    return output_file


# Id should be unique everywhere
with DAG(
    "weather_dag", default_args=default_args, schedule_interval="@daily", catchup=False
) as dag:

    # Using this as a Python operator to call the openweather script
    extract_transform_weather_data = PythonOperator(
        task_id="tsk_extract_transform_weather_data", python_callable=etl_weather_data
    )

    # Now we have to send the data to S3 bucket
    # aws s3 is the service, mv is known as move, ti = task instance, xcom_pull will take file which is returned from the function above return output_file
    # we have used aws so we need to install aws cli
    # "" denoted task name and S3 is the bucket URL

    load_to_S3 = BashOperator(
        task_id="tsk_load_to_S3",
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_transform_weather_data")}} s3://weatherapi-slack',
    )

    # We are creating a task for the Slack operator to get the notifications
    slack_notification = SlackWebhookOperator(
        task_id="tsk_slack_notification",
        slack_webhook_conn_id="slack_conn_id",
        message="This is just a test for the notification",
        channel="#airflow_slack",
    )

    extract_transform_weather_data >> load_to_S3 >> slack_notification
