import schedule
import time

from googleSheet_resources import (
    update_driver_standings,
    update_team_standings,
    update_prediction_scores,
    update_wildcards
)


def scheduler():
    schedule.every().sunday.at("22:00").do(update_driver_standings)
    schedule.every().sunday.at("22:00").do(update_team_standings)
    schedule.every().sunday.at("22:01").do(update_prediction_scores)
    schedule.every().sunday.at("22:02").do(update_wildcards)

    try:
        print("F1 2023 Sheet updater running...")
        while True:
            schedule.run_pending()
            time.sleep(100)

    except Exception as e:
        print(f"Scheduler interrupted: {e}")


if __name__ == "__main__":
    scheduler()
