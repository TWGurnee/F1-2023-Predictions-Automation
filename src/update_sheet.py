import schedule
import time
import threading as td

from googleSheet_resources import (
    update_driver_standings,
    update_team_standings,
    update_prediction_scores,
    update_wildcards
)

if __name__ == "__main__":
    
    starttime = time.time()
    
    updaters = [
        td.Thread(target=update_driver_standings),
        td.Thread(target=update_team_standings),
        td.Thread(target=update_wildcards)
    ]
    
    for u in updaters:
        u.start()
        
    for u in updaters:
        u.join()
    
    update_prediction_scores()
    
    endtime = time.time()
       
    print(f"total time taken: {endtime-starttime}s")
