from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from scraper import download_and_update_tdcc
from screener import run_screener
from pydantic import BaseModel
import os
from database import SessionLocal, TDCCData
from sqlalchemy import func


app = FastAPI()

current_progress = {"percent": 0, "eta": -1}



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScreenParams(BaseModel):
    retail_level: int = 9
    large_level: int = 14
    weeks: int = 3
    ma_diff_percent: float = 5.0
    start_date: str = None
    end_date: str = None

class UpdateParams(BaseModel):
    weeks: int = 12

@app.post("/api/update_data")
def update_data(params: UpdateParams):
    result = download_and_update_tdcc(weeks=params.weeks)
    return result

@app.get("/api/dates")
def get_dates():
    db = SessionLocal()
    dates_counts = db.query(TDCCData.date, func.count(TDCCData.id)).group_by(TDCCData.date).all()
    db.close()

    
    if not dates_counts:
        return {"status": "success", "dates": []}
        
    date_list = sorted([{"date": d[0], "count": d[1]} for d in dates_counts], key=lambda x: x["date"], reverse=True)
    return {"status": "success", "dates": date_list}

@app.get("/api/progress")
def get_progress():
    return current_progress



@app.post("/api/screener")
def screener(params: ScreenParams):
    global current_progress
    current_progress = {"percent": 0, "eta": -1}
    
    def update_progress(percent, eta):
        global current_progress
        current_progress = {"percent": percent, "eta": eta}
        
    results = run_screener(
        retail_level=params.retail_level,
        large_level=params.large_level,
        weeks=params.weeks,
        ma_diff_percent=params.ma_diff_percent,
        start_date=params.start_date,
        end_date=params.end_date,
        progress_callback=update_progress
    )
    
    current_progress = {"percent": 100, "eta": 0}
    if isinstance(results, dict) and "error" in results:
        return {"status": "error", "message": results["error"]}
        
    return {"status": "success", "data": results}


# Mount frontend
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
