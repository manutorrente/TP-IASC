# app.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import asyncio

app = FastAPI(title="App Satelites")

@app.get("/health", tags=["Health"])
async def health_check():
    await asyncio.sleep(0.1)  
    return JSONResponse(content={"status": "ok", "message": "Service is healthy"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)