# commerce_app/main.py
import uvicorn

if __name__ == "__main__":
    uvicorn.run("commerce_app.api.rest.app:app", host="0.0.0.0", port=8000, reload=True)
