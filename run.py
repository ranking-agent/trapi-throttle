import uvicorn

if __name__ == "__main__":
    uvicorn.run("trapi_throttle.server:APP",
                host="0.0.0.0",
                port=7830,
                reload=True)
