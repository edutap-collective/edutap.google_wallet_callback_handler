import uvicorn


def main():
    uvicorn.run(
        "edutap.google_wallet_callback_handler.callback_handler:app",
        host="127.0.0.1",
        port=9000,
        log_level="debug",
        reload=True,
    )


if __name__ == "__main__":
    main()
