from .callback_handler import app

import uvicorn


def main():
    uvicorn.run(app, host="127.0.0.1", port=9000, log_level="debug")


if __name__ == "__main__":
    main()
