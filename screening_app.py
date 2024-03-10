
from os import getenv
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from starlette.requests import Request

screen_app = FastAPI()



screen_app.mount("/static", StaticFiles(directory="static"), name="static")
screen_app.add_middleware(GZipMiddleware)
templates = Jinja2Templates(directory="templates")


@screen_app.get("/")
async def root(request: Request) -> Jinja2Templates:
    return templates.TemplateResponse("base.html",{"request":request})


