from fastapi import FastAPI
from starlette.responses import JSONResponse

from src.exeptions.service_exc import ResourceNotFound, NoRightsService, ServiceException


def register_exception_handlers(app: FastAPI):

    @app.exception_handler(ServiceException)
    async def service_handler(request, exc):
        return JSONResponse(status_code=500, content={"detail": "Server Error"})

    @app.exception_handler(ResourceNotFound)
    async def not_found_handler(request, exc):
        return JSONResponse(status_code=404, content={"detail": "Not found"})

    @app.exception_handler(NoRightsService)
    async def no_rights_handler(request, exc):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})