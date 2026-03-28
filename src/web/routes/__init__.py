"""
API 路由模块
"""

from fastapi import APIRouter

from .accounts import router as accounts_router
from .registration import router as registration_router
from .settings import router as settings_router
from .email import router as email_services_router
from .payment import router as payment_router
from .cliproxy import router as cliproxy_router
from .audit import router as audit_router
from .upload.cpa_services import router as cpa_services_router
from .upload.sub2api_services import router as sub2api_services_router
from .upload.tm_services import router as tm_services_router

api_router = APIRouter()

# 注册各模块路由
api_router.include_router(accounts_router, prefix="/accounts", tags=["accounts"])
api_router.include_router(registration_router, prefix="/registration", tags=["registration"])
api_router.include_router(settings_router, prefix="/settings", tags=["settings"])
api_router.include_router(email_services_router, prefix="/email-services", tags=["email-services"])
api_router.include_router(payment_router, prefix="/payment", tags=["payment"])
api_router.include_router(cliproxy_router, prefix="/cliproxy-environments", tags=["cliproxy-environments"])
api_router.include_router(cliproxy_router, prefix="/cliproxy", tags=["cliproxy"])
api_router.include_router(audit_router, prefix="/audit", tags=["audit"])
api_router.include_router(cpa_services_router, prefix="/cpa-services", tags=["cpa-services"])
api_router.include_router(sub2api_services_router, prefix="/sub2api-services", tags=["sub2api-services"])
api_router.include_router(tm_services_router, prefix="/tm-services", tags=["tm-services"])
