from fastapi import APIRouter

from server.timedTask.api import router as timed_task_router

router = APIRouter()
router.include_router(timed_task_router, prefix="/interview", tags=["timed_task"])
