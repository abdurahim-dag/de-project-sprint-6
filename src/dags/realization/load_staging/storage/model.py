from pydantic import BaseModel

class Workflow(BaseModel):
    id: int = 0
    workflow_key: str

    class Config:
        # Проверка всех типов
        smart_union = True
