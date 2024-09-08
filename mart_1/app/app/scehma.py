from sqlmodel import Field, SQLModel

class Order(SQLModel):
    id: int | None = Field(default=None)
    product: str = Field(index=True)
    product_id: int = Field(default =None, index=True)