from datetime import datetime

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator


class CreateLinkRequest(BaseModel):
    url: AnyHttpUrl
    custom_code: str | None = Field(default=None, min_length=3, max_length=32)
    expires_at: datetime | None = None

    @field_validator("url")
    @classmethod
    def reject_url_credentials(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("URLs containing user credentials are not accepted")
        return value

    @field_validator("custom_code")
    @classmethod
    def validate_custom_code(cls, value: str | None) -> str | None:
        if value is not None and not value.replace("-", "").replace("_", "").isalnum():
            raise ValueError("custom_code may contain only letters, digits, '-' and '_'")
        return value

    @field_validator("expires_at")
    @classmethod
    def validate_expiration_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("expires_at must include a timezone")
        return value


class LinkResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    short_url: str
    url: str
    created_at: datetime
    expires_at: datetime | None


class DailyClicks(BaseModel):
    date: datetime
    clicks: int


class AnalyticsResponse(BaseModel):
    code: str
    total_clicks: int
    daily_clicks: list[DailyClicks]
