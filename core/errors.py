"""
Коды и исключения верхнего уровня для конвейера парсинга.

Цели:
- Единый набор стабильных кодов ошибок (для логов/UI).
- Базовый класс PipelineError с полем code для мэппинга в логи.

Заметка:
- Ошибки парсинга отсутствующих полей НЕ должны ронять процесс —
  их лучше фиксировать как ParseIssue; исключения — для критики инфраструктуры.
"""

from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    ERR_LOGIN_FAILED = "ERR_LOGIN_FAILED"
    ERR_HTTP_STATUS = "ERR_HTTP_STATUS"
    ERR_TIMEOUT = "ERR_TIMEOUT"
    ERR_NETWORK = "ERR_NETWORK"
    ERR_ENCODING = "ERR_ENCODING"
    ERR_STOP_REQUESTED = "ERR_STOP_REQUESTED"
    ERR_UNEXPECTED = "ERR_UNEXPECTED"


class PipelineError(Exception):
    """
    Базовое исключение конвейера. Все наследники содержат machine-readable code.
    """

    code: ErrorCode = ErrorCode.ERR_UNEXPECTED

    def __init__(self, message: str = "", *args) -> None:
        super().__init__(message, *args)


class LoginFailedError(PipelineError):
    code = ErrorCode.ERR_LOGIN_FAILED


class HttpStatusError(PipelineError):
    code = ErrorCode.ERR_HTTP_STATUS

    def __init__(self, status: int, url: str, message: str | None = None) -> None:
        msg = message or f"Unexpected HTTP status {status} for {url}"
        super().__init__(msg)
        self.status = status
        self.url = url


class TimeoutError_(PipelineError):  # избегаем конфликта с builtins TimeoutError
    code = ErrorCode.ERR_TIMEOUT


class NetworkError(PipelineError):
    code = ErrorCode.ERR_NETWORK


class EncodingError(PipelineError):
    code = ErrorCode.ERR_ENCODING


class StopRequestedError(PipelineError):
    code = ErrorCode.ERR_STOP_REQUESTED


class UnexpectedError(PipelineError):
    code = ErrorCode.ERR_UNEXPECTED
