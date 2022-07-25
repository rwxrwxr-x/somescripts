import abc
from typing import Dict, List

User = type('User', tuple(), {})


class BaseKeyCloak(abc.ABC):
    """
    Base class for Keycloak authentication provider.
    """
    settings_key_template = 'base:%s'
    content_type = 'application/x-www-form-urlencoded'
    grant_types = {
        'access': 'authorization_code',
        'client': 'client_credentials',
        'password': 'password',
        'refresh': 'refresh_token'
    }

    @abc.abstractmethod
    def authenticate(self, response_type: str, response_mode: str, scope: List[str],
                     redirect_uri: str = None):
        ...

    @abc.abstractmethod
    def token(self, grant_type: str, **kwargs) -> Dict[str, str]:
        ...

    @abc.abstractmethod
    def token_verify(self, token: str) -> User:
        ...

    @abc.abstractmethod
    def introspect(self, sub: str) -> Dict[str, str]:
        ...

    @abc.abstractmethod
    def logout(self, refresh_token: str) -> bool:
        ...


class Connect:
    @abc.abstractmethod
    def connect(self) -> BaseKeyCloak:
        ...


class MetaSingleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(MetaSingleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
