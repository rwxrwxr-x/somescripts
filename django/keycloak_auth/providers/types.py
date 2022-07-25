import base64
import hashlib
import inspect
from dataclasses import dataclass, asdict
from typing import NewType

CodeVerifier = NewType('CodeVerifier', str)
HOST = NewType('HOST', str)
URL = NewType('URL', str)
KeycloakRealm = NewType('KeyCloakRealm', str)


@dataclass
class KeycloakUser:
    """
    Keycloak User data type.
    Can be filled by a 'from_dict' method.
    """
    email: str = None
    username: str = None
    preferred_username: str = None
    email_verified: str = None
    sub: str = None
    name: str = None
    given_name: str = None
    family_name: str = None

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**{
            k: v for k, v in kwargs.items()
            if k in inspect.signature(cls).parameters
        })

    def asdict(self):
        return asdict(self)


@dataclass
class KeycloakPath:
    """
    Keycloak endpoints templates, may be override by keycloak_settings.ENDPOINTS_OVERRIDE.

    By default, should be use that with .format() method.
    """
    base_endpoint = 'https://{host}/auth/realms/{realm}/'
    wellknown_endpoint = 'https://{host}/auth/realms/{realm}/.well-known/openid-configuration'
    authorization_endpoint = 'https://{host}/auth/realms/{realm}/protocol/openid-connect/auth?{params}'
    token_endpoint = 'https://{host}/auth/realms/{realm}/protocol/openid-connect/token'
    token_introspection_endpoint = 'https://{host}/realms/{realm}/protocol/openid-connect/token/introspect'
    userinfo_endpoint = 'https://{host}/realms/{realm}/protocol/openid-connect/userinfo/'
    end_session_endpoint = 'https://{host}/auth/realms/{realm}/protocol/openid-connect/logout/'
    jwks_uri = 'https://{host}/auth/realms/{realm}/protocol/openid-connect/certs'
    api_redirect_endpoint = '{api_url}/{endpoint_path_prefix}/auth/token/'

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k.lower(), v)


@dataclass
class CodeChallenge:
    """
    Code Challenge type can generate code_challenge by method and code_verifier. That available by code attribute.
    """
    method: str
    code_verifier: str
    code: str = None

    @staticmethod
    def get_code_challenge(code_verifier: str) -> str:
        hashed = hashlib.sha256(code_verifier.encode('ascii')).digest()
        encoded = base64.urlsafe_b64encode(hashed)
        return encoded.decode('ascii')[:-1]

    def __post_init__(self):
        if self.method != 'S256':
            raise ValueError('Only S256 is supported')

        self.code = self.get_code_challenge(self.code_verifier)
