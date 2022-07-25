import urllib.parse
from base64 import b64decode
from dataclasses import fields
from typing import Optional
from typing import Union, List, Dict, Any

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PUBLIC_KEY_TYPES
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from rest_framework.settings import APISettings

from keycloak_auth.providers.abstract import BaseKeyCloak, MetaSingleton
from keycloak_auth.providers.basic.jwt import KeycloakJWT
from keycloak_auth.providers.types import CodeChallenge, KeycloakPath, HOST, URL, KeycloakRealm, KeycloakUser
from keycloak_auth.providers.utils import make_nonce, make_state, make_redirect_uri
from keycloak_auth.settings import keycloak_settings

__all__ = ['Keycloak', 'KeycloakConnect']


class Keycloak(BaseKeyCloak, Session):
    DEFAULT_POOL_CONNECTIONS: int = 5
    DEFAULT_POOL_MAXSIZE: int = 5
    DEFAULT_CODE_CHALLENGE_METHOD: str = 'S256'
    DEFAULT_JWT_ALGORITHMS: frozenset = frozenset(['RS256'])
    settings_key_template: str = 'keycloak:%s'
    user_type_cls = KeycloakUser

    def __init__(self,
                 host: HOST,
                 realm: KeycloakRealm,
                 client_id: str,
                 client_secret: str,
                 api_url: URL = None,
                 endpoints_override: KeycloakPath = None,
                 endpoint_path_prefix: str = None,
                 **kwargs) -> None:
        """

        :param host:
        :param realm:
        :param client_id:
        :param client_secret:
        :param api_url:
        :param endpoint_path_prefix:
        :param kwargs:
        Optional params:
            code_verifier: str
            pool_connections: int
            pool_maxsize: int
        """
        self._public_key = kwargs.get('public_key')
        self._host = host
        self._realm = realm
        self._client_id = client_id
        self._client_secret = client_secret
        self._settings_key = kwargs.get('settings_key')
        self._api_url = api_url or getattr(keycloak_settings, self._settings_key).API_URL
        self._endpoint_path_prefix = endpoint_path_prefix or getattr(keycloak_settings, self._settings_key) \
            .ENDPOINT_PATH_PREFIX
        self._code_verifier = kwargs.get('code_verifier')
        self._keycloak_path = endpoints_override or KeycloakPath()

        options = kwargs.get('options', {})
        self._algorithms = options.get('algorithms') or self.DEFAULT_JWT_ALGORITHMS
        self._audience = options.get('audience')

        super(Keycloak, self).__init__()

        self.headers.update({'Content-Type': self.content_type, 'Host': self._host})
        set(map(lambda x: self.mount(x % self._host,
                                     HTTPAdapter(
                                         pool_connections=options.get('pool_connections',
                                                                      self.DEFAULT_POOL_CONNECTIONS),
                                         pool_maxsize=options.get('pool_maxsize', self.DEFAULT_POOL_MAXSIZE),
                                     )),
                {'https://%s/', 'http://%s/'}))

    @property
    def _credentials(self) -> Dict[str, str]:
        return {
            'client_secret': self._client_secret,
            'client_id': self._client_id,
        }

    @property
    def public_key(self) -> Optional[PUBLIC_KEY_TYPES]:
        """
        Get complete public key of keycloak
        :return:
        """
        if not self._public_key:
            raise ValueError('Public key is not set')
        return serialization.load_der_public_key(b64decode(self._public_key.encode()))

    @public_key.setter
    def public_key(self, value) -> None:
        """
        Set public key from raw value.
        :param value:
        :return:
        """
        self._public_key = value

    def token(self, grant_type: str, **kwargs) -> Dict[str, str]:
        """
        Obtain a access_token and refresh_token pair from Keycloak.

        :param grant_type:
        :param kwargs:
        :return:
        """
        grant_type = self.grant_types.get(grant_type, grant_type)
        request_kwargs = {
            **self._credentials,
            'grant_type': grant_type,
        }

        if grant_type == 'authorization_code':
            request_kwargs.update(**{
                'code': kwargs.get('code'),
                'redirect_uri': make_redirect_uri(api_url=self._api_url,
                                                  endpoints_path_prefix=self._endpoint_path_prefix,
                                                  quote_uri=False)
            })
            if self._code_verifier:
                request_kwargs['code_verifier'] = self._code_verifier
        elif grant_type == 'refresh_token':
            request_kwargs.update(**{
                'refresh_token': kwargs.get('refresh_token')})
        elif grant_type == 'password':
            request_kwargs.update(**{
                'username': kwargs.get('username'),
                'password': kwargs.get('password')})
        elif grant_type == 'client_credentials':
            request_kwargs.update(**{})
        else:
            raise ValueError('Unknown grant_type')
        response = self.post(
            url=KeycloakPath.token_endpoint.format(
                host=self._host,
                realm=self._realm
            ),
            data=request_kwargs
        )
        return response.json()

    def token_verify(self, token) -> KeycloakUser:
        """
        Verify a jwt token, and return the user of that.
        :param token:
        :return: AbstractUserType
        """

        decoded_payload = KeycloakJWT() \
            .decode(jwt=token,
                    key=self.public_key,
                    audience=self._audience,
                    algorithms=self._algorithms,
                    options={'verify_typ': 'Bearer'})
        return self.user_type_cls.from_dict(**decoded_payload)

    def introspect(self, sub) -> Dict[str, str]:
        """
        Token introspection by sub value.
        :param sub:
        :return:
        """
        return self.post(
            self._keycloak_path.token_introspection_endpoint.format(host=self._host, realm=self._realm),
            data={'client_id': self._client_id, 'client_secret': self._client_secret, 'token': sub}).json()

    def authenticate(self, response_type: str, response_mode: str, scope: List[str],
                     redirect_uri: str = None) -> KeycloakPath:
        """
        Generate a authentication uri, uses in redirect
        :param response_type:
        :param response_mode:
        :param scope:
        :param redirect_uri:
        :return:
        """
        assert isinstance(scope, list)
        payload = {
            'response_type': response_type,
            'response_mode': response_mode,
            'scope': ' '.join(scope),
            'redirect_uri': redirect_uri or make_redirect_uri(api_url=self._api_url,
                                                              endpoints_path_prefix=self._endpoint_path_prefix,
                                                              quote_uri=False),
            'nonce': make_nonce(),
            'state': make_state(),
            **self._credentials
        }
        if self._code_verifier:
            code_challenge = CodeChallenge(method=self.DEFAULT_CODE_CHALLENGE_METHOD,
                                           code_verifier=self._code_verifier)
            payload['code_challenge'] = code_challenge.code
            payload['code_challenge_method'] = code_challenge.method

        return self._keycloak_path.authorization_endpoint.format(
            host=self._host,
            realm=self._realm,
            params=urllib.parse.urlencode(payload)
        )

    def logout(self, refresh_token: str) -> bool:
        """
        Logout a user by refresh_token.
        :param refresh_token:
        :return:
        """
        response = self.post(
            self._keycloak_path.end_session_endpoint.format(host=self._host, realm=self._realm),
            data={'refresh_token': refresh_token,
                  'client_id': self._client_id,
                  'client_secret': self._client_secret})

        return response.status_code == 204


class KeycloakConnect(metaclass=MetaSingleton):
    """Proxy class for making main keycloak instance. This class is a singleton."""
    inner_cls = Keycloak
    settings_key = 'KEYCLOAK'

    def __init__(self, settings: Union[APISettings, Dict[str, Any]] = None):
        """

        :param settings: keycloak settings or dict (maybe raw settings from global_settings)
        """
        if not settings:
            settings = getattr(keycloak_settings, self.settings_key)

        keys = ('HOST', 'REALM', 'CLIENT_ID', 'CLIENT_SECRET', 'API_URL', 'ENDPOINT_PATH_PREFIX',
                'CODE_VERIFIER', 'PUBLIC_KEY', 'ENDPOINTS_OVERRIDE', 'OPTIONS')

        _settings = {}
        if isinstance(settings, dict):
            _settings = {x: settings.get(x, None) for x in keys}
        else:
            _settings = {x: getattr(settings, x, None) for x in keys}
        settings = {k.lower(): {_k.lower(): _v for _k, _v in v.items()} if isinstance(v, dict) else v for k, v in
                    _settings.items()}
        for key in ('host', 'realm', 'client_id', 'client_secret'):
            assert settings.get(key), '%s: %s settings variable is not provided' % \
                                      (self.__class__.__name__, key.upper())

        endpoints_override = settings.pop('endpoints_override', {})
        wellknown = endpoints_override.pop('WELLKNOWN_ENDPOINT', None)
        keycloak_path = KeycloakPath()
        if wellknown and all(endpoints_override.values()):
            response: requests.Response = requests.get(wellknown)
            response_json = response.json() if response.status_code == 200 else {}
            for field in fields(keycloak_path):
                setattr(keycloak_path, field.name, response_json.get(field.name, None))
        elif any(endpoints_override.values()):
            keycloak_path.update(**{k.lower(): v for k, v in endpoints_override if v != ''})

        self.__keycloak_connect = self.inner_cls(
            **settings,
            endpoints_override=keycloak_path,
            settings_key=self.settings_key
        )

    def connect(self) -> Keycloak:
        """
        :return: Keycloak instance
        """
        return self.__keycloak_connect
