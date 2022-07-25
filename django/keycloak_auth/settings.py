import functools
from contextlib import contextmanager
from typing import Dict, Any, Generator, List

from django.conf import settings
from rest_framework.settings import APISettings

__all__ = ['KEYCLOAK_DEFAULTS', 'KEYCLOAK_GLOBAL_DEFAULTS',
           'KeycloakSettings', 'keycloak_settings', 'patched_settings']

KEYCLOAK_GLOBAL_DEFAULTS: Dict[str, Any] = {
}

KEYCLOAK_DEFAULTS: Dict[str, Any] = {
    'CLIENT_ID': '',
    'CLIENT_SECRET': '',
    'HOST': '',
    'REALM': '',
    'CODE_VERIFIER': '',
    'CODE_CHALLENGE_METHOD': 'S256',
    'API_URL': '',
    'ENDPOINT_PATH_PREFIX': '',
    'PUBLIC_KEY': '',
    'OPTIONS': {
        'AUDIENCE': '',
        'JWT_ALGORITHMS': '',
        'POOL_CONNECTIONS': '',
        'POOL_MAXSIZE': '',
    },
    'ENDPOINTS_OVERRIDE': {
        'WELLKNOWN_ENDPOINT': '',
        'BASE_ENDPOINT': '',
        'AUTHORIZATION_ENDPOINT': '',
        'TOKEN_ENDPOINT': '',
        'TOKEN_INTROSPECTION_ENDPOINT': '',
        'USERINFO_ENDPOINT': '',
        'END_SESSION_ENDPOINT': '',
        'JWKS_URI': ''
    }
}


class SettingsContainer(APISettings):
    _original_settings: Dict[str, Any] = {}
    _modules: List[str] = []

    def apply_patches(self, patches: Dict[str, Any]) -> None:
        for attr, val in patches.items():
            self._original_settings[attr] = getattr(self, attr)
            setattr(self, attr, val)

    def clear_patches(self) -> None:
        for attr, orig_val in self._original_settings.items():
            setattr(self, attr, orig_val)
        self._original_settings = {}

    @property
    def __fields__(self):
        return set(self._user_settings.keys())

    def __repr__(self):
        return '%s settings container. %s' % ('Keycloak', 'Available settings in container: %s' %
                                              self._modules if self._modules else '')


class KeycloakSettings(SettingsContainer):
    _original_settings: Dict[str, Any] = {}

    def __init__(self, user_settings=None, defaults=None, import_strings=None, module=None):
        self._module = module
        for attr in user_settings:
            if attr == 'ENDPOINT_PATH_PREFIX':
                user_settings.update({attr: user_settings[attr].lstrip('/').rstrip('/')})
        super().__init__(user_settings=user_settings, defaults=defaults, import_strings=import_strings)

    def __repr__(self):
        return '%s of %s' % (self.__class__.__name__, self._module)


_keycloak_settings = SettingsContainer(user_settings=getattr(settings, 'KEYCLOAK_SETTINGS', {}),
                                       defaults=KEYCLOAK_GLOBAL_DEFAULTS)


@functools.lru_cache()
def init_settings():
    for module in [md for md in getattr(settings, 'KEYCLOAK_SETTINGS', {}) if md not in KEYCLOAK_GLOBAL_DEFAULTS]:
        _keycloak_settings._modules.append(module)  # noqa # pylint: disable=protected-access
        setattr(_keycloak_settings, module, KeycloakSettings(
            user_settings=getattr(settings, 'KEYCLOAK_SETTINGS', {}).get(module, {}),
            defaults=KEYCLOAK_DEFAULTS,
            module=module,
        ))
    return _keycloak_settings


keycloak_settings = init_settings()


@contextmanager
def patched_settings(patches: Dict[str, Any]) -> Generator[Any, Any, None]:
    """ temporarily patch the global settings """
    if not patches:
        yield
    else:
        try:
            keycloak_settings.apply_patches(patches)
            yield
        finally:
            keycloak_settings.clear_patches()
