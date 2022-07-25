import itertools
import random
import secrets
import string
import urllib.parse
from time import gmtime, strftime

import requests

from keycloak_auth.providers.types import KeycloakPath, CodeVerifier

TIME_FTM = '%Y-%m-%dT%H:%M:%SZ'
NONCE_CHARS = string.ascii_letters + string.digits


def make_nonce(when=None, salt_length=6):
    """
    Make nonce for OAuth2 flow.
    :param when:
    :param salt_length:
    :return:
    """
    if when:
        t = gmtime(when)
    else:
        t = gmtime()

    time_str = strftime(TIME_FTM, t)

    return time_str + ''.join(itertools.starmap(random.SystemRandom().choice,
                                                itertools.repeat((NONCE_CHARS,),
                                                                 salt_length)))


def make_state(salt_length=18):
    """
    Make state for OAuth2 flow.
    :param salt_length:
    :return:
    """
    return ''.join(itertools.starmap(random.SystemRandom().choice, itertools.repeat((NONCE_CHARS,), salt_length)))


def make_redirect_uri(api_url: str, endpoints_path_prefix: str, quote_uri=True) -> str:
    """
    Make redirect uri by keycloak_redirect template
    :param api_url:
    :param endpoints_path_prefix:
    :param quote_uri:
    :return:
    """
    path = KeycloakPath.api_redirect_endpoint.format(
        api_url=api_url,
        endpoint_path_prefix=endpoints_path_prefix
    )
    return urllib.parse.quote_plus(path) if quote_uri else path


def get_code_verifier(length: int = 128) -> CodeVerifier:
    """
    "Lazy" way to generate code verifier. Needs put it into settings.py
    :param length:
    :return:
    """
    return secrets.token_urlsafe(96)[:length]


def get_public_key(host: str, realm: str) -> str:
    """Refresh PEM data from Keycloak realm"""
    response = requests.get(KeycloakPath.base_endpoint.format(host=host,
                                                              realm=realm)).json()
    key = response.get('public_key')
    if not key:
        raise ValueError('No public key in Keycloak response')
    return key
