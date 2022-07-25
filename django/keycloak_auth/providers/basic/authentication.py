from jwt import ExpiredSignatureError, InvalidAudienceError, InvalidIssuer, InvalidIssuedAtError
from rest_framework import status
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed

from keycloak_auth import Registry
from keycloak_auth.providers.basic import KeycloakConnect

__all__ = ['AuthenticationError', 'KeycloakAuthentication']


class AuthenticationError(AuthenticationFailed):
    """
    Keycloak Authentication exception. Raised when the token verification fails.
    :param kwargs: status_code, detail
    """
    def __init__(self, **kwargs):
        super().__init__()
        self.status_code = kwargs.get('status_code')
        self.detail = kwargs.get('detail')


class KeycloakAuthentication(BaseAuthentication):
    """Base authentication class for Keycloak, overloads the authenticate method to made what you need."""
    token_type = 'Bearer'
    provider = 'KEYCLOAK'
    provider_connect = KeycloakConnect
    www_authenticate_realm = 'api'

    def authenticate_header(self, request):
        return '{} realm="{}"'.format(
            'Bearer',
            self.www_authenticate_realm,
        )

    def authenticate(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED,
                                      detail='Authorization header is not provided ')
        if self.token_type not in auth_header:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED,
                                      detail='Incorrect token type')

        try:
            access_token = auth_header.replace('%s ' % self.token_type, '')
            user = Registry().register(self.provider, self.provider_connect).token_verify(access_token)
        except ExpiredSignatureError:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED, detail='Token expired')
        except InvalidAudienceError:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED, detail='Audience not match')
        except InvalidIssuer:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED, detail='Issuer not match')
        except InvalidIssuedAtError as exc:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED, detail=exc.__str__())
        except Exception as exc:
            raise AuthenticationError(status_code=status.HTTP_401_UNAUTHORIZED, detail=exc.__str__())

        request.user = user
        request.user.is_authenticated = True
        return request.user, access_token
