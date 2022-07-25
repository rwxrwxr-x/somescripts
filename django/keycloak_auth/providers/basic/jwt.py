from typing import Union, List

from jwt import PyJWT, InvalidIssuedAtError

from keycloak_auth.providers.types import URL


class KeycloakJWT(PyJWT):
    """
    Extend the PyJWT._validate_claims method to validate the 'typ' and 'azp' claim.
    """
    def _validate_claims(self, payload: dict, options: dict, audience: Union[List[str], str] = None,
                         issuer: URL = None,
                         leeway: int = 0, **kwargs) -> None:
        super(KeycloakJWT, self)._validate_claims(payload, options, audience, issuer, leeway, **kwargs) # noqa
        if options.get('verify_typ'):
            self._validate_typ(payload, options.get('verify_typ'))

        if options.get('verify_azp'):
            self._validate_azp(payload, options.get('verify_azp'))

    @staticmethod
    def _validate_typ(payload, typ) -> None:
        if payload.get('typ') != typ:
            raise InvalidIssuedAtError('Incorrect typ token value')

    @staticmethod
    def _validate_azp(payload, azp) -> None:
        if payload.get('azp') != azp:
            raise InvalidIssuedAtError('Incorrect azp token value')
