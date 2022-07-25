import urllib.parse

from django.http import HttpResponseRedirect
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet, mixins
from core.logger import logger

from keycloak_auth import Registry
from keycloak_auth.providers.basic import KeycloakConnect
from keycloak_auth.providers.basic.authentication import KeycloakAuthentication
from keycloak_auth.settings import keycloak_settings


class AuthViewSet(mixins.RetrieveModelMixin, GenericViewSet):
    permission_classes = [AllowAny]
    authentication_classes = []
    client = Registry().register('X5ID', X5IDConnect)

    def retrieve(self, request, *args, **kwargs):
        """
        Redirect to login page.
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        return HttpResponseRedirect(self.client.authenticate(response_type='code',
                                                             response_mode='query',
                                                             scope=keycloak_settings.X5ID.OPTIONS.get('SCOPE'), ))

    @action(methods=['POST'], url_path='logout', detail=False)
    def logout(self, request):

        is_logout = self.client.logout(refresh_token=request.data.get('refresh_token'))
        if is_logout:
            logger.info(event='logout succeed')
            ret = Response(status=status.HTTP_204_NO_CONTENT)
        else:
            logger.info(event='incorrect logout', payload__refresh_token=request.data.get('refresh_token'))
            ret = Response({'error': 'logout failed'}, status=status.HTTP_400_BAD_REQUEST)
        return ret

    @action(methods=['GET'], url_path='token', detail=False)
    def token(self, request):
        """
        Callback from X5ID. Obtain access token and refresh token by code.
        :param request:
        :return:
        """
        result = self.client.token(grant_type='access', code=request.GET.get('code'))
        logger.info(event='keycloak obtain token', payload__code=request.GET.get('code'))
        return HttpResponseRedirect('{callback_url}?{params}'.format(
            callback_url=keycloak_settings.X5ID.OPTIONS.get('SPA_CALLBACK_URL'),
            params=urllib.parse.urlencode(result)))

    @token.mapping.post
    def refresh_token(self, request):
        """
        Obtain new pair of access token and refresh token by refresh token.
        :param request:
        :return:
        """
        token = request.data.get('refresh_token')
        result = self.client.token(grant_type='refresh', refresh_token=token)
        logger.info(event='keycloak refresh token', payload__refresh_token=token)
        return Response(result)


class UserViewSet(mixins.RetrieveModelMixin, GenericViewSet):
    permission_classes = [AllowAny]
    authentication_classes = [KeycloakAuthentication]
    client = Registry().register('X5ID', KeycloakConnect)

    @action(methods=['GET'], url_path='introspect', detail=False, authentication_classes=[KeycloakAuthentication])
    def introspect(self, request):
        """
        Introspect access token.
        :param request:
        :return:
        """
        response = self.client.introspect(request.get('access_token'))
        return Response(response)

    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve user info.
        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        response = self.client.get_user(request.get('access_token'))
        return Response(response)
