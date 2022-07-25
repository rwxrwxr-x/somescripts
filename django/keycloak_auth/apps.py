from django.apps import AppConfig


class AuthAppConfig(AppConfig):
    name = 'keycloak_auth'

    def ready(self):
        # Add System checks
        from keycloak_auth.checks import code_verifier_check  # pylint: disable=import-outside-toplevel,unused-import # noqa
