from django.core import checks


@checks.register(checks.Tags.security)
def code_verifier_check(app_configs, **kwargs): # noqa  # pylint: disable=unused-argument
    errors = []
    from keycloak_auth.settings import keycloak_settings  # pylint: disable=import-outside-toplevel
    for module in keycloak_settings._modules: # noqa # pylint: disable=protected-access
        code_verifier = getattr(keycloak_settings, module).CODE_VERIFIER
        if not code_verifier:
            errors.append(
                checks.Warning(
                    "CODE_VERIFIER parameter is empty for %s provider,"
                    " without that, auth security will be decreased." % module,
                    hint="Use the keycloak_auth.utils.get_code_verifier() method for generate that.",
                    id="keycloak_auth.W001"
                )
            )
        if code_verifier and (len(code_verifier) > 128 or len(code_verifier) < 128):
            errors.append(
                checks.Warning(
                    "CODE_VERIFIER parameter is not valid for %s provider,"
                    " it should be 128 characters long." % module,
                    hint="Use the keycloak_auth.utils.get_code_verifier() method for generate that.",
                    id="keycloak_auth.W002"
                )
            )
    return errors
