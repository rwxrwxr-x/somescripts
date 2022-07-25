from typing import Dict, Type

from keycloak_auth.providers.abstract import MetaSingleton, Connect, BaseKeyCloak

__all__ = ['Registry']


class Registry(metaclass=MetaSingleton):
    """
    Registry of keycloak providers.
    This is a singleton class, so it initialize all providers by first call (for example — in views)

    Example:
        Registry().CIP.token(args, kw)


    """
    _registry: Dict[str, Connect] = {}

    def register(self, name: str, module: Type[Connect], settings: dict = None) -> Type[BaseKeyCloak]:
        """
        Register provider by name and put it to registry
        :param name:
        :param module:
        :param settings:
        :return:
        """
        if name not in self._registry:
            if settings:
                initiated = module(settings).connect()
            else:
                initiated = module().connect()
            self._registry[name] = initiated
            setattr(self, name, self._registry[name])
        return self._registry[name]

    def unregister(self, name: str) -> None:
        """
        Unregister provider by name and remove it from registry
        :param name:
        :return:
        """
        del self._registry[name]
        delattr(self, name)

    def __getattr__(self, item: str) -> Type[BaseKeyCloak]:
        return self.__dict__.get(item)
