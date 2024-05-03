import importlib
from gcp_pal.utils import try_import


class ModuleHandler:
    """
    Class to handle module import and instantiation.
    """

    _imported = {}

    def __init__(self, module_name: str = None):
        """
        Initialize the ModuleHandler.

        Args:
        - module_name: The name of the module.
        """
        self.module_name = module_name

    def please_import(
        self,
        thing: str = None,
        who_is_calling: str = None,
        errors: str = "raise",
    ):
        """
        Import the module.

        Args:
        - thing: The name of the thing to import (if any). If None, import the module.
        - who_is_calling: The name of the function/class that is calling this method (for information only).
        """
        full_name = self.module_name
        if thing is not None:
            full_name += f".{thing}"

        if full_name in ModuleHandler._imported:
            # Load the module from the cache:
            return ModuleHandler._imported[full_name]

        # Try to import the module. This will show the missing dependencies.
        try_import(full_name, who_is_calling, errors)

        # If no errors, we can import the module (this is fast):
        try:
            module = importlib.import_module(full_name)
        except ModuleNotFoundError:
            module = importlib.import_module(self.module_name)
            if thing is not None:
                module = getattr(module, thing)

        # Save the imported module to the cache:
        ModuleHandler._imported[full_name] = module

        return module


if __name__ == "__main__":
    bq_client1 = ModuleHandler("google.cloud.bigquery").please_import("Client")()
