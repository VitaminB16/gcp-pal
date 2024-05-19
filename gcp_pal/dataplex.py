import os
import threading
from gcp_pal.utils import try_import

from gcp_pal.utils import (
    get_auth_default,
    log,
    get_all_kwargs,
    ClientHandler,
    ModuleHandler,
)


class Dataplex:

    def __init__(
        self,
        path: str = None,
        lake: str = None,
        zone: str = None,
        asset: str = None,
        project: str = None,
        location: str = "europe-west2",
    ):
        """
        Initializes the Dataplex object.

        Args:
        - path (str): The path of the object. The path is in the format `"lake/zone/asset"`, or `"lake/zone"`, `"lake"` or `None`.
                      The path will overwrite the lake, zone and asset attributes.
        - lake (str): The name of the lake.
        - zone (str): The name of the zone.
        - asset (str): The name of the asset.
        - project (str): The project ID. Default is the PROJECT environment variable or the default project from `gcloud config list`.
        - location (str): The location of the Dataplex resources. Default is `"europe-west2"`.
        """

        self.project = project or os.environ.get("PROJECT") or get_auth_default()[1]
        self.location = location

        self.lake = lake
        self.zone = zone
        self.asset = asset

        if isinstance(path, str) and path.startswith("projects/"):
            path = self._convert_full_id_to_path(path)

        try:
            # e.g. path = "lake" or "lake/zone" or "lake/zone/asset"
            self.lake = path.split("/")[0]
        except:
            pass
        try:
            # e.g. path = "lake/zone" or "lake/zone/asset"
            self.zone = path.split("/")[1]
        except:
            pass
        try:
            # e.g. path = "lake/zone/asset"
            self.asset = path.split("/")[2]
        except:
            pass

        self.lake_id = self.lake
        self.zone_id = self.zone
        self.asset_id = self.asset

        self.type = self._get_type()
        self._verify_attributes()
        self.path = self._get_path()
        self.parent = self._get_parent()

        self.dataplex = ModuleHandler("google.cloud").please_import(
            "dataplex_v1", who_is_calling="Dataplex"
        )
        self.types = self.dataplex.types
        self.client = ClientHandler(self.dataplex.DataplexServiceClient).get()
        self.exceptions = ModuleHandler("google.api_core.exceptions").please_import(
            who_is_calling="Dataplex"
        )

    def _refresh_client(self):
        """
        Refresh the client. This is useful when the client caches some data and it needs to be refreshed.
        """
        self.client = ClientHandler(self.dataplex.DataplexServiceClient).get(
            force_refresh=True
        )

    def _get_type(self):
        """
        Get the type of the object. The type is either a lake, zone or an asset.

        Returns:
        - (str): The type of the object.
        """
        if not self.lake and not self.zone and not self.asset:
            return "project"
        elif self.lake and not self.zone and not self.asset:
            return "lake"
        elif self.lake and self.zone and not self.asset:
            return "zone"
        elif self.lake and self.zone and self.asset:
            return "asset"

    def _get_path(self):
        """
        Get the path of the object.

        Returns:
        - (str): The path of the object. Of the form "lake/zone/asset" or "lake/zone" or "lake" or None.
        """
        if self.type == "project":
            path = None
        elif self.type == "lake":
            path = self.lake
        elif self.type == "zone":
            path = f"{self.lake}/{self.zone}"
        elif self.type == "asset":
            path = f"{self.lake}/{self.zone}/{self.asset}"
        return path

    def _verify_attributes(self):
        """
        Verifies that the attributes are valid.
        For example, if the object is an asset, then it must have a lake, a zone and an asset.

        Raises:
        - ValueError: If the attributes are invalid.
        """
        if self.type == "asset" and not (self.lake and self.zone and self.asset):
            raise ValueError(
                "The object is an asset, so the following attributes must be provided: lake, zone, asset."
            )
        elif self.type == "zone" and not (self.lake and self.zone):
            raise ValueError(
                "The object is a zone, so the following attributes must be provided: lake, zone."
            )
        elif self.type == "lake" and not self.lake:
            raise ValueError(
                "The object is a lake, so the following attribute must be provided: lake."
            )

    def _get_parent(self):
        """
        Get the parent resource of the object.

        Returns:
        - (str): The parent resource of the object.
        """
        parent = f"projects/{self.project}/locations/{self.location}"
        if self.type == "lake":
            parent += ""
        elif self.type == "zone":
            parent += f"/lakes/{self.lake}"
        elif self.type == "asset":
            parent += f"/lakes/{self.lake}/zones/{self.zone}"
        return parent

    def _convert_full_id_to_path(self, full_id):
        """
        Converts the full resource ID to the path of the form "lake/zone/asset" or "lake/zone" or "lake" or None.

        Args:
        - full_id (str): The full resource ID.

        Returns:
        - (str): The path of the resource.
        """
        if not full_id:
            return None
        paths = full_id.split("/")[1::2][2:]
        path = "/".join(paths)
        return path

    def get_lake(self):
        """
        Retrieves the lake resource.

        Returns:
        - (dict): The lake resource.
        """
        name = f"{self.parent}/lakes/{self.lake}"
        got_lake = self.client.get_lake(name=name)
        return got_lake

    def get_zone(self):
        """
        Retrieves the zone resource.

        Returns:
        - (dict): The zone resource.
        """
        name = f"{self.parent}/zones/{self.zone}"
        got_zone = self.client.get_zone(name=name)
        return got_zone

    def get_asset(self):
        """
        Retrieves the asset resource.

        Returns:
        - (dict): The asset resource.
        """
        name = f"{self.parent}/assets/{self.asset}"
        got_asset = self.client.get_asset(name=name)
        return got_asset

    def get(self):
        """
        Retrieves the resource.

        Returns:
        - (dict): The resource.
        """
        got_method = {
            "lake": self.get_lake,
            "zone": self.get_zone,
            "asset": self.get_asset,
        }.get(self.type)
        got = got_method()
        return got

    def create_lake(
        self,
        display_name: str = None,
        description: str = None,
        labels: dict = None,
        metadata: dict = {},
        metastore_service: str = None,
        if_exists: str = "ignore",
        **kwargs,
    ):
        """
        Creates a lake resource.

        Args:
        - display_name (str): User-friendly display name of the lake. If not provided, it will be based on the lake_id.
        - description (str): The description of the lake resource.
        - labels (dict): The labels of the lake resource.
        - metadata (dict): The metadata of the lake resource.
        - metastore_service (str): The name of the metastore service.

        Returns:
        - (dict): The lake resource.
        """
        log(f"Dataplex - Creating lake '{self.lake_id}' [location: {self.location}]...")
        metastore = self.types.Lake.Metastore(service=metastore_service)
        lake = self.types.Lake(
            display_name=display_name,
            description=description,
            labels=labels,
            metastore=metastore,
        )
        try:
            created_lake = self.client.create_lake(
                parent=self.parent,
                lake_id=self.lake_id,
                lake=lake,
                timeout=600,
                metadata=metadata,
            )
        except self.exceptions.AlreadyExists as e:
            if if_exists == "ignore":
                log(f"Dataplex - Lake '{self.lake_id}' already exists.")
                return
            raise e
        result = created_lake.result()
        log(f"Dataplex - Lake '{self.lake_id}' created.")
        return result

    def create_zone(
        self,
        zone_type: str,  # "raw" or "curated"
        location_type: str = "single-region",  # "single-region"/"single" or "multi-region"/"multi"
        display_name: str = None,
        description: str = None,
        labels: dict = None,
        metadata: dict = {},
        if_exists: str = "ignore",
        **kwargs,
    ):
        """
        Creates a zone resource in the lake.

        Args:
        - zone_type (str): The type of the zone. Either `"raw"` or `"curated"`.
        - location_type (str): The location type of the zone. Either `"single-region"`/`"single"` or `"multi-region"`/`"multi"`.
        - display_name (str): User-friendly display name of the zone. If not provided, it will be based on the zone_id.
        - description (str): The description of the zone resource.
        - labels (dict): The labels of the zone resource.
        - metadata (dict): The metadata of the zone resource.
        - if_exists (str): If `"ignore"`, the operation will be ignored if the zone already exists.

        Returns:
        - (dict): The zone resource.
        """
        ResourceSpec = self.types.Zone.ResourceSpec
        LocationType = ResourceSpec.LocationType
        ZoneType = self.types.Zone.Type
        log(
            f"Dataplex - Creating zone '{self.zone_id}' [type: {zone_type}, location: {self.location} ({location_type})]..."
        )
        location_type = {
            "single-region": 1,
            "multi-region": 2,
            "single": 1,
            "multi": 2,
        }.get(location_type, 0)
        zone_type = {"raw": 1, "curated": 2}.get(zone_type, 0)
        location_type = LocationType(location_type)
        zone_type = ZoneType(zone_type)
        resource_spec = self.types.Zone.ResourceSpec(location_type=location_type)
        zone = self.types.Zone(
            display_name=display_name,
            description=description,
            labels=labels,
            type_=zone_type,
            resource_spec=resource_spec,
        )
        try:
            created_zone = self.client.create_zone(
                parent=self.parent,
                zone_id=self.zone_id,
                zone=zone,
                timeout=600,
                metadata=metadata,
            )
        except self.exceptions.AlreadyExists as e:
            if if_exists == "ignore":
                log(f"Dataplex - Zone '{self.zone_id}' already exists.")
                return
            raise e

        result = created_zone.result()
        log(f"Dataplex - Zone '{self.zone_id}' created.")
        return result

    def create_asset(
        self,
        asset_source: str,
        asset_type: str = None,
        display_name: str = None,
        description: str = None,
        labels: dict = None,
        metadata: dict = {},
        if_exists: str = "ignore",
        **kwargs,
    ):
        """
        Creates an asset resource in the zone in the lake.

        Args:
        - asset_source (str): The source of the asset. Either a bucket name or a dataset name.
        - asset_type (str): The type of the asset. Either `"storage"` or `"bigquery"`.
        - display_name (str): User-friendly display name of the asset. If not provided, it will be based on the asset_id.
        - description (str): The description of the asset resource.
        - labels (dict): The labels of the asset resource.
        - metadata (dict): The metadata of the asset resource.

        Returns:
        - (dict): The asset resource.
        """
        if not asset_type or asset_type not in ["storage", "bigquery"]:
            raise ValueError(
                "asset_type must be provided: Either 'storage' or 'bigquery'."
            )
        log(
            f"Dataplex - Creating asset '{self.asset_id}' [source: {asset_source}, type: {asset_type}]..."
        )
        ResourceSpec = self.types.Asset.ResourceSpec
        ResourceType = ResourceSpec.Type
        path_bit = {"storage": "buckets", "bigquery": "datasets"}.get(asset_type)
        asset_type = {"storage": 1, "bigquery": 2}.get(asset_type, 0)
        asset_type = ResourceType(asset_type)
        if not asset_source.startswith("projects/"):
            asset_source = f"projects/{self.project}/{path_bit}/{asset_source}"
        resource_spec = ResourceSpec(name=asset_source, type_=asset_type)
        asset = self.types.Asset(
            display_name=display_name,
            description=description,
            labels=labels,
            resource_spec=resource_spec,
        )
        try:
            created_asset = self.client.create_asset(
                parent=self.parent,
                asset_id=self.asset_id,
                asset=asset,
                timeout=600,
                metadata=metadata,
            )
        except self.exceptions.AlreadyExists as e:
            if if_exists == "ignore":
                log(f"Dataplex - Asset '{self.asset_id}' already exists.")
                return
            raise e
        result = created_asset.result()
        log(f"Dataplex - Asset '{self.asset_id}' created.")
        return result

    def create_parents(self, **kwargs):
        """
        Checks that the parent resources exist. If not, they will be created.

        Returns:
        - (bool): True if the operation was successful.
        """
        # We do not want to pass the redundant kwargs to the create methods of
        # the parent resources.
        redundant_kwargs = ["description", "display_name", "labels", "metadata"]
        kwargs = {k: v for k, v in kwargs.items() if k not in redundant_kwargs}
        if self.type == "lake":
            return True
        elif self.type == "zone":
            parent_lake = Dataplex(
                lake=self.lake,
                project=self.project,
                location=self.location,
            )
            if not parent_lake.exists():
                log(f"Dataplex - Parent lake '{self.lake}' does not exist.")
                parent_lake.create_lake(**kwargs)
        elif self.type == "asset":
            parent_lake = Dataplex(
                lake=self.lake,
                project=self.project,
                location=self.location,
            )
            if not parent_lake.exists():
                log(f"Dataplex - Parent lake '{self.lake}' does not exist.")
                parent_lake.create_lake(**kwargs)
            parent_zone = Dataplex(
                lake=self.lake,
                zone=self.zone,
                project=self.project,
                location=self.location,
            )
            if not parent_zone.exists():
                log(f"Dataplex - Parent zone '{self.zone}' does not exist.")
                parent_zone.create_zone(**kwargs)
        return True

    def create(
        self,
        asset_source: str,
        asset_type: str,
        zone_type: str = None,
        location_type: str = None,
        display_name: str = None,
        description: str = None,
        labels: dict = None,
        metadata: dict = {},
        **kwargs,
    ):
        """
        Creates the resource. If the lake doesn't exist, it will be created first. Then the zone and the assets will be created.

        Args:
        - asset_source (str): The source of the asset. Either a bucket name or a dataset name.
        - asset_type (str): The type of the asset. Either `"storage"` or `"bigquery"`. Has to be provided if the resource is a zone or an asset.
        - zone_type (str): The type of the zone. Either `"raw"` or `"curated"`. Has to be provided if the resource is a zone or an asset.
        - location_type (str): The location type of the zone. Either `"single-region"` or `"multi-region"`.
        - display_name (str): User-friendly display name of the resource. If not provided, it will be based on the resource_id.
        - description (str): The description of the resource.
        - labels (dict): The labels of the resource.
        - metadata (dict): The metadata of the resource.
        """
        all_kwargs = get_all_kwargs(locals())
        self.create_parents(**all_kwargs)
        if self.type == "lake":
            return self.create_lake(
                display_name=display_name,
                description=description,
                labels=labels,
                metadata=metadata,
            )
        elif self.type == "zone":
            return self.create_zone(
                zone_type=zone_type,
                location_type=location_type,
                display_name=display_name,
                description=description,
                labels=labels,
                metadata=metadata,
            )
        elif self.type == "asset":
            return self.create_asset(
                asset_source=asset_source,
                asset_type=asset_type,
                display_name=display_name,
                description=description,
                labels=labels,
                metadata=metadata,
            )

    def ls_lakes(self, name=None, full_id=False):
        """
        Lists the lakes in the project.

        Args:
        - name (str): Optional path of the lake.
        - full_id (bool): If True, returns the full resource ID of the lakes.

        Returns:
        - (list): The list of lakes.
        """
        parent = name or self.parent
        lakes = self.client.list_lakes(parent=parent)
        output = [lake.name for lake in lakes]
        if not full_id:
            output = [self._convert_full_id_to_path(name) for name in output]
        return output

    def ls_zones(self, name=None, full_id=False):
        """
        Lists the zones in the lake.

        Args:
        - name (str): Optional path of the zone.
        - full_id (bool): If True, returns the full resource ID of the zones. Otherwise returns the path of form "lake/zone".

        Returns:
        - (list): The list of zones.
        """
        output = []
        if self.type == "project":
            # Listing all zones in the project
            zones = []
            lakes = self.ls_lakes(full_id=True)
            for lake in lakes:
                lake = Dataplex(path=lake)
                zones = lake.ls_zones(full_id=True)
                output += zones
        else:
            parent = name or f"{self.parent}/lakes/{self.lake}"
            zones = self.client.list_zones(parent=parent)
            output = [zone.name for zone in zones]
        if not full_id:
            output = [self._convert_full_id_to_path(name) for name in output]
        return output

    def ls_assets(self, name=None, full_id=False):
        """
        Lists the assets in the zone.

        Args:
        - name (str): Optional path of the asset.
        - full_id (bool): If True, returns the full resource ID of the assets. Otherwise returns the path of form "lake/zone/asset".

        Returns:
        - (list): The list of assets.
        """
        output = []
        if self.type == "project":
            # Listing all assets in the project
            assets = []
            lakes = self.ls_lakes(full_id=True)
            for lake in lakes:
                lake = Dataplex(path=lake)
                zones = lake.ls_zones(full_id=True)
                for zone in zones:
                    zone = Dataplex(path=zone)
                    assets = zone.ls_assets(full_id=True)
                    output += assets
        elif self.type == "lake":
            # Listing all assets in the lake
            zones = self.ls_zones(full_id=True)
            for zone in zones:
                zone = Dataplex(path=zone)
                assets = zone.ls_assets(full_id=True)
                output += assets
        else:
            parent = name or f"{self.parent}/zones/{self.zone}"
            assets = self.client.list_assets(parent=parent)
            output = [asset.name for asset in assets]
        if not full_id:
            output = [self._convert_full_id_to_path(name) for name in output]
        return output

    def ls(self, level=None, full_id=False):
        """
        Lists the resources.

        Args:
        - level (str): The level of the resources to list. Either "lakes", "zones" or "assets".
        - full_id (bool): If True, returns the full resource ID of the resources.

        Returns:
        - (list): The list of resources.
        """
        if self.type == "asset":
            raise ValueError("The method 'Dataplex.ls' is not supported for assets.")
        if level:
            list_method = {
                "lakes": self.ls_lakes,
                "zones": self.ls_zones,
                "assets": self.ls_assets,
            }.get(level)
        else:
            list_method = {
                "project": self.ls_lakes,
                "lake": self.ls_zones,
                "zone": self.ls_assets,
            }.get(self.type)
        output = list_method(full_id=full_id)
        return output

    def exists(self):
        """
        Checks if the resource exists.

        Returns:
        - (bool): True if the resource exists, False otherwise.
        """
        try:
            self.get()
            return True
        except:
            return False

    def delete_lake(self, name: str = None, errors="ignore", wait_to_complete=True):
        """
        Deletes the lake resource.

        Args:
        - name (str): The full resource ID of the lake.

        Returns:
        - (dict): The response of the delete operation.
        """
        name = name or f"{self.parent}/lakes/{self.lake}"
        try:
            output = self.client.delete_lake(name=name)
        except self.exceptions.FailedPrecondition as e:
            msg = (
                f"Dataplex - Lake '{self.lake}' is not empty. Deleting all its zones..."
            )
            log(msg)
            lake = Dataplex(
                lake=self.lake, project=self.project, location=self.location
            )
            lake_assets = lake.ls_assets(full_id=True)
            for asset in lake_assets:
                Dataplex(path=asset).delete()
            zones = lake.ls_zones(full_id=True)
            for zone in zones:
                Dataplex(path=zone).delete()
            self._refresh_client()
            output = self.client.delete_lake(name=name)
        except self.exceptions.NotFound as e:
            if errors == "ignore":
                log(f"Dataplex - Lake '{self.lake}' does not exist to delete.")
                return
            raise e
        if wait_to_complete:
            output = output.result(timeout=600)
        log(f"Dataplex - Lake deleted: '{self.lake}'.")
        return output

    def delete_zone(self, name: str = None, errors="ignore", wait_to_complete=True):
        """
        Deletes the zone resource.

        Args:
        - name (str): The full resource ID of the zone.

        Returns:
        - (dict): The response of the delete operation.
        """
        name = name or f"{self.parent}/zones/{self.zone}"
        log(f"Dataplex - Deleting zone: '{self.path}'...")
        try:
            output = self.client.delete_zone(name=name)
        except self.exceptions.FailedPrecondition as e:
            msg = f"Dataplex - Zone '{self.path}' is not empty. Deleting all its assets..."
            log(msg)
            zone = Dataplex(
                lake=self.lake,
                zone=self.zone,
                project=self.project,
                location=self.location,
            )
            self._delete_parallel(zone.ls_assets(full_id=True))
            self._refresh_client()
            output = self.client.delete_zone(name=name)
        except self.exceptions.NotFound as e:
            if errors == "ignore":
                log(f"Dataplex - Zone '{self.path}' does not exist to delete.")
                return
            raise e
        if wait_to_complete:
            output = output.result(timeout=600)
        log(f"Dataplex - Zone deleted: '{self.path}'.")
        return output

    def delete_asset(self, name: str = None, errors="ignore", wait_to_complete=True):
        """
        Deletes the asset resource.

        Args:
        - name (str): The full resource ID of the asset.
        - wait_to_complete (bool): If True, waits until the operation is completed.

        Returns:
        - (dict): The response of the delete operation.
        """
        name = name or f"{self.parent}/assets/{self.asset}"
        log(f"Dataplex - Deleting asset: '{self.path}'...")
        try:
            output = self.client.delete_asset(name=name)
        except self.exceptions.NotFound as e:
            if errors == "ignore":
                log(f"Dataplex - Asset '{self.path}' does not exist to delete.")
                return
            raise e
        if wait_to_complete:
            output = output.result(timeout=600)
        log(f"Dataplex - Asset deleted: '{self.path}'.")
        return output

    def delete_lake(self, name: str = None, errors="ignore", wait_to_complete=True):
        """
        Deletes the lake resource using parallel deletion for assets and zones.

        Args:
        - name (str): The full resource ID of the lake.

        Returns:
        - (dict): The response of the delete operation.
        """
        name = name or f"{self.parent}/lakes/{self.lake}"
        try:
            output = self.client.delete_lake(name=name)
        except self.exceptions.FailedPrecondition:
            log(
                f"Dataplex - Lake '{self.lake}' is not empty. Deleting all its zones and assets..."
            )
            lake = Dataplex(
                lake=self.lake, project=self.project, location=self.location
            )

            # Deleting all assets in all zones in the lake
            self._delete_parallel(lake.ls_assets(full_id=True))

            # Deleting zones in the lake
            self._delete_parallel(lake.ls_zones(full_id=True))

            self._refresh_client()
            output = self.client.delete_lake(name=name)
        except self.exceptions.NotFound:
            if errors == "ignore":
                log(f"Dataplex - Lake '{self.lake}' does not exist to delete.")
                return
            raise
        if wait_to_complete:
            output = output.result(timeout=600)
        log(f"Dataplex - Lake deleted: '{self.lake}'.")
        return output

    def _delete_parallel(self, items):
        """
        Deletes items in parallel using threading.

        Args:
        - items (list): List of full resource IDs of assets or zones to delete.
        """
        threads = []
        for item in items:
            thread = threading.Thread(
                target=lambda item=item: Dataplex(path=item).delete()
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def delete(self, name: str = None, errors="ignore", wait_to_complete=True):
        """
        Deletes the resource.

        Args:
        - name (str): The full resource ID of the resource.
        - errors (str): If "ignore", the operation will be ignored if the resource does not exist.
        - wait_to_complete (bool): If True, waits until the operation is completed.

        Returns:
        - (dict): The response of the delete operation.
        """
        delete_method = {
            "lake": self.delete_lake,
            "zone": self.delete_zone,
            "asset": self.delete_asset,
        }.get(self.type)
        output = delete_method(
            name=name, errors=errors, wait_to_complete=wait_to_complete
        )
        return output

    def state(self):
        """
        Retrieves the state of the resource.

        Returns:
        - (str): The state of the resource.
        """
        state = self.get().state.name
        return state


if __name__ == "__main__":
    # Example: Create a Dataplex object
    from gcp_pal import BigQuery

    lake_name = "artem-lake1"
    zone_name = "artem-zone1"
    # Dataplex(path=lake_name).create_lake()
    # Dataplex(path=f"{lake_name}/{zone_name}").create_zone(
    #     zone_type="raw", location_type="single-region"
    # )
    # BigQuery(dataset="dataplex_dataset1").create()
    # BigQuery(dataset="dataplex_dataset2").create()
    # Dataplex(
    #     path=f"{lake_name}/{zone_name}/test-asset1",
    # ).create_asset(
    #     asset_source="dataplex_dataset1",
    #     asset_type="bigquery",
    # )
    # Dataplex(
    #     path=f"{lake_name}/{zone_name}/test-asset2",
    # ).create_asset(
    #     asset_source="dataplex_dataset2",
    #     asset_type="bigquery",
    # )

    Dataplex(lake_name, location="europe-west2").delete()
