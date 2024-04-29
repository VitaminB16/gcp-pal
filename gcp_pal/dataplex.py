import os
from gcp_pal.utils import try_import

try_import("google.cloud.dataplex_v1", "Dataplex")
from google.cloud.dataplex_v1 import DataplexServiceClient
from google.cloud.dataplex_v1.types import Lake, Zone, Asset

from gcp_pal.utils import get_auth_default, log, get_all_kwargs


class Dataplex:

    _clients = {}

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
        self.parent = self._get_parent()

        if self.project in Dataplex._clients:
            self.client = Dataplex._clients[self.project]
        else:
            self.client = DataplexServiceClient()
            Dataplex._clients[self.project] = self.client

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
        metastore = Lake.Metastore(service=metastore_service)
        lake = Lake(
            display_name=display_name,
            description=description,
            labels=labels,
            metastore=metastore,
        )
        created_lake = self.client.create_lake(
            parent=self.parent,
            lake_id=self.lake_id,
            lake=lake,
            timeout=600,
            metadata=metadata,
        )
        result = created_lake.result()
        log(f"Dataplex - Lake '{self.lake_id}' created.")
        return result

    def create_zone(
        self,
        zone_type: str = "raw",  # or "curated"
        location_type: str = "single-region",  # or "multi-region"
        display_name: str = None,
        description: str = None,
        labels: dict = None,
        metadata: dict = {},
        **kwargs,
    ):
        """
        Creates a zone resource in the lake.

        Args:
        - display_name (str): User-friendly display name of the zone. If not provided, it will be based on the zone_id.
        - description (str): The description of the zone resource.
        - labels (dict): The labels of the zone resource.
        - metadata (dict): The metadata of the zone resource.

        Returns:
        - (dict): The zone resource.
        """
        ResourceSpec = Zone.ResourceSpec
        LocationType = ResourceSpec.LocationType
        ZoneType = Zone.Type
        log(
            f"Dataplex - Creating zone '{self.zone_id}' [type: {zone_type}, location: {self.location} ({location_type})]..."
        )
        location_type = {"single-region": 1, "multi-region": 2}.get(location_type, 0)
        zone_type = {"raw": 1, "curated": 2}.get(zone_type, 0)
        location_type = LocationType(location_type)
        zone_type = ZoneType(zone_type)
        resource_spec = Zone.ResourceSpec(location_type=location_type)
        zone = Zone(
            display_name=display_name,
            description=description,
            labels=labels,
            type_=zone_type,
            resource_spec=resource_spec,
        )
        created_zone = self.client.create_zone(
            parent=self.parent,
            zone_id=self.zone_id,
            zone=zone,
            timeout=600,
            metadata=metadata,
        )
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
        ResourceSpec = Asset.ResourceSpec
        ResourceType = ResourceSpec.Type
        path_bit = {"storage": "buckets", "bigquery": "datasets"}.get(asset_type)
        asset_type = {"storage": 1, "bigquery": 2}.get(asset_type, 0)
        asset_type = ResourceType(asset_type)
        if not asset_source.startswith("projects/"):
            asset_source = f"projects/{self.project}/{path_bit}/{asset_source}"
        resource_spec = ResourceSpec(name=asset_source, type_=asset_type)
        asset = Asset(
            display_name=display_name,
            description=description,
            labels=labels,
            resource_spec=resource_spec,
        )
        created_asset = self.client.create_asset(
            parent=self.parent,
            asset_id=self.asset_id,
            asset=asset,
            timeout=600,
            metadata=metadata,
        )
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
        - asset_type (str): The type of the asset. Either `"storage"` or `"bigquery"`.
        - zone_type (str): The type of the zone. Either `"raw"` or `"curated"`.
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

    def ls_lakes(self, full_id=False):
        """
        Lists the lakes in the project.

        Args:
        - full_id (bool): If True, returns the full resource ID of the lakes.

        Returns:
        - (list): The list of lakes.
        """
        lakes = self.client.list_lakes(parent=self.parent)
        output = [lake.name for lake in lakes]
        if not full_id:
            output = [name.split("/")[-1] for name in output]
        return output

    def ls_zones(self, full_id=False):
        """
        Lists the zones in the lake.

        Args:
        - full_id (bool): If True, returns the full resource ID of the zones. Otherwise returns the path of form "lake/zone".

        Returns:
        - (list): The list of zones.
        """
        parent = f"{self.parent}/lakes/{self.lake}"
        zones = self.client.list_zones(parent=parent)
        output = [zone.name for zone in zones]
        if not full_id:
            zones = [name.split("/")[-1] for name in output]
            output = [f"{self.lake}/{zone}" for zone in zones]
        return output

    def ls_assets(self, full_id=False):
        """
        Lists the assets in the zone.

        Args:
        - full_id (bool): If True, returns the full resource ID of the assets. Otherwise returns the path of form "lake/zone/asset".

        Returns:
        - (list): The list of assets.
        """
        parent = f"{self.parent}/zones/{self.zone}"
        assets = self.client.list_assets(parent=parent)
        output = [asset.name for asset in assets]
        if not full_id:
            assets = [name.split("/")[-1] for name in output]
            output = [f"{self.lake}/{self.zone}/{asset}" for asset in assets]
        return output

    def ls(self, full_id=False):
        """
        Lists the resources.

        Args:
        - full_id (bool): If True, returns the full resource ID of the resources.

        Returns:
        - (list): The list of resources.
        """
        if self.type == "asset":
            raise ValueError("The method 'Dataplex.ls' is not supported for assets.")
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


if __name__ == "__main__":
    # Example: Create a Dataplex object
    from gcp_pal import BigQuery

    # Dataplex(path="test-lake1").create_lake()
    # Dataplex(path="test-lake1/test-zone1").create_zone()
    BigQuery(dataset="dataplex_dataset2").create()
    Dataplex(
        path="test-lake2/test-zone2/test-asset2",
    ).create(
        asset_source="dataplex_dataset2",
        asset_type="bigquery",
    )
