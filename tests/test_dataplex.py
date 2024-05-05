from uuid import uuid4
from gcp_pal import Dataplex, BigQuery


def test_dataplex():
    success = {}

    lake_name = f"test-lake-{uuid4()}"
    zone_name = f"test-zone-{uuid4()}"
    asset_name = f"test-asset-{uuid4()}"
    zone_path = f"{lake_name}/{zone_name}"
    asset_path = f"{lake_name}/{zone_name}/{asset_name}"
    bq_dataset_name = f"test_dataset_{uuid4().hex}"

    try:
        success[0] = not Dataplex(lake_name).exists()
        success[1] = not Dataplex(zone_path).exists()
        success[2] = not Dataplex(asset_path).exists()
        success[3] = not BigQuery(dataset=bq_dataset_name).exists()

        Dataplex(lake_name).create_lake()
        success[4] = Dataplex(lake_name).exists()

        Dataplex(zone_path).create_zone(zone_type="raw", location_type="single")
        success[5] = Dataplex(zone_path).exists()

        BigQuery(dataset=bq_dataset_name).create()
        success[6] = BigQuery(dataset=bq_dataset_name).exists()

        Dataplex(asset_path).create_asset(
            asset_source=bq_dataset_name,
            asset_type="bigquery",
        )

        success[7] = Dataplex(asset_path).exists()

        got_lake = Dataplex(lake_name).get()
        success[8] = got_lake.name.endswith(lake_name)
        success[9] = Dataplex(lake_name).state() == "ACTIVE"

        got_zone = Dataplex(zone_path).get()
        success[10] = got_zone.name.endswith(zone_name)
        success[11] = Dataplex(zone_path).state() == "ACTIVE"
        success[12] = got_zone.resource_spec.location_type.name == "SINGLE_REGION"

        got_asset = Dataplex(asset_path).get()
        success[13] = got_asset.name.endswith(asset_name)
        success[14] = Dataplex(asset_path).state() == "ACTIVE"
        success[15] = got_asset.resource_spec.name.endswith(
            f"datasets/{bq_dataset_name}"
        )
        success[16] = got_asset.resource_spec.type_.name == "BIGQUERY_DATASET"

        Dataplex(asset_path).delete()
        success[17] = Dataplex(zone_path).exists()
        success[18] = not Dataplex(asset_path).exists()

        Dataplex(zone_path).delete()
        success[19] = not Dataplex(zone_path).exists()
        success[20] = Dataplex(lake_name).exists()

        Dataplex(lake_name).delete()
        success[21] = not Dataplex(lake_name).exists()

        BigQuery(bq_dataset_name).delete()
        success[22] = not BigQuery(bq_dataset_name).exists()

    except Exception as e:
        if "429 Quota exceeded for quota metric" in str(e):
            print("Could not finish test_dataplex due to quota exceeded error.")
        else:
            raise e

    failed = [k for k, v in success.items() if not v]
    assert not failed
