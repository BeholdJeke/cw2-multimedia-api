import json
import os
import base64
import uuid
from datetime import datetime, timezone
from azure.data.tables import TableServiceClient, UpdateMode
from datetime import timedelta

from azure.storage.blob import generate_blob_sas, BlobSasPermissions


import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="media", methods=["POST"])
def media_upload(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()

        user_id = body.get("user_id")
        caption = body.get("caption", "")
        filename = body.get("filename", "upload.bin")
        content_type = body.get("content_type", "application/octet-stream")
        data_b64 = body.get("data_base64")

        if not user_id or not data_b64:
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields: user_id, data_base64"}),
                status_code=400,
                mimetype="application/json"
            )

        file_bytes = base64.b64decode(data_b64)

        media_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()

        conn_str = os.environ["AzureWebJobsStorage"]
        container_name = os.environ.get("BLOB_CONTAINER", "media")
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        # ---- Blob upload ----
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(container_name)
        try:
            container.create_container()
        except Exception:
            pass

        blob_name = f"{user_id}/{media_id}-{filename}"
        blob_client = container.get_blob_client(blob_name)
        blob_client.upload_blob(
            file_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type)
        )

        blob_url = blob_client.url

        # ---- Table metadata ----
        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)
        try:
            table.create_table()
        except Exception:
            pass

        entity = {
            "PartitionKey": user_id,
            "RowKey": media_id,
            "filename": filename,
            "contentType": content_type,
            "caption": caption,
            "blobName": blob_name,
            "blobUrl": blob_url,
            "createdAt": created_at
        }
        table.upsert_entity(entity)

        return func.HttpResponse(
            json.dumps({"id": media_id, "user_id": user_id, "blobUrl": blob_url}),
            status_code=201,
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

        
@app.route(route="media", methods=["GET"])
def media_list(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        user_id = req.params.get("user_id")  # optional filter

        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)

        items = []
        if user_id:
            # Filter by PartitionKey (user_id)
            query = f"PartitionKey eq '{user_id}'"
            entities = table.query_entities(query_filter=query)
        else:
            # List everything (ok for coursework demo)
            entities = table.list_entities()

        for e in entities:
            items.append({
                "user_id": e["PartitionKey"],
                "id": e["RowKey"],
                "filename": e.get("filename"),
                "contentType": e.get("contentType"),
                "caption": e.get("caption"),
                "blobName": e.get("blobName"),
                "blobUrl": e.get("blobUrl"),
                "createdAt": e.get("createdAt")
            })

        return func.HttpResponse(
            json.dumps(items),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

        
@app.route(route="media/{user_id}/{media_id}", methods=["DELETE"])
def media_delete(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        container_name = os.environ.get("BLOB_CONTAINER", "media")
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        user_id = req.route_params.get("user_id")
        media_id = req.route_params.get("media_id")

        # Table client
        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)

        # Get entity to find blobName
        entity = table.get_entity(partition_key=user_id, row_key=media_id)
        blob_name = entity.get("blobName")

        # Delete blob
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        container = blob_service.get_container_client(container_name)
        if blob_name:
            container.get_blob_client(blob_name).delete_blob()

        # Delete entity
        table.delete_entity(partition_key=user_id, row_key=media_id)

        return func.HttpResponse(
            json.dumps({"deleted": True, "user_id": user_id, "id": media_id}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

        
@app.route(route="media/{user_id}/{media_id}", methods=["PUT"])
def media_update(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        user_id = req.route_params.get("user_id")
        media_id = req.route_params.get("media_id")

        body = req.get_json()
        caption = body.get("caption")
        filename = body.get("filename")  # optional

        if caption is None and filename is None:
            return func.HttpResponse(
                json.dumps({"error": "Provide at least one field to update: caption or filename"}),
                mimetype="application/json",
                status_code=400
            )

        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)

        entity = table.get_entity(partition_key=user_id, row_key=media_id)

        if caption is not None:
            entity["caption"] = caption
        if filename is not None:
            entity["filename"] = filename

        table.upsert_entity(entity=entity, mode=UpdateMode.MERGE)

        return func.HttpResponse(
            json.dumps({"updated": True, "user_id": user_id, "id": media_id}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="media/{user_id}/{media_id}", methods=["GET"])
def media_get_one(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        user_id = req.route_params.get("user_id")
        media_id = req.route_params.get("media_id")

        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)

        entity = table.get_entity(partition_key=user_id, row_key=media_id)

        result = {
            "user_id": entity["PartitionKey"],
            "id": entity["RowKey"],
            "filename": entity.get("filename"),
            "contentType": entity.get("contentType"),
            "caption": entity.get("caption"),
            "blobName": entity.get("blobName"),
            "blobUrl": entity.get("blobUrl"),
            "createdAt": entity.get("createdAt")
        }

        return func.HttpResponse(
            json.dumps(result),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        # If not found, Table SDK usually throws a ResourceNotFoundError
        msg = str(e)
        status = 404 if "ResourceNotFound" in msg or "NotFound" in msg else 500

        return func.HttpResponse(
            json.dumps({"error": msg}),
            mimetype="application/json",
            status_code=status
        )


@app.route(route="media/{user_id}/{media_id}/sas", methods=["GET"])
def media_get_sas(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        container_name = os.environ.get("BLOB_CONTAINER", "media")
        table_name = os.environ.get("TABLE_NAME", "MediaMetadata")

        user_id = req.route_params.get("user_id")
        media_id = req.route_params.get("media_id")

        # Read metadata from Table to find the blobName
        table_service = TableServiceClient.from_connection_string(conn_str)
        table = table_service.get_table_client(table_name)
        entity = table.get_entity(partition_key=user_id, row_key=media_id)

        blob_name = entity.get("blobName")
        if not blob_name:
            return func.HttpResponse(
                json.dumps({"error": "No blobName stored for this item."}),
                mimetype="application/json",
                status_code=500
            )

        # Get account name + key from connection string
        # Connection string includes AccountName and AccountKey
        parts = dict(
            p.split("=", 1) for p in conn_str.split(";") if "=" in p
        )
        account_name = parts.get("AccountName")
        account_key = parts.get("AccountKey")

        if not account_name or not account_key:
            return func.HttpResponse(
                json.dumps({"error": "Storage connection string missing AccountName/AccountKey."}),
                mimetype="application/json",
                status_code=500
            )

        expiry_minutes = int(req.params.get("minutes", "15"))
        expires_on = datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expires_on
        )

        sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

        return func.HttpResponse(
            json.dumps({
                "sasUrl": sas_url,
                "expiresOn": expires_on.isoformat()
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        msg = str(e)
        status = 404 if "ResourceNotFound" in msg or "NotFound" in msg else 500
        return func.HttpResponse(
            json.dumps({"error": msg}),
            mimetype="application/json",
            status_code=status
        )
