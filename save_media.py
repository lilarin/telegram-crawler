import base64
from datetime import datetime
from typing import Dict

import magic
from telethon.tl.types import InputDocumentFileLocation
from telethon.tl.types import InputPhotoFileLocation

MEDIA_DIR = "media"


def get_file_extension(mime_type: str) -> str:
    mime_to_ext = {
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'image/webp': '.webp',
        'image/bmp': '.bmp',
        'image/tiff': '.tiff',
        'image/svg+xml': '.svg',

        'audio/mpeg': '.mp3',
        'audio/wav': '.wav',
        'audio/ogg': '.ogg',

        'video/mp4': '.mp4',
        'video/mpeg': '.mpeg',
        'video/quicktime': '.mov',
        'video/x-msvideo': '.avi',
        'video/x-flv': '.flv',
        'video/webm': '.webm'
    }

    return mime_to_ext.get(mime_type, '')


async def save_media(client, file_location, file_path) -> None:
    downloaded_bytes = await client.download_file(file_location)
    if downloaded_bytes:
        mime = magic.from_buffer(downloaded_bytes, mime=True)
        file_ext = get_file_extension(mime)

        with open(file_path + file_ext, "wb") as f:
            f.write(downloaded_bytes)
        return file_path


async def format_file_location_data(media_data: dict):
    photo_id = media_data.get("id")
    access_hash = media_data.get("access_hash")
    file_reference = media_data.get("file_reference")

    if isinstance(file_reference, str):
        file_reference = base64.b64decode(file_reference)

    return photo_id, access_hash, file_reference


async def download_media_from_dict(client, channel_id: str, message_id: str, media_dict: Dict) -> None:
    try:
        media_type = media_dict.get("_")
        now = round(datetime.now().timestamp())
        base_dir = MEDIA_DIR
        file_name = f"{base_dir}/{channel_id}_{message_id}_{now}"

        if media_type == "MessageMediaPhoto":
            media_data = media_dict.get("photo", {})
            if media_data:
                media_id, access_hash, file_reference = await format_file_location_data(media_data)

                if media_id and access_hash:
                    try:
                        file_location = InputPhotoFileLocation(
                            id=media_id,
                            access_hash=access_hash,
                            file_reference=file_reference,
                            thumb_size="x"
                        )

                        await save_media(client, file_location, file_name)
                    except Exception as loc_error:
                        print(f"Failed to download photo via location: {loc_error}")

        elif media_type == "MessageMediaDocument":
            media_data = media_dict.get("document", {})
            if media_data:
                media_id, access_hash, file_reference = await format_file_location_data(media_data)

                if media_id and access_hash:
                    try:
                        file_location = InputDocumentFileLocation(
                            id=media_id,
                            access_hash=access_hash,
                            file_reference=file_reference,
                            thumb_size=""
                        )

                        await save_media(client, file_location, file_name)
                    except Exception as loc_error:
                        print(f"Failed to download document via location: {loc_error}")
        else:
            print(f"Skipped download of media type: {media_type}")

    except Exception as e:
        print(f"Error downloading media from dict: {e}")
