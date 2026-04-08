"""
Extract raster tiles from a GeoPackage (GPKG) and upload them to Azure Blob Storage
in OSM slippy-tile layout: {z}/{x}/{y} (no file extension).

The GPKG stores tiles in TMS row order (Y increases downward from the bottom), while
OSM/XYZ uses the opposite convention (Y increases downward from the top). The conversion
for a given zoom level z is:

    osm_y = (2**z - 1) - gpkg_tile_row

Usage:
    pip install azure-storage-blob tqdm
    python extract-gpkg-tiles.py \\
        --gpkg path/to/topowebb.gpkg \\
        --connection-string "DefaultEndpointsProtocol=https;AccountName=...;..." \\
        --container basemap-tiles \\
        [--table topowebb] \\
        [--overwrite] \\
        [--zoom 0,1,2,3,4,5,6,7,8,9,10,11,12]

The tiles will be reachable at:
    https://<account>.blob.core.windows.net/<container>/{z}/{x}/{y}

Configure MapLibre with:
    {
      "type": "raster",
      "tiles": ["https://<account>.blob.core.windows.net/<container>/{z}/{x}/{y}"],
      "tileSize": 256
    }
"""

import argparse
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.storage.blob import BlobServiceClient, ContentSettings
from tqdm import tqdm


# Detect image MIME type from the first bytes of tile data
def _content_type(data: bytes) -> str:
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return 'image/png'
    if data[:3] == b'\xff\xd8\xff':
        return 'image/jpeg'
    if data[:4] in (b'RIFF', b'WEBP') or data[8:12] == b'WEBP':
        return 'image/webp'
    return 'application/octet-stream'


def _upload_tile(container_client, blob_name: str, data: bytes, overwrite: bool) -> None:
    content_settings = ContentSettings(
        content_type=_content_type(data),
        cache_control='public, max-age=86400',
    )
    container_client.upload_blob(
        name=blob_name,
        data=data,
        content_settings=content_settings,
        overwrite=overwrite,
    )


def extract_and_upload(
    gpkg_path: str,
    connection_string: str,
    container_name: str,
    table_name: str,
    overwrite: bool,
    zoom_filter: list[int] | None,
    max_workers: int,
) -> None:
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)

    # Create container if it does not exist yet
    if not container_client.exists():
        container_client.create_container()
        print(f"Created container '{container_name}'")

    conn = sqlite3.connect(f'file:{gpkg_path}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row

    # Build query — optionally filter by zoom level
    if zoom_filter:
        placeholders = ','.join('?' * len(zoom_filter))
        query = (
            f'SELECT zoom_level, tile_column, tile_row, tile_data '
            f'FROM [{table_name}] '
            f'WHERE zoom_level IN ({placeholders})'
        )
        params: list = list(zoom_filter)
    else:
        query = f'SELECT zoom_level, tile_column, tile_row, tile_data FROM [{table_name}]'
        params = []

    # Count rows for progress bar
    count_query = query.replace(
        'SELECT zoom_level, tile_column, tile_row, tile_data',
        'SELECT COUNT(*)',
    )
    total = conn.execute(count_query, params).fetchone()[0]
    print(f"Uploading {total:,} tiles from '{gpkg_path}' → container '{container_name}'")

    rows = conn.execute(query, params)

    skipped = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        with tqdm(total=total, unit='tile') as bar:
            for row in rows:
                z = row['zoom_level']
                x = row['tile_column']
                # GeoPackage uses TMS row order; convert to OSM/XYZ
                osm_y = (2 ** z - 1) - row['tile_row']
                blob_name = f'{z}/{x}/{osm_y}'
                data: bytes = row['tile_data']

                future = executor.submit(
                    _upload_tile, container_client, blob_name, data, overwrite
                )
                futures[future] = blob_name

                # Drain completed futures periodically to cap memory usage
                if len(futures) >= max_workers * 4:
                    done_futures = [f for f in futures if f.done()]
                    for f in done_futures:
                        name = futures.pop(f)
                        try:
                            f.result()
                        except Exception as exc:
                            print(f'\nError uploading {name}: {exc}', file=sys.stderr)
                            errors += 1
                        bar.update(1)

            # Wait for remaining futures
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f'\nError uploading {name}: {exc}', file=sys.stderr)
                    errors += 1
                bar.update(1)

    conn.close()

    print(f'Done. Skipped: {skipped}, Errors: {errors}')
    if errors:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description='Upload GPKG tiles to Azure Blob Storage')
    parser.add_argument('--gpkg', required=True, help='Path to the .gpkg file')
    parser.add_argument(
        '--connection-string',
        required=True,
        help='Azure Storage connection string',
    )
    parser.add_argument(
        '--container',
        default='basemap-tiles',
        help='Blob container name (default: basemap-tiles)',
    )
    parser.add_argument(
        '--table',
        default='topowebb',
        help='Tile table name inside the GeoPackage (default: topowebb)',
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing blobs (default: skip)',
    )
    parser.add_argument(
        '--zoom',
        help='Comma-separated list of zoom levels to upload (default: all)',
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=16,
        help='Number of parallel upload threads (default: 16)',
    )
    args = parser.parse_args()

    zoom_filter = (
        [int(z.strip()) for z in args.zoom.split(',')]
        if args.zoom
        else None
    )

    extract_and_upload(
        gpkg_path=args.gpkg,
        connection_string=args.connection_string,
        container_name=args.container,
        table_name=args.table,
        overwrite=args.overwrite,
        zoom_filter=zoom_filter,
        max_workers=args.workers,
    )


if __name__ == '__main__':
    main()
