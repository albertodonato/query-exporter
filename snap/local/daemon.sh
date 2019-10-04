#!/bin/sh -e

CONFIG_FILE="$SNAP_DATA/config.yaml"

[ -r "$CONFIG_FILE" ] || snapctl stop "$SNAP_INSTANCE_NAME"

exec "$SNAP/bin/query-exporter" -H 0.0.0.0 :: -- "$CONFIG_FILE"
