#!/bin/sh -e

[ -r "$SNAP_DATA/config.yaml" ] || snapctl stop "$SNAP_INSTANCE_NAME"

exec env -C "$SNAP_DATA" "$SNAP/bin/query-exporter" --
