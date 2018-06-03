#!/bin/bash
set -ex
set -o pipefail

function compress {
    TEMPL=cluster_info.XXXXXX.tar
    if ! [ -x "$(command -v xz)" ]; then
        OUTFILE=$(mktemp -t "${TEMPL}.xz")
        env XZ_OPT="--memlimit=1GiB -5" tar -C "$1" -acvf "$OUTFILE" $(ls "$1") >/dev/null
    else
        OUTFILE=$(mktemp -t "${TEMPL}.gz")
        env GZIP=-5 tar -C "$1" -acvf "$OUTFILE" $(ls "$1") >/dev/null
    fi
    echo $OUTFILE
}

function usage {
    echo "$0 HOSTS_FILE"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

if ! [ -x "$(command -v parallel-ssh)" ]; then
  echo 'Error: parallel-ssh is not installed.' >&2
  exit 1
fi

PSSH=parallel-ssh -h "$1" -p 16 -O StrictHostKeyChecking=no -P
OUTPUT_FOLDER=$(mktemp -t -d cluster_info.pssh.XXXXXXXX)

$PSSH -o "$OUTPUT_FOLDER/ceph_disk_list_js" sudo ceph-disk list --format json
$PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk --format json
$PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk -O -J
$PSSH -o "$OUTPUT_FOLDER/ipa" sudo ip a
$PSSH -o "$OUTPUT_FOLDER/ls_ceph_dirs" sudo ls -l '/var/lib/ceph/osd/ceph-*'
$PSSH -o "$OUTPUT_FOLDER/ls_ceph_journals" sudo ls -l '/var/lib/ceph/osd/ceph-*/journal'

compress "$OUTPUT_FOLDER"
