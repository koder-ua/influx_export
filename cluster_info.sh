#!/bin/bash
set -eux
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

function collect_ceph {
    PSSH="$1"
    OUTPUT_FOLDER="$2"
    ceph osd tree -f json > "$OUTPUT_FOLDER/cephosdtree.js"
    $PSSH -o "$OUTPUT_FOLDER/ceph_disk_list_js" sudo ceph-disk list --format json
    $PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk -J
    $PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk -O -J
    $PSSH -o "$OUTPUT_FOLDER/ipa" sudo ip a
    $PSSH -o "$OUTPUT_FOLDER/ls_ceph_dirs" sudo ls -l '/var/lib/ceph/osd/ceph-*'
    $PSSH -o "$OUTPUT_FOLDER/ls_ceph_journals" sudo ls -l '/var/lib/ceph/osd/ceph-*/journal'
}

function collect_compute {
    PSSH="$PSSH_CMP"
    $PSSH -o "$OUTPUT_FOLDER/ipa" sudo ip a
}


function usage {
    echo "$1 COMPUTES_HOSTS_FILE CEPTH_OSD_HOSTS_FILE"
}


function main {
    NAME="$1"

    if [[ $# -ne 3 ]]; then
        usage "$NAME"
        exit 1
    fi

    if ! [ -x "$(command -v parallel-ssh)" ]; then
      echo 'Error: parallel-ssh is not installed.' >&2
      exit 1
    fi

    PSSH_CMP=parallel-ssh -h "$1" -p 16 -O StrictHostKeyChecking=no -P
    PSSH_CEPH=parallel-ssh -h "$2" -p 16 -O StrictHostKeyChecking=no -P
    OUTPUT_FOLDER=$(mktemp -t -d cluster_info.pssh.XXXXXXXX)

    collect_ceph "$PSSH_CEPH" "$OUTPUT_FOLDER"
    collect_compute "$PSSH_CMP" "$OUTPUT_FOLDER"

    compress "$OUTPUT_FOLDER"
}

main "$0" "$@"


