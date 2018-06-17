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
    echo "Data saved into: $OUTFILE"
}

function collect_ceph {
    PSSH="$1"
    OUTPUT_FOLDER="$2"
    PSCP="$3"

    $PSSH -o "$OUTPUT_FOLDER/ceph_disk_list_js" sudo ceph-disk list --format json
    $PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk -J
    $PSSH -o "$OUTPUT_FOLDER/lsblk" sudo lsblk -O -J
    $PSSH -o "$OUTPUT_FOLDER/ipa" sudo ip a

    $PSCP /tmp/collect.sh

    $PSSH -o "$OUTPUT_FOLDER/ls_ceph_dirs" sudo bash /tmp/collect.sh --ls-ceph
    $PSSH -o "$OUTPUT_FOLDER/ls_ceph_journals" sudo bash /tmp/collect.sh --ls-ceph-journal

    $PSSH rm /tmp/collect.sh
}

function collect_compute {
    PSSH="$PSSH_CMP"
    $PSSH -o "$OUTPUT_FOLDER/ipa" sudo ip a
}

function usage {
    echo "$1 COMPUTES_HOSTS_FILE CEPTH_OSD_HOSTS_FILE CEPH_HOST"
}

function ceph_info {
    CEPH_HOST="$1"
    OUTPUT_FOLDER="$2"
    SSH="ssh -o StrictHostKeyChecking=no"

    $SSH "$CEPH_HOST" sudo ceph osd tree -f json > "$OUTPUT_FOLDER/cephosdtree.js"
    $SSH "$CEPH_HOST" sudo ceph osd dump -f json > "$OUTPUT_FOLDER/cephosddump.js"
}

function main {
    CLI_NAME="$1"

    if [[ $# -ne 4 ]]; then
        if [ "$2" == "--ls-ceph" ] ; then
            ls -l /var/lib/ceph/osd/ceph-*
            exit 0
        fi
        if [ "$2" == "--ls-ceph-journal" ] ; then
            ls -l /var/lib/ceph/osd/ceph-*/journal
            exit 0
        fi
        usage "CLI_NAME" >&2
        exit 1
    fi

    COMPUTES_HOSTS_FILE="$2"
    CEPTH_OSD_HOSTS_FILE="$3"
    CEPH_HOST="$4"

    if ! [ -x "$(command -v parallel-ssh)" ]; then
      echo 'Error: parallel-ssh is not installed.' >&2
      exit 1
    fi

    PSSH_CMP="parallel-ssh -h $COMPUTES_HOSTS_FILE -p 16 -O StrictHostKeyChecking=no"
    PSSH_CEPH="parallel-ssh -h $CEPTH_OSD_HOSTS_FILE -p 16 -O StrictHostKeyChecking=no"
    PSCP_CEPH="parallel-scp -h $CEPTH_OSD_HOSTS_FILE -p 16 -O StrictHostKeyChecking=no $CLI_NAME"
    OUTPUT_FOLDER=$(mktemp -t -d cluster_info.pssh.XXXXXXXX)

    collect_ceph "$PSSH_CEPH" "$OUTPUT_FOLDER" "$PSCP_CEPH"
    collect_compute "$PSSH_CMP" "$OUTPUT_FOLDER"
    ceph_info "$CEPH_HOST" "$OUTPUT_FOLDER"

    compress "$OUTPUT_FOLDER"
}

main "$0" "$@"


