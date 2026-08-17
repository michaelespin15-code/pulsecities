# Shared R2 credential derivation. Source this; do not execute it.
#
# Extracted from backup_offsite.sh when retire_raw_data.sh needed the same
# thing and got it wrong in two ways: it used whatever `rclone listremotes`
# returned first, which on this box is an ambient remote with no access to our
# bucket, and it treated `pulsecities-backups` as a bucket name when it is a
# prefix inside `vs-archive`. Both produced a 403 after the archives had
# already been written and verified. Rather than let a second copy of this
# logic exist and drift, both scripts now derive credentials here.
#
# WHERE things live: the vs-archive bucket (violation-leads' R2, same owner,
# same box) under the pulsecities-backups/ prefix, using that project's
# bucket-scoped token. A dedicated bucket and token is the clean end-state;
# when it exists, set PULSECITIES_R2_BUCKET, PULSECITIES_R2_TOKEN and
# PULSECITIES_R2_ACCOUNT_ID in this project's .env and nothing else changes.
#
# TRANSPORT: bucket-scoped Object R&W tokens authorize R2's S3 endpoint only.
# Access key id is the token's own id, from /tokens/verify; the secret is the
# SHA-256 hex of the token value. Nothing here is ever printed.
#
# Exports on success: R2_BUCKET, R2_PREFIX, R2_S3_KEYID, R2_S3_SECRET,
# R2_S3_ENDPOINT, and the RCLONE_CONFIG_R2_* vars that define the `R2:` remote.
# Returns non-zero with a message on stderr if credentials cannot be derived;
# callers decide whether that is fatal.

r2_load_credentials() {
    local app_dir="${1:-/root/pulsecities}"
    local cred_env="${2:-/root/violation-leads/.env}"

    set -a; . "$app_dir/.env" 2>/dev/null || true; set +a
    local token="${PULSECITIES_R2_TOKEN:-}"
    local account="${PULSECITIES_R2_ACCOUNT_ID:-}"
    local bucket="${PULSECITIES_R2_BUCKET:-}"

    if [ -z "$token" ] || [ -z "$account" ]; then
        set -a; . "$cred_env" 2>/dev/null || true; set +a
        token="${R2_CLOUDFLARE_API_TOKEN:-}"
        account="${CLOUDFLARE_ACCOUNT_ID:-}"
        bucket="${bucket:-vs-archive}"
    fi

    [ -n "$token" ]   || { echo "no R2 token (PULSECITIES_R2_TOKEN or $cred_env)" >&2; return 1; }
    [ -n "$account" ] || { echo "no R2 account id" >&2; return 1; }

    local keyid
    keyid=$(curl -s --max-time 15 -H "Authorization: Bearer $token" \
        "https://api.cloudflare.com/client/v4/accounts/$account/tokens/verify" \
        | python3 -c "import json,sys;print(json.load(sys.stdin)['result']['id'])" 2>/dev/null)
    [ -n "$keyid" ] || { echo "could not verify the R2 token / derive its id" >&2; return 1; }

    export R2_BUCKET="$bucket"
    export R2_PREFIX="pulsecities-backups"
    export R2_S3_KEYID="$keyid"
    export R2_S3_SECRET
    R2_S3_SECRET=$(printf '%s' "$token" | sha256sum | cut -d' ' -f1)
    export R2_S3_ENDPOINT="https://$account.r2.cloudflarestorage.com"

    # rclone streams and multiparts properly; curl 7.81 on this box cannot sign
    # a streamed body (it drops x-amz-content-sha256 from the canonical request)
    # and --data-binary would slurp a multi-GB file into memory.
    export RCLONE_CONFIG_R2_TYPE=s3
    export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_S3_KEYID"
    export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_S3_SECRET"
    export RCLONE_CONFIG_R2_ENDPOINT="$R2_S3_ENDPOINT"
}

# Size of a remote object in bytes, or empty if absent. `curl --head` with
# %header{} writeout needs curl >= 7.83 and this box has 7.81, so the length is
# read off the dumped response headers instead.
r2_remote_size() {
    local key="$1"
    curl -s --head --max-time 60 \
        --aws-sigv4 "aws:amz:auto:s3" --user "$R2_S3_KEYID:$R2_S3_SECRET" \
        -H "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" \
        "$R2_S3_ENDPOINT/$R2_BUCKET/$R2_PREFIX/$key" \
        | awk 'tolower($1) == "content-length:" {print $2}' | tr -dc '0-9'
}
