#!/usr/bin/env bash
# One-time AWS setup for scripts/aws-drill.sh: the drill bucket, its lifecycle rules, the
# IAM users and their host profiles. Run it with administrator credentials (the default AWS
# CLI chain, e.g. `AWS_PROFILE=admin`). Safe to re-run: it creates only what is missing, and
# re-applies the policies and lifecycle rules so they follow the files in contrib/aws/.
#
# Usage: scripts/aws-drill-setup.sh <bucket>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY_DIR="$SCRIPT_DIR/../contrib/aws"
BUCKET=${1:?usage: $0 <bucket>}
# The drill's config is fixed to this region.
REGION=us-east-1
PROFILE=${BCB_DRILL_PROFILE:-bcb-drill}
NO_TAG_PROFILE=${BCB_DRILL_NO_TAG_PROFILE:-bcb-drill-no-tag}
RESTORE_PROFILE=${BCB_DRILL_RESTORE_PROFILE:-bcb-drill-restore}

policy() { sed "s/BUCKET_NAME/$BUCKET/g" "$POLICY_DIR/$1"; }

echo "==> Bucket s3://$BUCKET"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null
fi
# The lifecycle call below replaces every rule on the bucket, and its tag rule expires level 1
# backups, so a bucket holding anything but drill runs is refused rather than modified.
foreign=$(aws s3api list-objects-v2 --bucket "$BUCKET" --delimiter / --output json |
    jq -r '[.CommonPrefixes[]?.Prefix, .Contents[]?.Key] | map(select(. != "aws-drill/")) | .[]')
if [ -n "$foreign" ]; then
    echo "s3://$BUCKET holds data that is not from the drill; use a dedicated bucket:" >&2
    echo "$foreign" | head -5 >&2
    exit 1
fi
foreign=$(aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET" --output json 2>/dev/null |
    jq -r --slurpfile ours "$POLICY_DIR/drill-lifecycle.json" \
        '[.Rules[].ID] - [$ours[0].Rules[].ID] | .[]' || true)
if [ -n "$foreign" ]; then
    echo "s3://$BUCKET has lifecycle rules that are not the drill's; use a dedicated bucket:" >&2
    echo "$foreign" >&2
    exit 1
fi
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
    --lifecycle-configuration "file://$POLICY_DIR/drill-lifecycle.json"

# <user> <profile> <bcb policy document>
setup_user() {
    local user=$1 profile=$2 bcb_policy=$3
    echo "==> IAM user $user, profile $profile"
    if ! aws iam get-user --user-name "$user" &>/dev/null; then
        aws iam create-user --user-name "$user" >/dev/null
    fi
    aws iam put-user-policy --user-name "$user" --policy-name bcb \
        --policy-document "$bcb_policy"
    aws iam put-user-policy --user-name "$user" --policy-name drill-assert \
        --policy-document "$(policy drill-assert-policy.json)"
    aws configure set region "$REGION" --profile "$profile"

    if [ "$(profile_user "$profile")" = "$user" ]; then
        return
    fi
    # The profile is missing its key or holds another user's: replace it with a new key.
    local key id secret
    key=$(aws iam create-access-key --user-name "$user" \
        --query 'AccessKey.[AccessKeyId, SecretAccessKey]' --output text)
    read -r id secret <<<"$key"
    aws configure set aws_access_key_id "$id" --profile "$profile"
    aws configure set aws_secret_access_key "$secret" --profile "$profile"
    # A new key takes a few seconds to become valid.
    for _ in $(seq 1 30); do
        [ "$(profile_user "$profile")" = "$user" ] && return
        sleep 2
    done
    echo "profile $profile does not authenticate as $user" >&2
    exit 1
}

# The IAM user a profile authenticates as, or nothing.
profile_user() {
    aws sts get-caller-identity --profile "$1" --query Arn --output text 2>/dev/null |
        sed -n 's|.*:user/||p'
}

setup_user bcb-drill "$PROFILE" "$(policy bcb-backup-policy.json)"
setup_user bcb-drill-no-tag "$NO_TAG_PROFILE" "$(policy bcb-backup-policy.json |
    jq '(.Statement[] | select(.Sid == "Objects") | .Action) -= ["s3:PutObjectTagging"]')"
setup_user bcb-drill-restore "$RESTORE_PROFILE" "$(policy bcb-restore-policy.json)"

echo "==> Done. Run: BCB_DRILL_BUCKET=$BUCKET task aws-drill"
