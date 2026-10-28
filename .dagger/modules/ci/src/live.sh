#!/usr/bin/env bash
# Runs the DSQL plugin's live tests on a throwaway Aurora DSQL cluster and DynamoDB table,
# and deletes both however the run ends. The ci module's live function runs it from the
# repository root, with AWS credentials, AWS_REGION and a run ID, CI_RUN, in the
# environment.
#
# Every resource is tagged purpose=temporal-dsql-ci. A run first deletes the ones an
# interrupted run left behind more than three hours ago, longer than any run takes.
# Output shows the account and cluster IDs as <account> and <cluster>, since CI logs can
# be public.
set -euo pipefail

# A web identity token is exchanged once, here, and the run goes on with the role's
# temporary credentials, which outlast the token.
if [ -n "${AWS_WEB_IDENTITY_TOKEN_FILE:-}" ]; then
	eval "$(aws configure export-credentials --format env)"
	unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_ROLE_SESSION_NAME
fi

tag=temporal-dsql-ci
table=$tag-$CI_RUN
cluster=
account=$(aws sts get-caller-identity --query Account --output text)

mask() {
	sed -e "s/$account/<account>/g" -e "s/${cluster:-<none>}/<cluster>/g"
}

cleanup() {
	local status=$?
	if [ -n "$cluster" ]; then
		aws dsql delete-cluster --identifier "$cluster" >/dev/null ||
			echo "couldn't delete the cluster; the next run deletes it"
	fi
	if aws dynamodb describe-table --table-name "$table" >/dev/null 2>&1; then
		aws dynamodb delete-table --table-name "$table" >/dev/null ||
			echo "couldn't delete the table; the next run deletes it"
	fi
	exit "$status"
}
trap cleanup EXIT

sweep() {
	local cutoff arn created name
	cutoff=$(date -u -d "@$(($(date +%s) - 3 * 3600))" +%Y-%m-%dT%H:%M:%S)
	for arn in $(aws dsql list-clusters --query 'clusters[].arn' --output text); do
		[ "$(aws dsql list-tags-for-resource --resource-arn "$arn" --query tags.purpose --output text 2>/dev/null)" = "$tag" ] || continue
		created=$(aws dsql get-cluster --identifier "${arn##*/}" --query creationTime --output text)
		if [[ ${created:0:19} < $cutoff ]]; then
			aws dsql delete-cluster --identifier "${arn##*/}" >/dev/null
			echo "deleted a cluster an earlier run left behind"
		fi
	done
	for name in $(aws dynamodb list-tables --query "TableNames[?starts_with(@, '$tag-')]" --output text); do
		created=$(aws dynamodb describe-table --table-name "$name" --query Table.CreationDateTime --output text)
		if [[ ${created:0:19} < $cutoff ]]; then
			aws dynamodb delete-table --table-name "$name" >/dev/null
			echo "deleted a table an earlier run left behind"
		fi
	done
}
sweep || echo "couldn't check for resources earlier runs left behind"

echo "--- creating a cluster and a table in $AWS_REGION"
cluster=$(aws dsql create-cluster --no-deletion-protection-enabled \
	--tags "purpose=$tag,run=$CI_RUN" --query identifier --output text)
aws dynamodb create-table --table-name "$table" --billing-mode PAY_PER_REQUEST \
	--attribute-definitions AttributeName=pk,AttributeType=S \
	--key-schema AttributeName=pk,KeyType=HASH \
	--tags "Key=purpose,Value=$tag" "Key=run,Value=$CI_RUN" >/dev/null
aws dynamodb wait table-exists --table-name "$table"
aws dynamodb update-time-to-live --table-name "$table" \
	--time-to-live-specification Enabled=true,AttributeName=ttl_epoch >/dev/null
aws dsql wait cluster-active --identifier "$cluster"
endpoint=$cluster.dsql.$AWS_REGION.on.aws

echo "--- setting up the schema"
go build -o /tmp/temporal-dsql-tool ./cmd/tools/dsql
# The tool logs each statement at debug level, which the run leaves out.
/tmp/temporal-dsql-tool --endpoint "$endpoint" setup-schema --schema-name dsql/temporal --version 1.2 2>&1 |
	mask | { grep -v DEBUG || true; }

echo "--- running the live tests"
DSQL_TEST_ENDPOINT=$endpoint DSQL_TEST_DDB_TABLE=$table \
	go test -count=1 -tags integration ./common/persistence/sql/sqlplugin/dsql/... 2>&1 | mask
