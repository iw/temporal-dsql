package dsql

import (
	"fmt"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

var maxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

const (
	jsonBuildArrayFuncName = "jsonb_build_array"
	jsonContainsOp         = "@>"

	// likeEscapeChar is the escape character the SQL visibility store uses for LIKE.
	likeEscapeChar = '!'
)

var likeEscapeExpr = query.NewUnsafeSQLString(string(likeEscapeChar))

type queryConverter struct{}

var _ sqlplugin.VisibilityQueryConverter = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return "2006-01-02 15:04:05.999999"
}

func (c *queryConverter) GetCoalesceCloseTimeExpr() sqlparser.Expr {
	return query.NewFuncExpr(
		"coalesce",
		query.CloseTimeSAColumn,
		query.NewUnsafeSQLString(maxDatetime.Format(c.GetDatetimeFormat())),
	)
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	switch operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		newExpr := newJSONContainsExpr(col, value)
		if operator == sqlparser.NotEqualStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	case sqlparser.InStr, sqlparser.NotInStr:
		valTupleExpr, isValTuple := value.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(value),
			)
		}
		var newExpr sqlparser.Expr = &sqlparser.ParenExpr{
			Expr: c.convertInExpr(col, valTupleExpr),
		}
		if operator == sqlparser.NotInStr {
			newExpr = &sqlparser.NotExpr{Expr: newExpr}
		}
		return newExpr, nil
	default:
		// this should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type",
			query.InvalidExpressionErrMessage,
			operator,
		)
	}
}

// ConvertTextComparisonExpr matches any of the value's words as a whole word, as
// PostgreSQL's tsvector match does. DSQL has no tsvector, so Text columns hold their
// words separated by single spaces, with a space at each end, and each word is a LIKE.
func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *query.SAColumn,
	value sqlparser.Expr,
) (sqlparser.Expr, error) {
	valueExpr, ok := value.(*query.UnsafeSQLString)
	if !ok {
		return nil, query.NewConverterError(
			"%s: unexpected value type (expected string, got %s)",
			query.InvalidExpressionErrMessage,
			sqlparser.String(value),
		)
	}
	tokens := query.TokenizeTextQueryString(valueExpr.Val)
	if len(tokens) == 0 {
		return nil, query.NewConverterError(
			"%s: unexpected value for Text type search attribute (no tokens found)",
			query.InvalidExpressionErrMessage,
		)
	}
	wordExprs := make([]sqlparser.Expr, len(tokens))
	for i, token := range tokens {
		wordExprs[i] = &sqlparser.ComparisonExpr{
			Operator: sqlparser.LikeStr,
			Left:     col,
			Right:    query.NewUnsafeSQLString("% " + escapeLikeValue(token) + " %"),
			Escape:   likeEscapeExpr,
		}
	}
	var newExpr sqlparser.Expr = &sqlparser.ParenExpr{Expr: reduceOrExprs(wordExprs...)}
	if operator == sqlparser.NotEqualStr {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

// BuildSelectStmt orders every sort column newest first, so DSQL can serve the order
// and the page token's position with a backward scan of an ascending index.
func (c *queryConverter) BuildSelectStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
	pageSize int,
	token *sqlplugin.VisibilityPageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any

	if queryParams.QueryExpr != nil {
		if queryString := sqlparser.String(queryParams.QueryExpr); queryString != "" {
			whereClauses = append(whereClauses, queryString)
		}
	}

	if token != nil {
		// DSQL applies the row comparison as a filter, so the plain bound on the close
		// time is what lets an index scan start at the token.
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"%s <= ? AND (%s, %s, %s) < (?, ?, ?)",
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sqlparser.String(c.GetCoalesceCloseTimeExpr()),
				sadefs.GetSqlDbColName(sadefs.StartTime),
				sadefs.GetSqlDbColName(sadefs.RunID),
			),
		)
		queryArgs = append(queryArgs, token.CloseTime, token.CloseTime, token.StartTime, token.RunID)
	}

	whereString := ""
	if len(whereClauses) > 0 {
		whereString = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	stmt := fmt.Sprintf(
		`SELECT %s FROM executions_visibility%s ORDER BY %s DESC, %s DESC, %s DESC LIMIT ?`,
		strings.Join(sqlplugin.DbFields, ", "),
		whereString,
		sqlparser.String(c.GetCoalesceCloseTimeExpr()),
		sadefs.GetSqlDbColName(sadefs.StartTime),
		sadefs.GetSqlDbColName(sadefs.RunID),
	)
	queryArgs = append(queryArgs, pageSize)

	return stmt, queryArgs
}

func (c *queryConverter) BuildCountStmt(
	queryParams *query.QueryParams[sqlparser.Expr],
) (string, []any) {
	whereString := ""
	if queryParams.QueryExpr != nil {
		whereString = sqlparser.String(queryParams.QueryExpr)
		if whereString != "" {
			whereString = " WHERE " + whereString
		}
	}

	groupBy := make([]string, 0, len(queryParams.GroupBy)+1)
	for _, field := range queryParams.GroupBy {
		groupBy = append(groupBy, sadefs.GetSqlDbColName(field.FieldName))
	}

	groupByClause := ""
	if len(queryParams.GroupBy) > 0 {
		groupByClause = fmt.Sprintf(" GROUP BY %s", strings.Join(groupBy, ", "))
	}

	return fmt.Sprintf(
		"SELECT %s FROM executions_visibility%s%s",
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		whereString,
		groupByClause,
	), nil
}

func (c *queryConverter) convertInExpr(
	leftExpr sqlparser.Expr,
	values sqlparser.ValTuple,
) sqlparser.Expr {
	exprs := make([]sqlparser.Expr, len(values))
	for i, value := range values {
		exprs[i] = newJSONContainsExpr(leftExpr, value)
	}
	return reduceOrExprs(exprs...)
}

func reduceOrExprs(exprs ...sqlparser.Expr) sqlparser.Expr {
	return query.ReduceExprs(
		func(left, right sqlparser.Expr) sqlparser.Expr {
			if left == nil {
				return right
			}
			if right == nil {
				return left
			}
			return &sqlparser.OrExpr{
				Left:  left,
				Right: right,
			}
		},
		exprs...,
	)
}

// escapeLikeValue escapes LIKE's wildcards and the escape character itself.
func escapeLikeValue(value string) string {
	var sb strings.Builder
	for _, r := range value {
		if r == '%' || r == '_' || r == likeEscapeChar {
			sb.WriteRune(likeEscapeChar)
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

func newJSONContainsExpr(
	jsonExpr sqlparser.Expr,
	valueExpr sqlparser.Expr,
) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: jsonContainsOp,
		Left:     jsonExpr,
		Right:    query.NewFuncExpr(jsonBuildArrayFuncName, valueExpr),
	}
}
