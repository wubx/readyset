//! ReadySet's native adapter rewrite/read path, with a bounded Databend source context.
//! The validator below is an allowlist, NOT a parameterizer or query executor.
use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Context, Result, bail, ensure};
use readyset_client::View;
use readyset_client::query::QueryId;
use readyset_client::read::LocalReader;
use readyset_data::{DfType, DfValue};
use readyset_sql::ast::*;
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
use readyset_sql_passes::adapter_rewrites::{
    AdapterRewriteContext, AdapterRewriteParams, DfQueryParameters, rewrite_query,
};
use readyset_sql_passes::{
    BaseSchemasContext, CanQuery, ImpliedTablesContext, ResolveSchemasContext, RewriteBetween,
    RewriteDialectContext, StarExpansionContext,
};
use serde_json::{Value as Json, json};
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::declaration::Declaration;

pub const COLUMNS: [&str; 4] = ["id", "payload", "score", "note"];
pub const LOCAL_TABLE: &str = "m2_source";
// Databend STRING equality must be byte/codepoint exact, NOT MySQL's default ai_ci.
pub const LOCAL_DDL: &str = "CREATE TABLE m2_source (id BIGINT NOT NULL, payload TEXT COLLATE utf8mb4_0900_bin NOT NULL, score BIGINT NOT NULL, note TEXT COLLATE utf8mb4_0900_bin, PRIMARY KEY(id))";

#[derive(Clone, Copy)]
pub enum Input {
    Declaration,
    Read,
}

pub struct NativeQuery {
    pub statement: SelectStatement,
    pub params: DfQueryParameters,
    pub id: QueryId,
    pub projection: Vec<String>,
}

/// Enforce a single bounded statement even when a parsing backend accepts a prefix.
pub fn check_sql(sql: &str) -> Result<()> {
    ensure!(sql.len() <= 16384, "SQL exceeds 16 KiB");
    // Conservative false negatives, including comment markers inside strings, go upstream.
    ensure!(
        !sql.contains("/*") && !sql.contains("--") && !sql.contains('#'),
        "comments/hints are not supported"
    );
    let tokens = Tokenizer::new(&MySqlDialect {}, sql).tokenize()?;
    let tokens: Vec<_> = tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    ensure!(
        tokens
            .iter()
            .enumerate()
            .all(|(i, t)| **t != Token::SemiColon || i + 1 == tokens.len()),
        "one statement required"
    );
    Ok(())
}

pub fn parse_select(sql: &str) -> Result<SelectStatement> {
    parse_select_with(sql, ParsingPreset::for_prod())
}

/// Read-path parse. `for_prod` (BothPreferSqlparser) already returns the sqlparser-derived
/// AST whenever sqlparser succeeds; nom-sql only runs for divergence warnings and fallback.
/// Reads skip that comparison, as ReadySet itself does in shallow mode. If sqlparser cannot
/// parse a read it is a conservative miss (upstream), never a different local AST.
pub fn parse_read_select(sql: &str) -> Result<SelectStatement> {
    parse_select_with(sql, ParsingPreset::OnlySqlparser)
}

fn parse_select_with(sql: &str, preset: ParsingPreset) -> Result<SelectStatement> {
    check_sql(sql)?;
    // Guard syntax that conversion to the ReadySet AST might otherwise discard (time travel,
    // locks, table hints, etc.). This checks the raw statement envelope, not query identity.
    use sqlparser::{
        ast::{SetExpr, Statement, TableFactor},
        parser::Parser,
    };
    let raw = Parser::parse_sql(&MySqlDialect {}, sql)?;
    let [Statement::Query(q)] = raw.as_slice() else {
        bail!("one SELECT required")
    };
    let SetExpr::Select(s) = q.body.as_ref() else {
        bail!("single SELECT required")
    };
    ensure!(s.from.len() == 1, "one table required");
    let TableFactor::Table { name, .. } = &s.from[0].relation else {
        bail!("one base table required")
    };
    ensure!(s.selection.is_some(), "WHERE required");
    // Copy the allowed AST fields rather than stringify/reparse expressions: Display is not
    // dialect-aware and can change backslash-containing MySQL string literals.
    let mut envelope = Parser::parse_sql(
        &MySqlDialect {},
        &format!("SELECT id FROM {name} WHERE id=?"),
    )?;
    let Statement::Query(expected) = &mut envelope[0] else {
        unreachable!()
    };
    let SetExpr::Select(expected) = expected.body.as_mut() else {
        unreachable!()
    };
    expected.projection = s.projection.clone();
    expected.selection = s.selection.clone();
    ensure!(
        raw == envelope,
        "unsupported raw SELECT clause or table modifier"
    );
    let tokens = Tokenizer::new(&MySqlDialect {}, sql).tokenize()?;
    ensure!(
        !tokens
            .iter()
            .any(|t| matches!(t, Token::DoubleQuotedString(_))),
        "double-quoted string semantics not supported"
    );
    match parse_query_with_config(preset, Dialect::MySQL, sql)? {
        SqlQuery::Select(select) => Ok(select),
        _ => bail!("single SELECT required"),
    }
}

/// A real, closed catalog: only the mapped replica exists and its real DDL supplies types,
/// nullability, collation and unique keys to native rewrite passes.
struct SourceContext {
    table: Relation,
    body: CreateTableBody,
}
impl SourceContext {
    fn new() -> Result<Self> {
        let SqlQuery::CreateTable(table) =
            parse_query_with_config(ParsingPreset::for_prod(), Dialect::MySQL, LOCAL_DDL)?
        else {
            unreachable!()
        };
        Ok(Self {
            table: LOCAL_TABLE.into(),
            body: table.body.map_err(anyhow::Error::msg)?,
        })
    }
}

/// The catalog is fixed DDL, so parse it once instead of on every compiled read.
fn source_context() -> Result<&'static SourceContext> {
    static CONTEXT: OnceLock<SourceContext> = OnceLock::new();
    if let Some(context) = CONTEXT.get() {
        return Ok(context);
    }
    let context = SourceContext::new()?;
    Ok(CONTEXT.get_or_init(|| context))
}
impl AdapterRewriteContext for SourceContext {}
impl RewriteDialectContext for SourceContext {
    fn dialect(&self) -> readyset_data::Dialect {
        readyset_data::Dialect::DEFAULT_MYSQL
    }
}
impl BaseSchemasContext for SourceContext {
    fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
        Box::new(std::iter::once((&self.table, &self.body)))
    }
    fn base_schema(&self, table: &Relation) -> Option<&CreateTableBody> {
        (table == &self.table).then_some(&self.body)
    }
}
impl ResolveSchemasContext for SourceContext {
    fn add_invalidating_table(&self, _: Relation) {}
    fn can_query_table(&self, _: &SqlIdentifier, _: &SqlIdentifier) -> Option<CanQuery> {
        None
    }
    fn search_path(&self) -> &[SqlIdentifier] {
        &[]
    }
    fn schema_contains_custom_type(&self, _: &SqlIdentifier, _: &SqlIdentifier) -> bool {
        false
    }
}
impl StarExpansionContext for SourceContext {
    fn schema_for_relation(
        &self,
        table: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
        (table == &self.table).then(|| COLUMNS.map(SqlIdentifier::from))
    }
    fn is_relation_non_replicated(&self, table: &Relation) -> bool {
        table != &self.table
    }
}
impl ImpliedTablesContext for SourceContext {
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
        vec![(
            self.table.clone(),
            COLUMNS.map(SqlIdentifier::from).to_vec(),
        )]
    }
}

fn map_column(column: &mut Column, source: &Relation) -> Result<String> {
    ensure!(COLUMNS.contains(&column.name.as_str()), "unknown column");
    if let Some(table) = &column.table {
        ensure!(
            table.catalog.is_none()
                && table.name == source.name
                && (table.schema.is_none() || table.schema == source.schema),
            "unauthorized column qualifier"
        );
    }
    column.table = Some(LOCAL_TABLE.into());
    Ok(column.name.to_string())
}

#[derive(Clone, Copy, PartialEq)]
enum PredicateKind {
    Equality,
    In,
    Lower,
    Upper,
    Between,
}

fn validate_value(expr: &Expr, name: &str, input: Input) -> Result<()> {
    let Expr::Literal(value) = expr else {
        bail!("literal or ? required")
    };
    match input {
        Input::Declaration => ensure!(
            matches!(value, Literal::Placeholder(ItemPlaceholder::QuestionMark)),
            "cache predicates require ? placeholders"
        ),
        Input::Read => match (name, value) {
            ("id" | "score", Literal::Integer(_)) => {}
            ("id" | "score", Literal::UnsignedInteger(v)) if i64::try_from(*v).is_ok() => {}
            ("payload" | "note", Literal::String(_)) => {}
            _ => bail!("literal type is not supported; no implicit coercion or NULL lookup"),
        },
    }
    Ok(())
}

// A closed syntax/type allowlist, not an evaluator or a parameterizer. Native passes own
// BETWEEN desugaring, IN expansion, parameter order, physical keys and residual filters.
fn validate_predicate(
    expr: &mut Expr,
    source: &Relation,
    input: Input,
    seen: &mut HashMap<String, Vec<PredicateKind>>,
) -> Result<()> {
    let (column, values, kind) = match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            lhs,
            rhs,
        } => {
            validate_predicate(lhs, source, input, seen)?;
            return validate_predicate(rhs, source, input, seen);
        }
        Expr::BinaryOp { op, lhs, rhs }
            if *op == BinaryOperator::Equal || op.is_ordering_comparison() =>
        {
            let Expr::Column(column) = lhs.as_mut() else {
                bail!("comparison must have a column on the left")
            };
            let kind = match op {
                BinaryOperator::Equal => PredicateKind::Equality,
                BinaryOperator::Greater | BinaryOperator::GreaterOrEqual => PredicateKind::Lower,
                BinaryOperator::Less | BinaryOperator::LessOrEqual => PredicateKind::Upper,
                _ => unreachable!(),
            };
            (column, vec![&**rhs], kind)
        }
        Expr::In {
            lhs,
            rhs: InValue::List(values),
            negated: false,
        } => {
            let Expr::Column(column) = lhs.as_mut() else {
                bail!("single-column IN required")
            };
            ensure!(!values.is_empty(), "empty IN is not supported");
            (column, values.iter().collect(), PredicateKind::In)
        }
        Expr::Between {
            operand,
            min,
            max,
            negated: false,
        } => {
            let Expr::Column(column) = operand.as_mut() else {
                bail!("single-column BETWEEN required")
            };
            (column, vec![&**min, &**max], PredicateKind::Between)
        }
        _ => bail!("only AND equality, IN and numeric range predicates are supported"),
    };
    let name = map_column(column, source)?;
    if matches!(
        kind,
        PredicateKind::Lower | PredicateKind::Upper | PredicateKind::Between
    ) {
        ensure!(
            matches!(name.as_str(), "id" | "score"),
            "numeric ranges only"
        );
    }
    for value in values {
        validate_value(value, &name, input)?;
    }
    let previous = seen.entry(name).or_default();
    ensure!(
        previous.is_empty()
            || (previous.len() == 1
                && matches!(
                    (previous[0], kind),
                    (PredicateKind::Lower, PredicateKind::Upper)
                        | (PredicateKind::Upper, PredicateKind::Lower)
                )),
        "only one lower/upper bound pair may repeat a predicate column"
    );
    previous.push(kind);
    Ok(())
}

pub fn compile(
    mut select: SelectStatement,
    declaration: &Declaration,
    database: &str,
    local_table: &str,
    input: Input,
) -> Result<NativeQuery> {
    ensure!(local_table == LOCAL_TABLE, "unexpected local relation");
    ensure!(select.tables.len() == 1, "one source table required");
    let table = &select.tables[0];
    ensure!(
        table.alias.is_none() && table.column_aliases.is_empty(),
        "table aliases not supported"
    );
    let TableExprInner::Table(source) = &table.inner else {
        bail!("source must be a table")
    };
    ensure!(
        source.catalog.is_none()
            && source.name.as_str() == declaration.table
            && (source
                .schema
                .as_ref()
                .is_some_and(|s| s.as_str() == declaration.database)
                || (source.schema.is_none() && database == declaration.database)),
        "source is not the authorized relation/session database"
    );
    let source = Relation::new(
        Some(declaration.database.clone().into()),
        declaration.table.clone().into(),
    );
    // Reject everything not explicitly admitted, BEFORE running any optimizer that can erase it.
    let allowed = SelectStatement {
        fields: select.fields.clone(),
        tables: select.tables.clone(),
        where_clause: select.where_clause.clone(),
        ..Default::default()
    };
    ensure!(select == allowed, "unsupported SELECT clause");
    let projection = select
        .fields
        .iter_mut()
        .map(|field| {
            let FieldDefinitionExpr::Expr {
                expr: Expr::Column(column),
                alias: None,
            } = field
            else {
                bail!("plain projected columns required")
            };
            map_column(column, &source)
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(!projection.is_empty(), "empty projection");
    let mut seen = HashMap::new();
    validate_predicate(
        select.where_clause.as_mut().context("WHERE required")?,
        &source,
        input,
        &mut seen,
    )?;
    let has_in = seen.values().flatten().any(|k| *k == PredicateKind::In);
    let has_range = seen.values().flatten().any(|k| {
        matches!(
            k,
            PredicateKind::Lower | PredicateKind::Upper | PredicateKind::Between
        )
    });
    ensure!(
        !(has_in && has_range),
        "IN mixed with ranges is not yet admitted"
    );
    ensure!(
        seen.values()
            .filter(|kinds| kinds.iter().any(|k| matches!(
                k,
                PredicateKind::Lower | PredicateKind::Upper | PredicateKind::Between
            )))
            .count()
            <= 1,
        "only one numeric range column is admitted"
    );
    select.tables[0] = Relation::from(LOCAL_TABLE).into();
    // Use the existing native pass at the SAME boundary for CREATE and SELECT. Otherwise
    // adapter rewrites leave literal BETWEEN bounds inline until the later controller pass.
    select.rewrite_between();
    let context = source_context()?;
    let mut flags = AdapterRewriteParams::new(Dialect::MySQL);
    flags.server_supports_mixed_comparisons = true;
    let params = rewrite_query(&mut select, flags, context)?;
    ensure!(
        matches!(input, Input::Declaration) || params.user_param_placeholders()?.is_empty(),
        "HTTP text SELECT cannot bind placeholders"
    );
    let id = QueryId::from_select(&select, &[]);
    Ok(NativeQuery {
        statement: select,
        params,
        id,
        projection,
    })
}

impl NativeQuery {
    pub fn same_shape(&self, other: &Self) -> bool {
        self.id == other.id && self.statement == other.statement
    }
    pub fn sql(&self) -> String {
        self.statement.display(Dialect::MySQL).to_string()
    }
    /// Native shape alone loses IN arity/grouping. Match the native literal slots too, then
    /// carry the read's values using the production adapter's parameter orchestration.
    pub fn read_params(&self, read: &Self) -> Option<DfQueryParameters> {
        if !self.same_shape(read) {
            return None;
        }
        let lookup = self.params.slots().match_read(read.params.slots())?;
        Some(self.params.clone().for_read(&read.params, lookup))
    }
    pub fn project(&self, row: &crate::SourceRow) -> Vec<DfValue> {
        let values = row.values();
        self.projection
            .iter()
            .map(|name| values[COLUMNS.iter().position(|c| c == name).unwrap()].clone())
            .collect()
    }
}

pub const MAX_RESULT_ROWS: usize = 10_000;
pub const MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub type LocalResult = (Vec<Vec<Json>>, Vec<Json>);

/// Native parameter construction, View key remapping, result projection and schema.
/// Returns None on result overflow, never a partial success. No physical application keys
/// are constructed here; post-lookup filtering and the sentinel limit stay in read_cache.
pub async fn read(
    view: &mut View,
    query: &NativeQuery,
    params: &DfQueryParameters,
) -> Result<Option<LocalResult>> {
    read_with(view, None, query, params).await
}

/// Same as [`read`], optionally through the server's in-process reader (the production
/// standalone adapter path) instead of the View's TCP RPC.
pub async fn read_with(
    view: &mut View,
    local: Option<&mut (dyn LocalReader + '_)>,
    query: &NativeQuery,
    params: &DfQueryParameters,
) -> Result<Option<LocalResult>> {
    let keys = params.make_keys::<DfValue>(&[])?;
    let (_, offset) = params.limit_offset_params(&[])?;
    let result = readyset_client::read::read_cache(
        view,
        local,
        keys,
        // A sentinel row detects overflow; it is NEVER returned as a truncated success.
        // This bounds returned rows, not the engine's range-scan work or total heap usage.
        Some(MAX_RESULT_ROWS + 1),
        offset,
        params.post_lookup_plan(),
        readyset_data::Dialect::DEFAULT_MYSQL,
    )
    .await?;
    ensure!(
        result
            .schema
            .columns
            .iter()
            .map(|c| c.as_str())
            .eq(query.projection.iter().map(String::as_str)),
        "native returned columns disagree with declaration"
    );
    let schema = result.schema.columns.iter().zip(result.schema.schema.iter()).map(|(name, column)| {
        let ty = match column.column_type {
            DfType::BigInt => "Int64",
            DfType::Text(_) => "String",
            _ => bail!("unsupported native result type"),
        };
        Ok(json!({"name":name.as_str(), "type":if column.nullable { format!("Nullable({ty})") } else {ty.to_owned()} }))
    }).collect::<Result<Vec<_>>>()?;
    // Consume incrementally instead of making an unbounded second owned row collection.
    let mut rows = Vec::new();
    let mut bytes = 2;
    for row in result.rows {
        ensure!(
            row.len() == query.projection.len(),
            "native result arity mismatch"
        );
        if rows.len() == MAX_RESULT_ROWS {
            return Ok(None);
        }
        let row = row
            .iter()
            .zip(&query.projection)
            .map(|(value, column)| -> Result<Json> {
                Ok(match value {
                    DfValue::None => Json::Null,
                    _ if column == "id" || column == "score" => {
                        Json::String(i64::try_from(value.clone())?.to_string())
                    }
                    _ => Json::String(value.as_str().context("non-text projected value")?.into()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        bytes += serde_json::to_vec(&row)?.len() + usize::from(!rows.is_empty());
        if bytes > MAX_RESULT_BYTES {
            return Ok(None);
        }
        rows.push(row);
    }
    Ok(Some((rows, schema)))
}
