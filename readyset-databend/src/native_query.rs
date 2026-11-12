//! ReadySet's native adapter rewrite/read path, with a bounded Databend source context.
//! The validator below is an allowlist, NOT a parameterizer or query executor.
use std::collections::HashSet;

use anyhow::{Context, Result, bail, ensure};
use readyset_client::View;
use readyset_client::query::QueryId;
use readyset_data::{DfType, DfValue};
use readyset_sql::ast::*;
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
use readyset_sql_passes::adapter_rewrites::{
    AdapterRewriteContext, AdapterRewriteParams, DfQueryParameters, rewrite_query,
};
use readyset_sql_passes::{
    BaseSchemasContext, CanQuery, ImpliedTablesContext, ResolveSchemasContext,
    RewriteDialectContext, StarExpansionContext,
};
use serde_json::{Value as Json, json};
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::{SourceRow, declaration::Declaration};

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
    /// Original client placeholder order. Only used to construct independent barrier inputs.
    pub bindings: Vec<String>,
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
    match parse_query_with_config(ParsingPreset::for_prod(), Dialect::MySQL, sql)? {
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

fn validate_predicate(
    expr: &mut Expr,
    source: &Relation,
    input: Input,
    bindings: &mut Vec<String>,
) -> Result<()> {
    match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            lhs,
            rhs,
        } => {
            validate_predicate(lhs, source, input, bindings)?;
            validate_predicate(rhs, source, input, bindings)?;
        }
        Expr::BinaryOp {
            op: BinaryOperator::Equal,
            lhs,
            rhs,
        } => {
            let Expr::Column(column) = lhs.as_mut() else {
                bail!("equality must have a column on the left")
            };
            let name = map_column(column, source)?;
            ensure!(
                !bindings.contains(&name),
                "repeated predicate column is not supported"
            );
            let Expr::Literal(value) = rhs.as_ref() else {
                bail!("equality must have a literal or ? on the right")
            };
            match input {
                Input::Declaration => ensure!(
                    matches!(value, Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                    "cache predicates require ? placeholders"
                ),
                Input::Read => match (name.as_str(), value) {
                    ("id" | "score", Literal::Integer(_)) => {}
                    ("id" | "score", Literal::UnsignedInteger(v)) if i64::try_from(*v).is_ok() => {}
                    ("payload" | "note", Literal::String(_)) => {}
                    _ => {
                        bail!("literal type is not supported; no implicit coercion or NULL lookup")
                    }
                },
            }
            bindings.push(name);
        }
        _ => bail!("only distinct-column AND equality predicates are supported"),
    }
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
    let mut bindings = Vec::new();
    validate_predicate(
        select.where_clause.as_mut().context("WHERE required")?,
        &source,
        input,
        &mut bindings,
    )?;
    select.tables[0] = Relation::from(LOCAL_TABLE).into();
    let context = SourceContext::new()?;
    let params = rewrite_query(
        &mut select,
        AdapterRewriteParams::new(Dialect::MySQL),
        &context,
    )?;
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
        bindings,
    })
}

impl NativeQuery {
    pub fn same_shape(&self, other: &Self) -> bool {
        self.id == other.id && self.statement == other.statement
    }
    pub fn sql(&self) -> String {
        self.statement.display(Dialect::MySQL).to_string()
    }
    pub fn row_params(&self, row: &SourceRow) -> Option<Vec<DfValue>> {
        let values = row.values();
        let params: Vec<_> = self
            .bindings
            .iter()
            .map(|name| values[COLUMNS.iter().position(|c| c == name).unwrap()].clone())
            .collect();
        (!params.iter().any(DfValue::is_none)).then_some(params)
    }
    pub fn project(&self, row: &SourceRow) -> Vec<DfValue> {
        let values = row.values();
        self.projection
            .iter()
            .map(|name| values[COLUMNS.iter().position(|c| c == name).unwrap()].clone())
            .collect()
    }
}

/// Same orchestration as the production adapter: native parameter construction and native
/// View key remapping, result projection and schema. No hand-built physical lookup keys.
pub async fn read(
    view: &mut View,
    query: &NativeQuery,
    user_params: &[DfValue],
) -> Result<(Vec<Vec<DfValue>>, Vec<Json>)> {
    let keys = query.params.make_keys(user_params)?;
    let (limit, offset) = query.params.limit_offset_params(user_params)?;
    let result = readyset_client::read::read_cache(
        view,
        None,
        keys,
        limit,
        offset,
        query.params.post_lookup_plan(),
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
    Ok((result.rows.into_vec(), schema))
}

/// Debug-only POC fence: recover client parameter positions from native physical key mappings.
/// This is not used by application reads; it lets the oracle catch even unexpected extra keys.
pub fn params_from_reader_key(
    view: &View,
    key: &[DfValue],
    parameter_count: usize,
) -> Result<Vec<DfValue>> {
    let View::Single(reader) = view else {
        bail!("single reader required")
    };
    ensure!(
        reader.key_map().len() == key.len(),
        "unexpected native key arity"
    );
    let mut params = vec![DfValue::None; parameter_count];
    let mut seen = HashSet::new();
    for ((placeholder, _), value) in reader.key_map().iter().zip(key) {
        let readyset_client::ViewPlaceholder::OneToOne(index, BinaryOperator::Equal) = placeholder
        else {
            bail!("non-equality reader key")
        };
        ensure!(
            *index > 0 && *index <= parameter_count && seen.insert(*index),
            "unexpected placeholder mapping"
        );
        params[*index - 1] = value.clone();
    }
    ensure!(
        seen.len() == parameter_count,
        "incomplete placeholder mapping"
    );
    Ok(params)
}
