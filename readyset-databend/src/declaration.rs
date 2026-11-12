//! Conservative, AST-equality matching for declared single-table BIGINT point lookups.
//! Not the general ReadySet adapter rewrite pipeline. False negatives go upstream.
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement, UnaryOperator, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub database: String,
    pub table: String,
    pub queries: Vec<QueryDeclaration>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryDeclaration {
    pub name: String,
    pub sql: String,
}

pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
}

fn parse(sql: &str) -> Option<Statement> {
    // Refuse hints/versioned comments, rather than silently dropping executable semantics.
    if sql.len() > 16384 || sql.contains("/*") || sql.contains("--") || sql.contains('#') {
        return None;
    }
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, sql).ok()?;
    (stmts.len() == 1).then(|| stmts.remove(0))
}

fn rhs(statement: &mut Statement) -> Option<&mut Expr> {
    let Statement::Query(q) = statement else {
        return None;
    };
    let SetExpr::Select(s) = q.body.as_mut() else {
        return None;
    };
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = s.selection.as_mut()?
    else {
        return None;
    };
    if !matches!(left.as_ref(), Expr::Identifier(id) if id.value == "id" && id.quote_style.is_none())
    {
        return None;
    }
    Some(right)
}

fn integer(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(s, false) if s.bytes().all(|c| c.is_ascii_digit()) => s.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(v) => match &v.value {
                Value::Number(s, false) if s.bytes().all(|c| c.is_ascii_digit()) => {
                    format!("-{s}").parse().ok()
                }
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

pub struct Template {
    pub name: String,
    ast: Statement,
    pub projection: Vec<String>,
    pub local_sql: String,
}

impl Declaration {
    pub fn templates(&self, local_table: &str) -> Result<Vec<Template>> {
        ensure!(
            identifier(&self.database) && identifier(&self.table) && identifier(local_table),
            "invalid declared relation"
        );
        ensure!(
            !self.queries.is_empty() && self.queries.len() <= 8,
            "declare 1..8 queries"
        );
        let mut templates: Vec<Template> = Vec::new();
        for q in &self.queries {
            ensure!(
                identifier(&q.name) && !templates.iter().any(|t| t.name == q.name),
                "invalid or duplicate cache name"
            );
            let ast = parse(&q.sql)
                .context("declaration must be one supported SELECT, without comments")?;
            let Statement::Query(query) = &ast else {
                anyhow::bail!("not SELECT");
            };
            let SetExpr::Select(select) = query.body.as_ref() else {
                anyhow::bail!("not single SELECT");
            };
            let projection: Vec<String> = select
                .projection
                .iter()
                .map(|item| {
                    let SelectItem::UnnamedExpr(Expr::Identifier(id)) = item else {
                        anyhow::bail!("only plain projected columns supported");
                    };
                    ensure!(
                        id.quote_style.is_none()
                            && ["id", "payload", "score", "note"].contains(&id.value.as_str()),
                        "unknown projected column"
                    );
                    Ok(id.value.clone())
                })
                .collect::<Result<_>>()?;
            ensure!(!projection.is_empty(), "empty projection");
            let columns = projection.join(", ");
            let canonical = parse(&format!(
                "SELECT {columns} FROM {}.{} WHERE id = ?",
                self.database, self.table
            ))
            .unwrap();
            ensure!(
                ast == canonical,
                "template must exactly be SELECT <columns> FROM database.table WHERE id = ?"
            );
            ensure!(
                !templates.iter().any(|t| t.ast == ast),
                "duplicate query template"
            );
            templates.push(Template {
                name: q.name.clone(),
                ast,
                projection,
                local_sql: format!("SELECT {columns} FROM {local_table} WHERE id = ?"),
            });
        }
        Ok(templates)
    }
}

impl Template {
    pub fn match_key(&self, sql: &str, database: &str, declared: &Declaration) -> Option<i64> {
        let mut ast = parse(sql)?;
        // Resolve only an unqualified reference in the explicitly selected source database.
        if database == declared.database
            && let Statement::Query(q) = &mut ast
            && let SetExpr::Select(s) = q.body.as_mut()
            && s.from.len() == 1
            && matches!(
                &s.from[0].relation,
                sqlparser::ast::TableFactor::Table { .. }
            )
        {
            let bare = parse(&format!("SELECT id FROM {} WHERE id = ?", declared.table))?;
            let Statement::Query(b) = bare else {
                return None;
            };
            let SetExpr::Select(bs) = b.body.as_ref() else {
                return None;
            };
            if s.from[0].relation == bs.from[0].relation {
                let Statement::Query(t) = &self.ast else {
                    return None;
                };
                let SetExpr::Select(ts) = t.body.as_ref() else {
                    return None;
                };
                // No aliases, joins, modifiers or hints are normalized away.
                s.from[0].relation = ts.from[0].relation.clone();
            }
        }
        let key = integer(rhs(&mut ast)?)?;
        *rhs(&mut ast)? = Expr::Value(Value::Placeholder("?".into()).into());
        (ast == self.ast).then_some(key)
    }

    pub fn schema(&self) -> Vec<Json> {
        self.projection.iter().map(|name| json!({"name":name,"type": match name.as_str() {
            "id" | "score" => "Int64", "payload" => "String", "note" => "Nullable(String)", _ => unreachable!(),
        }})).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn declaration() -> Declaration {
        serde_json::from_value(json!({"database":"poc","table":"source_rows","queries":[
        {"name":"by_id","sql":"SELECT id, payload, score, note FROM poc.source_rows WHERE id = ?"}
    ]})).unwrap()
    }
    #[test]
    fn normalize_only_key_and_safe_unqualified_relation() {
        let d = declaration();
        let t = d.templates("local").unwrap();
        for (sql, key, db) in [
            (
                "select id,payload,score,note from poc.source_rows where id=42;",
                42,
                "other",
            ),
            (
                "SELECT id, payload, score, note FROM source_rows WHERE id = -9223372036854775808",
                i64::MIN,
                "poc",
            ),
        ] {
            assert_eq!(t[0].match_key(sql, db, &d), Some(key));
        }
    }
    #[test]
    fn unsupported_queries_never_match() {
        let d = declaration();
        let t = d.templates("local").unwrap();
        for sql in [
            "SELECT * FROM poc.source_rows WHERE id = 1",
            "SELECT id,payload,score,note FROM poc.source_rows WHERE id = '1'",
            "SELECT id,payload,score,note FROM poc.source_rows WHERE id=1 LIMIT 0",
            "SELECT id,payload,score,note FROM poc.source_rows WHERE id=1 OR true",
            "SELECT id,payload,score,note FROM poc.source_rows WHERE id=9223372036854775808",
            "SELECT id,payload,score,note FROM poc.source_rows WHERE id=1; SELECT 2",
            "SELECT id,payload,score,note FROM poc.source_rows AT (SNAPSHOT => 'old') WHERE id=1",
            "SELECT /* hint */ id,payload,score,note FROM poc.source_rows WHERE id=1",
            "SELECT id,payload,score,note FROM source_rows WHERE id=1",
        ] {
            assert_eq!(t[0].match_key(sql, "other", &d), None, "{sql}");
        }
    }
    #[test]
    fn invalid_declaration_fails_startup() {
        let mut d = declaration();
        d.queries[0].sql.push_str(" LIMIT 1");
        assert!(d.templates("local").is_err());
        let mut d = declaration();
        d.queries.push(d.queries[0].clone());
        assert!(d.templates("local").is_err());
    }
}
