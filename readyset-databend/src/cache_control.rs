//! Reuse ReadySet's CREATE/DROP/SHOW grammar, with explicit POC capability checks.
//! The raw AST must be checked BEFORE ChangeList, which does not retain every adapter option.
use anyhow::{Context, Result, bail, ensure};
use readyset_sql::Dialect;
use readyset_sql::ast::{CacheInner, CacheType, ShowStatement, SqlQuery, TrxCachePolicy};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::declaration::{Declaration, Template, identifier};

pub const INTERNAL_TABLE: &str = "m2_source";
pub const INTERNAL_QUERY: &str = "m2_by_id";

pub enum CacheControl {
    Create(Box<Template>),
    Drop(String),
    Show,
}

fn public_name(name: &str) -> Result<()> {
    ensure!(identifier(name), "cache name must be a simple identifier");
    ensure!(
        !name.eq_ignore_ascii_case(INTERNAL_TABLE) && !name.eq_ignore_ascii_case(INTERNAL_QUERY),
        "reserved internal name"
    );
    Ok(())
}

pub fn plan(
    sql: &str,
    database: &str,
    declaration: &Declaration,
    templates: &[Template],
) -> Result<CacheControl> {
    ensure!(sql.len() <= 16384, "cache SQL exceeds 16 KiB");
    ensure!(
        !sql.contains("/*") && !sql.contains("--") && !sql.contains('#'),
        "comments and hints are not supported in cache control SQL"
    );
    // Reject multiple statements even if one of the parsing backends accepts a prefix.
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
        "one cache statement per request required"
    );
    let parsed = parse_query_with_config(ParsingPreset::for_prod(), Dialect::MySQL, sql)
        .context("invalid ReadySet cache control SQL")?;
    match parsed {
        SqlQuery::CreateCache(create) => {
            ensure!(
                create.cache_type != Some(CacheType::Shallow),
                "SHALLOW is not supported"
            );
            ensure!(
                create.trx_cache_policy == TrxCachePolicy::Never
                    && !create.concurrently
                    && create.policy.is_none()
                    && create.coalesce_ms.is_none()
                    && !create.adaptive
                    && create.topk_buffer_multiplier.is_none()
                    && create.autoparam.is_default(),
                "cache modifiers are not supported by this Databend POC"
            );
            let name = create.name.context("explicit cache name required")?;
            ensure!(
                name.schema.is_none(),
                "qualified cache names are not supported"
            );
            let name = name.name.to_string();
            public_name(&name)?;
            ensure!(templates.len() < 8, "at most 8 public caches supported");
            ensure!(
                !templates.iter().any(|t| t.name.eq_ignore_ascii_case(&name)),
                "cache name already exists; DROP CACHE first"
            );
            let CacheInner::Statement {
                deep: Ok(_),
                shallow: Ok(query),
            } = create.inner
            else {
                bail!("CREATE CACHE FROM must contain a supported SELECT, not a query ID");
            };
            let template =
                declaration.template_for_sql(name, &query.to_string(), database, INTERNAL_TABLE)?;
            ensure!(
                !templates
                    .iter()
                    .any(|t| t.source_sql == template.source_sql),
                "query template already cached"
            );
            Ok(CacheControl::Create(Box::new(template)))
        }
        SqlQuery::DropCache(drop) => {
            ensure!(
                drop.name.schema.is_none(),
                "qualified cache names are not supported"
            );
            let name = drop.name.name.to_string();
            public_name(&name)?;
            ensure!(templates.iter().any(|t| t.name == name), "cache not found");
            Ok(CacheControl::Drop(name))
        }
        SqlQuery::Show(ShowStatement::CachedQueries(None | Some(CacheType::Deep), None)) => {
            Ok(CacheControl::Show)
        }
        _ => bail!(
            "only CREATE [DEEP] CACHE <name> FROM SELECT, DROP CACHE <name>, and SHOW [DEEP] CACHES are supported"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Declaration {
        Declaration {
            database: "poc".into(),
            table: "users".into(),
            queries: vec![],
        }
    }

    #[test]
    fn native_grammar_resolves_only_authorized_source() {
        let d = source();
        for sql in [
            "CREATE CACHE full_row FROM SELECT id, payload, score, note FROM poc.users WHERE id = ?",
            "CREATE DEEP CACHE full_row FROM SELECT id, payload, score, note FROM users WHERE id = ?;",
        ] {
            let CacheControl::Create(t) = plan(sql, "poc", &d, &[]).unwrap() else {
                panic!()
            };
            assert_eq!(t.name, "full_row");
            assert_eq!(
                t.local_sql,
                "SELECT id, payload, score, note FROM m2_source WHERE id = ?"
            );
            assert_eq!(
                t.match_key(
                    "SELECT id,payload,score,note FROM poc.users WHERE id=12",
                    "poc",
                    &d
                ),
                Some(12)
            );
        }
        assert!(
            plan(
                "CREATE CACHE c FROM SELECT payload FROM users WHERE id = ?",
                "other",
                &d,
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn unsupported_options_shapes_and_batches_fail_before_migration() {
        let d = source();
        for sql in [
            "CREATE SHALLOW CACHE c FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE CONCURRENTLY c FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE ALWAYS c FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE c WITH (AUTOPARAM OFF) FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE c FROM SELECT payload FROM poc.users WHERE score = ?",
            "CREATE CACHE c FROM SELECT payload FROM other.users WHERE id = ?",
            "CREATE CACHE c FROM SELECT payload FROM poc.other WHERE id = ?",
            "CREATE CACHE c FROM SELECT payload FROM poc.users WHERE id = ? LIMIT 1",
            "CREATE CACHE c FROM SELECT payload FROM poc.users WHERE id = 42",
            "CREATE CACHE FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE c FROM q_123",
            "CREATE CACHE m2_source FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE M2_BY_ID FROM SELECT payload FROM poc.users WHERE id = ?",
            "CREATE CACHE c FROM SELECT payload FROM poc.users WHERE id = ?; DROP CACHE c",
            "/* hint */ CREATE CACHE c FROM SELECT payload FROM poc.users WHERE id = ?",
        ] {
            assert!(plan(sql, "poc", &d, &[]).is_err(), "{sql}");
        }
    }

    #[test]
    fn duplicate_capacity_and_drop_guards() {
        let d = source();
        let CacheControl::Create(t) = plan(
            "CREATE CACHE c FROM SELECT payload FROM poc.users WHERE id=?",
            "poc",
            &d,
            &[],
        )
        .unwrap() else {
            panic!()
        };
        let templates = vec![*t];
        for sql in [
            "CREATE CACHE c FROM SELECT note FROM poc.users WHERE id=?",
            "CREATE CACHE C FROM SELECT note FROM poc.users WHERE id=?",
            "CREATE CACHE other FROM SELECT payload FROM poc.users WHERE id=?",
            "DROP CACHE m2_by_id",
            "DROP CACHE missing",
            "DROP CACHE poc.c",
            "SHOW SHALLOW CACHES",
            "SHOW CACHES WHERE query_id = 'x'",
        ] {
            assert!(plan(sql, "poc", &d, &templates).is_err(), "{sql}");
        }
        assert!(matches!(
            plan("DROP CACHE c", "poc", &d, &templates).unwrap(),
            CacheControl::Drop(_)
        ));
        assert!(matches!(
            plan("SHOW CACHES", "poc", &d, &templates).unwrap(),
            CacheControl::Show
        ));
        let full = (0..8)
            .map(|i| {
                d.template_for_sql(
                    format!("c{i}"),
                    "SELECT note FROM poc.users WHERE id=?",
                    "poc",
                    INTERNAL_TABLE,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            plan(
                "CREATE CACHE extra FROM SELECT id FROM poc.users WHERE id=?",
                "poc",
                &d,
                &full
            )
            .is_err()
        );
    }
}
