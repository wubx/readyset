//! Which source tables a statement forwarded to Databend can change.
//!
//! Local reads must not hide a session's own write: after a forwarded write, that session
//! reads the written table from Databend until a capture round that started after the write
//! completed. Scoping that fence needs the written table, so a forwarded statement is
//! classified here. Anything not recognized with certainty is [`Effect::Unknown`], and the
//! caller then fences every table for every session (the conservative default).
use sqlparser::ast::{FromTable, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// What a forwarded statement can do to table data.
#[derive(Debug, PartialEq, Eq)]
pub enum Effect {
    /// A plain query: changes no table.
    NoWrite,
    /// A single-table INSERT, UPDATE or DELETE: changes only this table, given as
    /// (database if qualified, table), both lowercased.
    Writes(Option<String>, String),
    /// Anything else, including all statements this classifier cannot parse with certainty.
    Unknown,
}

const MAX_SQL: usize = 64 * 1024;

/// Unquoted words that never appear in a plain query, or in a single-table DML statement
/// besides its own leading keyword. Their presence (for example a data-modifying CTE, `FOR
/// UPDATE`, `WITH CONSUME`, `INSERT ... SELECT ... INTO`) makes the statement `Unknown`.
const WRITE_WORDS: [&str; 20] = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CALL",
    "EXECUTE",
    "ALTER",
    "CREATE",
    "DROP",
    "UNDROP",
    "GRANT",
    "REVOKE",
    "OPTIMIZE",
    "VACUUM",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "CONSUME",
    "COPY",
    "OVERWRITE",
];

pub fn classify(sql: &str) -> Effect {
    // Comment syntax differs between MySQL and Databend; dollar quoting is Databend-only.
    if sql.len() > MAX_SQL
        || sql.contains("--")
        || sql.contains("/*")
        || sql.contains('#')
        || sql.contains('$')
    {
        return Effect::Unknown;
    }
    let Ok(tokens) = Tokenizer::new(&MySqlDialect {}, sql).tokenize() else {
        return Effect::Unknown;
    };
    let tokens: Vec<_> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    // One statement only (a trailing semicolon is allowed).
    if tokens
        .iter()
        .enumerate()
        .any(|(i, t)| *t == Token::SemiColon && i + 1 != tokens.len())
    {
        return Effect::Unknown;
    }
    let words: Vec<String> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
            _ => None,
        })
        .collect();
    let Ok(mut statements) = Parser::parse_sql(&MySqlDialect {}, sql) else {
        return Effect::Unknown;
    };
    if statements.len() != 1 {
        return Effect::Unknown;
    }
    // `allowed`: the statement's own keywords, which may appear among the words once each.
    let only = |allowed: &[&str]| {
        let mut seen = allowed.to_vec();
        words.iter().all(|w| {
            if let Some(i) = seen.iter().position(|a| a == w) {
                seen.remove(i);
                true
            } else {
                !WRITE_WORDS.contains(&w.as_str()) && w != "INTO" && w != "SET"
            }
        })
    };
    match statements.remove(0) {
        Statement::Query(_) if only(&[]) => Effect::NoWrite,
        Statement::Insert(insert) if only(&["INSERT", "INTO"]) => match insert.table {
            TableObject::TableName(name) if insert.on.is_none() => written(&name),
            _ => Effect::Unknown,
        },
        Statement::Update(update) if only(&["UPDATE", "SET"]) => {
            match (
                &update.table.relation,
                update.table.joins.is_empty(),
                &update.from,
            ) {
                (TableFactor::Table { name, .. }, true, None) => written(name),
                _ => Effect::Unknown,
            }
        }
        Statement::Delete(delete) if only(&["DELETE"]) => {
            let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &delete.from;
            match (
                delete.tables.is_empty(),
                delete.using.is_none(),
                from.as_slice(),
            ) {
                (true, true, [one]) if one.joins.is_empty() => match &one.relation {
                    TableFactor::Table { name, .. } => written(name),
                    _ => Effect::Unknown,
                },
                _ => Effect::Unknown,
            }
        }
        _ => Effect::Unknown,
    }
}

fn written(name: &ObjectName) -> Effect {
    let parts: Option<Vec<String>> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect();
    match parts.as_deref() {
        Some([table]) => Effect::Writes(None, table.clone()),
        Some([database, table]) => Effect::Writes(Some(database.clone()), table.clone()),
        _ => Effect::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(db: Option<&str>, t: &str) -> Effect {
        Effect::Writes(db.map(str::to_owned), t.to_owned())
    }

    #[test]
    fn databend_jdbc_statements() {
        assert_eq!(
            classify(
                "INSERT INTO a (id, ext_id, i32, u8, flag, amount, note) VALUES (1100456, 'jdbc-1100456', null, 7, true, 12.30, '它\\'s \"q\" \\\\ 😀')"
            ),
            w(None, "a")
        );
        assert_eq!(
            classify("UPDATE a SET amount = 99.99 WHERE id = 1100456"),
            w(None, "a")
        );
        assert_eq!(classify("DELETE FROM a WHERE id = 1100456"), w(None, "a"));
        assert_eq!(
            classify("delete from Shop.Orders where id = 1;"),
            w(Some("shop"), "orders")
        );
        assert_eq!(
            classify("INSERT INTO `db`.`T` VALUES (1)"),
            w(Some("db"), "t")
        );
        // Only the target is written; the SELECT reads.
        assert_eq!(classify("INSERT INTO a SELECT * FROM b"), w(None, "a"));
        assert_eq!(classify("SELECT version()"), Effect::NoWrite);
        assert_eq!(
            classify("SELECT count(*), sum(amount) FROM a WHERE id < 1000"),
            Effect::NoWrite
        );
        assert_eq!(
            classify("SELECT id FROM a WHERE note = 'delete from a; update a set x = 1'"),
            Effect::NoWrite
        );
    }

    #[test]
    fn anything_uncertain_is_unknown() {
        for sql in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "CREATE TEMP TABLE a (id BIGINT)",
            "DROP TABLE a",
            "TRUNCATE TABLE a",
            "REPLACE INTO a ON (id) VALUES (1)",
            "INSERT OVERWRITE a VALUES (1)",
            "INSERT INTO a VALUES (1) ON DUPLICATE KEY UPDATE x = 1",
            "MERGE INTO a USING b ON a.id = b.id WHEN MATCHED THEN DELETE",
            "UPDATE a JOIN b ON a.id = b.id SET b.x = 1",
            "UPDATE a, b SET b.x = 1",
            "DELETE a, b FROM a JOIN b ON a.id = b.id",
            "DELETE FROM a USING b WHERE a.id = b.id",
            "SELECT 1; DELETE FROM a",
            "SELECT * FROM a -- comment",
            "SELECT * FROM a /* x */",
            "SELECT * FROM a # x",
            "EXECUTE IMMEDIATE $$ DELETE FROM a $$",
            "SELECT * FROM s WITH CONSUME",
            "SELECT * FROM a FOR UPDATE",
            "COPY INTO a FROM @stage",
            "CALL system$fuse_snapshot('db', 'a')",
            "SHOW TABLES",
            "SET max_threads = 1",
            "INSERT INTO a.b.c VALUES (1)",
            "not sql at all",
        ] {
            assert_eq!(classify(sql), Effect::Unknown, "{sql}");
        }
    }
}
