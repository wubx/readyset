//! Explicit source binding and public cache declarations using ReadySet's native query pipeline.
use anyhow::{Result, ensure};
use readyset_sql::ast::SelectStatement;
use serde::Deserialize;

use crate::native_query::{self, Input, NativeQuery};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub database: String,
    pub table: String,
    #[serde(default)]
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

pub struct Template {
    pub name: String,
    pub query: NativeQuery,
    pub source_sql: String,
}

impl Declaration {
    pub fn templates(&self, local_table: &str) -> Result<Vec<Template>> {
        ensure!(
            identifier(&self.database) && identifier(&self.table) && identifier(local_table),
            "invalid declared relation"
        );
        ensure!(self.queries.len() <= 8, "declare at most 8 queries");
        let mut templates: Vec<Template> = Vec::new();
        for query in &self.queries {
            ensure!(
                identifier(&query.name)
                    && !["m2_source", "m2_by_id"]
                        .iter()
                        .any(|n| query.name.eq_ignore_ascii_case(n))
                    && !templates
                        .iter()
                        .any(|t| query.name.eq_ignore_ascii_case(&t.name)),
                "invalid or duplicate cache name"
            );
            let template =
                self.template_for_sql(query.name.clone(), &query.sql, &self.database, local_table)?;
            ensure!(
                !templates
                    .iter()
                    .any(|t| t.query.same_shape(&template.query)),
                "duplicate native query shape"
            );
            templates.push(template);
        }
        Ok(templates)
    }

    pub fn template_for_sql(
        &self,
        name: String,
        sql: &str,
        database: &str,
        local_table: &str,
    ) -> Result<Template> {
        self.template_for_select(
            name,
            native_query::parse_select(sql)?,
            database,
            local_table,
        )
    }

    pub fn template_for_select(
        &self,
        name: String,
        select: SelectStatement,
        database: &str,
        local_table: &str,
    ) -> Result<Template> {
        use readyset_sql::{Dialect, DialectDisplay};
        let source_sql = select.display(Dialect::MySQL).to_string();
        let query = native_query::compile(select, self, database, local_table, Input::Declaration)?;
        Ok(Template {
            name,
            query,
            source_sql,
        })
    }

    /// Unsupported syntax/types are conservative misses, never rewritten by this crate.
    pub fn read_query(&self, sql: &str, database: &str) -> Option<NativeQuery> {
        native_query::compile(
            native_query::parse_select(sql).ok()?,
            self,
            database,
            native_query::LOCAL_TABLE,
            Input::Read,
        )
        .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_data::DfValue;
    fn source() -> Declaration {
        Declaration {
            database: "poc".into(),
            table: "source_rows".into(),
            queries: vec![],
        }
    }
    fn template(sql: &str) -> Template {
        source()
            .template_for_sql("cache".into(), sql, "poc", native_query::LOCAL_TABLE)
            .unwrap()
    }

    #[test]
    fn native_rewrite_matches_literal_reads_and_builds_composite_keys() {
        let d = source();
        for (declaration, read, expected) in [
            (
                "SELECT payload FROM poc.source_rows WHERE score=?",
                "SELECT payload FROM source_rows WHERE score=-42",
                vec![DfValue::from(-42i64)],
            ),
            (
                "SELECT payload FROM poc.source_rows WHERE id=?",
                "SELECT payload FROM source_rows WHERE id=-9223372036854775808",
                vec![DfValue::from(i64::MIN)],
            ),
            (
                "SELECT payload,note FROM poc.source_rows WHERE id=? AND score=?",
                "SELECT payload,note FROM source_rows WHERE id=7 AND score=-100",
                vec![DfValue::from(7i64), DfValue::from(-100i64)],
            ),
            (
                "SELECT id FROM poc.source_rows WHERE payload=? AND score=? AND note=?",
                "SELECT id FROM poc.source_rows WHERE payload='中文é' AND score=3 AND note='x'",
                vec![
                    DfValue::from("中文é"),
                    DfValue::from(3i64),
                    DfValue::from("x"),
                ],
            ),
        ] {
            let t = template(declaration);
            let q = d.read_query(read, "poc").unwrap();
            assert!(t.query.same_shape(&q), "{declaration}: {read}");
            let keys = q.params.make_keys::<DfValue>(&[]).unwrap();
            assert_eq!(keys.len(), 1);
            // Native autoparameterization uses the MySQL literal collation; native read_cache
            // subsequently coerces text keys to the replica's explicit binary column collation.
            assert_eq!(keys[0].len(), expected.len());
            for (actual, expected) in keys[0].iter().zip(&expected) {
                if let Some(s) = expected.as_str() {
                    assert_eq!(actual.as_str(), Some(s));
                } else {
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    #[test]
    fn native_strings_are_not_stringified_between_parse_and_parameterization() {
        let d = source();
        let t = template("SELECT id FROM poc.source_rows WHERE payload=? AND note=?");
        let q = d
            .read_query(
                r"SELECT id FROM poc.source_rows WHERE payload='back\\slash' AND note='O''Reilly'",
                "poc",
            )
            .unwrap();
        assert!(t.query.same_shape(&q));
        let keys = q.params.make_keys::<DfValue>(&[]).unwrap();
        assert_eq!(keys[0][0].as_str(), Some(r"back\slash"));
        assert_eq!(keys[0][1].as_str(), Some("O'Reilly"));
    }

    #[test]
    fn four_equality_parameters_use_native_binding_metadata() {
        let d = source();
        let t = template(
            "SELECT payload FROM poc.source_rows WHERE note=? AND score=? AND id=? AND payload=?",
        );
        let q=d.read_query("SELECT payload FROM poc.source_rows WHERE note='n' AND score=-1 AND id=7 AND payload='p'", "poc").unwrap();
        assert!(t.query.same_shape(&q));
        let row = crate::SourceRow {
            id: 7,
            payload: "p".into(),
            score: -1,
            note: Some("n".into()),
        };
        let params = t.query.read_params(&q).unwrap();
        let keys = params.make_keys::<DfValue>(&[]).unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0][0].as_str(), Some("n"));
        assert_eq!(keys[0][1], DfValue::from(-1i64));
        assert_eq!(t.query.project(&row), vec![row.values()[1].clone()]);
    }

    #[test]
    fn unsafe_shapes_types_and_sessions_are_misses() {
        let d = source();
        for sql in [
            "SELECT * FROM poc.source_rows WHERE id=1",
            "SELECT payload FROM poc.source_rows WHERE score='1'",
            "SELECT payload FROM poc.source_rows WHERE payload=1",
            "SELECT payload FROM poc.source_rows WHERE note=NULL",
            "SELECT payload FROM poc.source_rows WHERE note IS NULL",
            "SELECT payload FROM poc.source_rows WHERE score=1 OR id=2",
            "SELECT payload FROM poc.source_rows WHERE score!=1",
            "SELECT payload FROM poc.source_rows WHERE id=1 LIMIT 0",
            "SELECT payload FROM poc.source_rows WHERE id=1 ORDER BY payload",
            "SELECT payload FROM poc.source_rows WHERE id=1; SELECT 2",
            "SELECT payload FROM poc.source_rows WHERE id=9223372036854775808",
            "SELECT payload FROM poc.source_rows WHERE id=-(-9223372036854775808)",
            "SELECT payload FROM poc.source_rows WHERE id=-9223372036854775809",
            "SELECT payload FROM poc.source_rows WHERE id=?",
            "SELECT payload FROM poc.source_rows WHERE score=1 AND score=2",
            "SELECT payload FROM other.source_rows WHERE id=1",
            "SELECT payload FROM poc.source_rows AT (SNAPSHOT => 'old') WHERE id=1",
            "SELECT payload FROM poc.source_rows WHERE id=1 FOR UPDATE",
            "SELECT /* hint */ payload FROM poc.source_rows WHERE id=1",
            "SELECT payload FROM source_rows WHERE id=1",
        ] {
            assert!(d.read_query(sql, "other").is_none(), "{sql}");
        }
        assert!(
            d.read_query(
                "SELECT payload FROM poc.source_rows WHERE id=-9223372036854775808",
                "other"
            )
            .is_some()
        );
    }

    #[test]
    fn shape_does_not_ignore_predicates_or_projection() {
        let d = source();
        let t = template("SELECT id,payload,score,note FROM poc.source_rows WHERE score=?");
        let full = d
            .read_query(
                "SELECT id,payload,score,note FROM poc.source_rows WHERE id=3",
                "poc",
            )
            .unwrap();
        assert!(!t.query.same_shape(&full));
        let other = d
            .read_query("SELECT payload FROM poc.source_rows WHERE score=3", "poc")
            .unwrap();
        assert!(!t.query.same_shape(&other));
    }

    #[test]
    fn native_in_matches_slots_and_preserves_grouping() {
        let d = source();
        let t =
            template("SELECT payload FROM poc.source_rows WHERE score IN (?,?) AND id IN (?,?,?)");
        let q = d
            .read_query(
                "SELECT payload FROM source_rows WHERE score IN (1,2) AND id IN (7,8,9)",
                "poc",
            )
            .unwrap();
        let params = t.query.read_params(&q).unwrap();
        let keys = params.make_keys::<DfValue>(&[]).unwrap();
        assert_eq!(keys, q.params.make_keys::<DfValue>(&[]).unwrap());
        assert_eq!(
            keys.into_iter()
                .map(|key| key.into_owned())
                .collect::<Vec<_>>(),
            vec![
                vec![1.into(), 7.into()],
                vec![1.into(), 8.into()],
                vec![1.into(), 9.into()],
                vec![2.into(), 7.into()],
                vec![2.into(), 8.into()],
                vec![2.into(), 9.into()]
            ]
        );
        for sql in [
            "SELECT payload FROM source_rows WHERE score IN (1,2,3) AND id IN (7,8)",
            "SELECT payload FROM source_rows WHERE score IN (1) AND id IN (7,8,9)",
            "SELECT payload FROM source_rows WHERE score IN (1,2) AND id IN (7,8,9,10)",
        ] {
            let q = d.read_query(sql, "poc").unwrap();
            assert!(
                t.query.same_shape(&q),
                "native collapse shares a physical shape"
            );
            assert!(t.query.read_params(&q).is_none(), "slots must reject {sql}");
        }
    }

    #[test]
    fn native_numeric_ranges_share_create_read_rewrites() {
        let d = source();
        for (decl, sql) in [
            ("score > ?", "score > -9223372036854775808"),
            ("score >= ?", "score >= 9223372036854775807"),
            ("score < ?", "score < 0"),
            ("score <= ?", "score <= -1"),
            ("score BETWEEN ? AND ?", "score BETWEEN -2 AND 9"),
            (
                "payload=? AND score>=? AND score<?",
                "payload='p' AND score>=-1 AND score<3",
            ),
            (
                "score BETWEEN ? AND ? AND note=?",
                "score BETWEEN -2 AND 9 AND note='n'",
            ),
        ] {
            let t = template(&format!("SELECT id FROM poc.source_rows WHERE {decl}"));
            let q = d
                .read_query(
                    &format!("SELECT id FROM poc.source_rows WHERE {sql}"),
                    "poc",
                )
                .unwrap();
            let params = t
                .query
                .read_params(&q)
                .unwrap_or_else(|| panic!("{decl}: {sql}"));
            assert_eq!(params.make_keys::<DfValue>(&[]).unwrap().len(), 1);
        }
    }

    #[test]
    fn unsupported_in_ranges_and_types_stay_closed() {
        let d = source();
        for predicate in [
            "payload > 'a'",
            "note BETWEEN 'a' AND 'z'",
            "score IN (1,NULL)",
            "score IN ('1','2')",
            "score NOT IN (1,2)",
            "score NOT BETWEEN 1 AND 2",
            "(id,score) IN ((1,2),(3,4))",
            "id IN (SELECT id FROM source_rows)",
            "id IN ()",
            "id IN (1,2) AND score>1",
            "score>1 AND id>2",
            "score>1 AND score>2",
            "score=1 AND score<2",
            "score BETWEEN 1 AND 2 AND score>0",
            "score>1.0",
            "score>9223372036854775808",
            "score<-9223372036854775809",
        ] {
            let sql = format!("SELECT id FROM poc.source_rows WHERE {predicate}");
            assert!(d.read_query(&sql, "poc").is_none(), "{sql}");
        }
    }

    #[test]
    fn in_expansion_uses_native_product_guard() {
        let d = source();
        let list = |n| vec!["?"; n].join(",");
        let sql = format!(
            "SELECT id FROM poc.source_rows WHERE id IN ({}) AND score IN ({})",
            list(100),
            list(101)
        );
        assert!(
            d.template_for_sql("c".into(), &sql, "poc", native_query::LOCAL_TABLE)
                .is_err()
        );
        let sql = format!(
            "SELECT id FROM poc.source_rows WHERE id IN ({}) AND score IN ({})",
            list(100),
            list(100)
        );
        assert!(
            d.template_for_sql("c".into(), &sql, "poc", native_query::LOCAL_TABLE)
                .is_ok()
        );
    }

    #[test]
    fn declaration_requires_placeholders_and_rejects_extra_clauses() {
        let d = source();
        for sql in [
            "SELECT payload FROM poc.source_rows WHERE id=42",
            "SELECT payload FROM poc.source_rows WHERE score=? AND id=1",
            "SELECT payload FROM poc.source_rows WHERE score=? AND score=?",
            "SELECT payload FROM poc.source_rows WHERE score=? LIMIT 1",
        ] {
            assert!(
                d.template_for_sql("c".into(), sql, "poc", native_query::LOCAL_TABLE)
                    .is_err(),
                "{sql}"
            );
        }
    }
}
