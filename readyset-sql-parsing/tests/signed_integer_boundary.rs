use readyset_sql::Dialect;
use readyset_sql::ast::{Expr, FieldDefinitionExpr, Literal, SqlQuery};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

#[test]
fn signed_boundary_has_exact_matching_asts_in_both_parsers() {
    for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
        for (input, expected) in [
            ("-9223372036854775808", Literal::Integer(i64::MIN)),
            ("-9223372036854775807", Literal::Integer(-i64::MAX)),
            ("-9223372036854775809", Literal::Number("-9223372036854775809".into())),
            ("-18446744073709551615", Literal::Number("-18446744073709551615".into())),
            ("-(-9223372036854775808)", Literal::UnsignedInteger(1u64 << 63)),
        ] {
            let SqlQuery::Select(select) = parse_query_with_config(
                ParsingPreset::BothErrorOnMismatch, dialect, format!("SELECT {input} AS value"),
            ).unwrap() else { panic!("not SELECT") };
            assert_eq!(select.fields, vec![FieldDefinitionExpr::Expr {
                expr: Expr::Literal(expected), alias: Some("value".into()),
            }], "{dialect:?}: {input}");
        }
    }
}
