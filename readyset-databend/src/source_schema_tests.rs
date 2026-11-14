use super::*;
use crate::native_query::{self, Input};
use serde_json::json;

pub(crate) fn describe(columns: &[(&str, &str, bool)]) -> Vec<Vec<Value>> {
    columns
        .iter()
        .map(|(name, ty, nullable)| {
            vec![
                json!(name),
                json!(ty),
                json!(if *nullable { "YES" } else { "NO" }),
            ]
        })
        .collect()
}

#[test]
fn exact_wire_codec_never_uses_float_or_assumes_id_type() {
    let schema = SourceSchema::from_describe(
        "sales",
        "orders",
        &describe(&[
            ("id", "VARCHAR", false),
            ("account", "BIGINT UNSIGNED", false),
            ("delta", "BIGINT", true),
            ("active", "BOOLEAN", false),
            ("amount", "DECIMAL(18,2)", false),
            ("label", "VARCHAR", true),
        ]),
    )
    .unwrap();
    let wire = vec![
        json!("text-id"),
        json!("18446744073709551615"),
        json!("-9223372036854775808"),
        json!("1"),
        json!("1234567890123456.78"),
        Value::Null,
    ];
    let values = schema.decode_row(&wire).unwrap();
    let output = schema
        .columns
        .iter()
        .zip(&values)
        .map(|(c, v)| c.encode(v).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(output, wire);
    assert!(schema.decode_row(&wire[..5]).is_err());
    for (column, invalid) in [
        (1, json!("18446744073709551616")),
        (2, json!(1.25)),
        (3, json!("yes")),
        (4, json!("1.999")),
        (4, json!("1e2")),
        (0, Value::Null),
    ] {
        let mut bad = wire.clone();
        bad[column] = invalid;
        assert!(schema.decode_row(&bad).is_err());
    }
    assert!(!schema.ddl().contains("PRIMARY KEY"));
    assert!(schema.ddl().contains("utf8mb4_0900_bin"));
}

#[test]
fn integer_widths_nullability_and_decimal_bounds_are_closed() {
    let types = [
        ("TINYINT", "127", "128"),
        ("TINYINT UNSIGNED", "255", "256"),
        ("SMALLINT", "-32768", "-32769"),
        ("INT UNSIGNED", "4294967295", "4294967296"),
    ];
    for (ty, valid, invalid) in types {
        let schema =
            SourceSchema::from_describe("db", "t", &describe(&[("v", ty, false)])).unwrap();
        assert!(schema.decode_row(&[json!(valid)]).is_ok());
        assert!(schema.decode_row(&[json!(invalid)]).is_err());
        assert!(!schema.columns[0].accepts_literal(&Literal::String(valid.into())));
    }
    let schema =
        SourceSchema::from_describe("db", "t", &describe(&[("n", "DECIMAL(3,2)", true)])).unwrap();
    for text in ["0.00", "-9.99", "0009.5"] {
        assert!(schema.decode_row(&[json!(text)]).is_ok());
    }
    for text in ["10.00", "1.001", "NaN", "1e0"] {
        assert!(schema.decode_row(&[json!(text)]).is_err());
    }
    assert!(schema.decode_row(&[Value::Null]).is_ok());
    for ty in [
        "VARIANT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(38,2)",
        "DECIMAL(2,3)",
        "ARRAY(INT)",
        "TIMESTAMP",
    ] {
        assert!(SourceSchema::from_describe("db", "t", &describe(&[("v", ty, true)])).is_err());
    }
}

#[test]
fn catalog_binding_never_crosses_database_or_catalog() {
    let rows = describe(&[("key", "BIGINT", false)]);
    let mut catalog = SourceCatalog::default();
    let a = catalog
        .insert(SourceSchema::from_describe("a", "items", &rows).unwrap())
        .unwrap();
    let b = catalog
        .insert(SourceSchema::from_describe("b", "items", &rows).unwrap())
        .unwrap();
    assert_ne!(a.local, b.local);
    assert_eq!(
        a.local,
        SourceSchema::from_describe("a", "items", &rows)
            .unwrap()
            .local
    );
    assert!(
        catalog
            .insert(SourceSchema::from_describe("a", "items", &rows).unwrap())
            .is_err()
    );
    assert_eq!(
        catalog
            .resolve(&Relation::from("items"), "a")
            .unwrap()
            .local,
        a.local
    );
    assert_eq!(
        catalog
            .resolve(&Relation::from("items"), "b")
            .unwrap()
            .local,
        b.local
    );
    assert!(
        catalog
            .resolve(&Relation::from("items"), "unknown")
            .is_none()
    );
    let mut qualified = Relation::new(Some("a".into()), "items".into());
    assert_eq!(catalog.resolve(&qualified, "b").unwrap().local, a.local);
    qualified.catalog = Some("other".into());
    assert!(catalog.resolve(&qualified, "a").is_none());
}

#[test]
fn dynamic_queries_use_native_identity_and_source_types() {
    let a = SourceSchema::from_describe(
        "a",
        "items",
        &describe(&[("id", "VARCHAR", false), ("cost", "DECIMAL(18,2)", true)]),
    )
    .unwrap();
    let b = SourceSchema::from_describe(
        "b",
        "items",
        &describe(&[
            ("id", "BIGINT UNSIGNED", false),
            ("active", "BOOLEAN", false),
        ]),
    )
    .unwrap();
    let compile = |schema: &SourceSchema, sql, input| {
        native_query::compile_for_source(
            native_query::parse_select(sql).unwrap(),
            schema,
            &schema.database,
            input,
        )
    };
    let template = compile(
        &a,
        "SELECT id,cost FROM items WHERE id=?",
        Input::Declaration,
    )
    .unwrap();
    let read = compile(&a, "SELECT id,cost FROM a.items WHERE id='x'", Input::Read).unwrap();
    assert!(template.read_params(&read).is_some());
    assert!(read.sql().contains(&a.local));
    assert!(matches!(read.output_columns[0].ty, SourceType::String));
    assert!(compile(&a, "SELECT id,cost FROM items WHERE id=1", Input::Read).is_err());
    assert!(compile(&a, "SELECT active FROM b.items WHERE id=1", Input::Read).is_err());
    assert!(
        compile(
            &b,
            "SELECT id,active FROM items WHERE id=18446744073709551615",
            Input::Read
        )
        .is_ok()
    );
    assert!(compile(&b, "SELECT id FROM items WHERE id=-1", Input::Read).is_err());
    assert!(compile(&b, "SELECT id FROM items WHERE active=true", Input::Read).is_ok());
    assert!(compile(&a, "SELECT id FROM items WHERE cost=12.34", Input::Read).is_ok());
    assert!(compile(&a, "SELECT id FROM items WHERE cost=12.345", Input::Read).is_err());
}

#[test]
fn typed_stream_changes_follow_discovered_width_and_keep_identical_rows() {
    use crate::replicator::parse_typed_changes;
    let schema = SourceSchema::from_describe(
        "db",
        "t",
        &describe(&[("id", "VARCHAR", false), ("n", "INT UNSIGNED", true)]),
    )
    .unwrap();
    let row = vec![
        json!("text-id"),
        Value::Null,
        json!("INSERT"),
        json!("physical-row-id"),
        json!(false),
    ];
    let changes = parse_typed_changes(&schema, &[row.clone(), row.clone()]).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].row, changes[1].row);
    for (index, invalid) in [
        (2, json!("UPDATE")),
        (3, json!("")),
        (4, json!("maybe")),
        (1, json!("-1")),
    ] {
        let mut bad = row.clone();
        bad[index] = invalid;
        assert!(parse_typed_changes(&schema, &[bad]).is_err());
    }
    assert!(parse_typed_changes(&schema, &[row[..4].to_vec()]).is_err());
}

#[test]
fn source_metadata_is_strict_and_column_quoting_is_not_sql() {
    let quoted =
        SourceSchema::from_describe("db", "t", &describe(&[("order`value", "VARCHAR", true)]))
            .unwrap();
    assert_eq!(quoted.column_list(), "`order``value`");
    assert!(SourceSchema::from_describe("db", "t", &[]).is_err());
    assert!(
        SourceSchema::from_describe(
            "db",
            "t",
            &describe(&[("x", "INT", false), ("X", "INT", true)])
        )
        .is_err()
    );
    assert!(
        SourceSchema::from_describe("db;drop", "t", &describe(&[("x", "INT", false)])).is_err()
    );
    assert!(
        SourceSchema::from_describe(
            "db",
            "t",
            &[vec![json!("x"), json!("INT"), json!("UNKNOWN")]]
        )
        .is_err()
    );
}
