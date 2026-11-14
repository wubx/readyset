use super::*;

fn config(tables: &str, extra: &[&str]) -> Result<Settings> {
    let dir = std::env::temp_dir().join(format!("rs-http-config-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&dir)?;
    let file = dir.join("config.toml");
    fs::write(
        &file,
        format!(
            "instance_id='testconfig'\nconfirm_create=true\n[upstream]\nurl='http://127.0.0.1:8000'\n[replication]\ntables=[{tables}]\n"
        ),
    )?;
    let mut args = vec![
        "readyset-databend".to_owned(),
        "--config".into(),
        file.display().to_string(),
        "--output".into(),
        dir.join("run").display().to_string(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    let result = Cli::try_parse_from(args)
        .map_err(anyhow::Error::from)
        .and_then(settings);
    fs::remove_dir_all(dir)?;
    result
}

#[test]
fn configured_sources_select_one_shared_runtime_even_for_one_real_schema() {
    let s = config("'a.items','b.items'", &[]).unwrap();
    assert_eq!(
        s.tables,
        vec![("a".into(), "items".into()), ("b".into(), "items".into())]
    );
    assert_eq!(config("'a.items'", &[]).unwrap().tables.len(), 1);
    assert!(
        config("", &["--database", "a", "--table", "items"])
            .unwrap()
            .tables
            .is_empty()
    );
}
#[test]
fn ambiguous_duplicate_unbounded_and_invalid_configs_fail_before_startup() {
    for tables in [
        "'a.items','a.items'",
        "'a.items','A.ITEMS'",
        "'a.b.items'",
        "'a.'",
        "'a.items;DROP DATABASE x'",
        "",
    ] {
        assert!(config(tables, &[]).is_err(), "{tables}");
    }
    assert!(config("'a.items'", &["--oracle"]).is_err());
    assert!(config("'a.items'", &["--database", "a", "--table", "items"]).is_err());
    let many = (0..33)
        .map(|i| format!("'a.t{i}'"))
        .collect::<Vec<_>>()
        .join(",");
    assert!(config(&many, &[]).is_err());
}
