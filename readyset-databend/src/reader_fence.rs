//! POC-only content oracle. Never used for application matching or execution.
//!
//! Every admitted cache is a plain projection with parameter-only predicates: its reader
//! stores the shared replica's rows, including hidden index columns. Verify comparable
//! physical keys directly using the reader's native schema/key_map, without evaluating SQL or
//! trying to invert non-bijective IN/range parameter mappings. BTree point probes MUST stay
//! degenerate ranges: an Equal lookup against a BTree-only reader can use the wrong path.
//! NULL predicate slots stay unadmitted and are not part of these equality point probes.
use anyhow::{Context, Result, bail, ensure};
use readyset_client::schema::SchemaType;
use readyset_client::{KeyComparison, View, ViewPlaceholder};
use readyset_data::{Bound, DfValue, Dialect};
use readyset_sql::ast::BinaryOperator;

use crate::{SourceRow, native_query::COLUMNS};

pub fn physical_key(view: &View, row: &SourceRow) -> Result<Vec<DfValue>> {
    key_values(view, &COLUMNS, &row.values())
}

pub fn key_values(
    view: &View,
    columns: &[impl AsRef<str>],
    values: &[DfValue],
) -> Result<Vec<DfValue>> {
    let View::Single(reader) = view else {
        bail!("single reader required")
    };
    let schema = reader
        .schema()
        .context("reader schema required")?
        .schema(SchemaType::ProjectedSchema);
    reader
        .key_map()
        .iter()
        .map(|(_, index)| {
            let column = &schema
                .get(*index)
                .context("invalid key column index")?
                .column
                .name;
            let index = columns
                .iter()
                .position(|name| name.as_ref() == column.as_str())
                .context("oracle requires a plain source key column")?;
            Ok(values[index].clone())
        })
        .collect()
}

/// Physical reader row, including hidden key columns (before post-lookup projection).
pub fn physical_row(view: &View, row: &SourceRow) -> Result<Vec<DfValue>> {
    row_values(view, &COLUMNS, &row.values())
}

pub fn row_values(
    view: &View,
    columns: &[impl AsRef<str>],
    values: &[DfValue],
) -> Result<Vec<DfValue>> {
    let View::Single(reader) = view else {
        bail!("single reader required")
    };
    reader
        .schema()
        .context("reader schema required")?
        .schema(SchemaType::ProjectedSchema)
        .iter()
        .map(|column| {
            let index = columns
                .iter()
                .position(|c| c.as_ref() == column.column.name.as_str())
                .context("fence requires plain source columns")?;
            Ok(values[index].clone())
        })
        .collect()
}

/// Native point comparison; BTree readers require a degenerate range.
pub fn point_comparison(view: &View, key: &[DfValue]) -> Result<KeyComparison> {
    let View::Single(reader) = view else {
        bail!("single reader required")
    };
    ensure!(
        reader.key_map().len() == key.len(),
        "unexpected physical key arity"
    );
    let range = reader.key_map().iter().any(|(placeholder, _)| {
        !matches!(
            placeholder,
            ViewPlaceholder::OneToOne(_, BinaryOperator::Equal)
        )
    });
    let KeyComparison::Equal(key) =
        KeyComparison::from_key_and_operator(key.to_vec(), BinaryOperator::Equal)?
    else {
        unreachable!()
    };
    let comparison = if range {
        KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
    } else {
        KeyComparison::Equal(key)
    };
    Ok(comparison)
}

pub async fn point_contents(view: &mut View, key: &[DfValue]) -> Result<Vec<Vec<DfValue>>> {
    let comparison = point_comparison(view, key)?;
    let View::Single(reader) = view else {
        bail!("single reader required")
    };
    Ok(reader
        .multi_lookup(vec![comparison], Dialect::DEFAULT_MYSQL)
        .await?
        .into_vec())
}
