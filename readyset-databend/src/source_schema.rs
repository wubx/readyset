//! Databend source metadata and lossless wire codecs. This is not a second type system:
//! every accepted type maps to ReadySet's existing DDL, DfType and DfValue. No primary key
//! is inferred, even if a source column is called `id` (Databend preserves duplicates).
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use readyset_data::{Collation, DfType, DfValue};
use readyset_sql::ast::{Literal, Relation};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::declaration::identifier;
use crate::native_query::SourceContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceType {
    Boolean,
    Signed(u8),
    Unsigned(u8),
    String,
    /// Native decimal uses a bounded mantissa. Wider Databend decimals must not be rounded.
    Decimal {
        precision: u16,
        scale: u8,
    },
}

impl SourceType {
    fn parse(name: &str) -> Result<Self> {
        let upper = name.to_ascii_uppercase().replace(' ', "");
        Ok(match upper.as_str() {
            "BOOLEAN" | "BOOL" => Self::Boolean,
            "TINYINT" | "INT8" => Self::Signed(8),
            "SMALLINT" | "INT16" => Self::Signed(16),
            "INT" | "INTEGER" | "INT32" => Self::Signed(32),
            "BIGINT" | "INT64" => Self::Signed(64),
            "TINYINTUNSIGNED" | "UINT8" => Self::Unsigned(8),
            "SMALLINTUNSIGNED" | "UINT16" => Self::Unsigned(16),
            "INTUNSIGNED" | "INTEGERUNSIGNED" | "UINT32" => Self::Unsigned(32),
            "BIGINTUNSIGNED" | "UINT64" => Self::Unsigned(64),
            "STRING" | "VARCHAR" | "TEXT" => Self::String,
            _ => {
                let args = upper
                    .strip_prefix("DECIMAL(")
                    .and_then(|s| s.strip_suffix(')'))
                    .context("unsupported Databend type (no implicit conversion)")?;
                let (precision, scale) = args.split_once(',').context("invalid DECIMAL type")?;
                let precision = precision.parse::<u16>()?;
                let scale = scale.parse::<u8>()?;
                ensure!(
                    (1..=28).contains(&precision) && u16::from(scale) <= precision,
                    "only exact DECIMAL precision <= 28 is supported"
                );
                Self::Decimal { precision, scale }
            }
        })
    }

    pub fn native_type(&self) -> DfType {
        match self {
            Self::Boolean => DfType::Bool,
            Self::Signed(8) => DfType::TinyInt,
            Self::Signed(16) => DfType::SmallInt,
            Self::Signed(32) => DfType::Int,
            Self::Signed(_) => DfType::BigInt,
            Self::Unsigned(8) => DfType::UnsignedTinyInt,
            Self::Unsigned(16) => DfType::UnsignedSmallInt,
            Self::Unsigned(32) => DfType::UnsignedInt,
            Self::Unsigned(_) => DfType::UnsignedBigInt,
            Self::String => DfType::Text(Collation::Utf8Binary),
            Self::Decimal { precision, scale } => DfType::Numeric {
                prec: *precision,
                scale: *scale,
            },
        }
    }

    fn ddl(&self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".into(),
            Self::Signed(bits) | Self::Unsigned(bits) => {
                let base = match bits {
                    8 => "TINYINT",
                    16 => "SMALLINT",
                    32 => "INT",
                    _ => "BIGINT",
                };
                format!(
                    "{base}{}",
                    if matches!(self, Self::Unsigned(_)) {
                        " UNSIGNED"
                    } else {
                        ""
                    }
                )
            }
            Self::String => "TEXT COLLATE utf8mb4_0900_bin".into(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        }
    }

    pub fn wire_type(&self) -> String {
        match self {
            Self::Boolean => "Boolean".into(),
            Self::Signed(bits) => format!("Int{bits}"),
            Self::Unsigned(bits) => format!("UInt{bits}"),
            Self::String => "String".into(),
            Self::Decimal { precision, scale } => format!("Decimal({precision}, {scale})"),
        }
    }

    pub fn numeric(&self) -> bool {
        !matches!(self, Self::Boolean | Self::String)
    }

    fn decimal_text(&self, text: &str) -> Result<()> {
        let Self::Decimal { precision, scale } = self else {
            bail!("not decimal")
        };
        let text = text.strip_prefix('-').unwrap_or(text);
        let mut parts = text.split('.');
        let whole = parts.next().unwrap_or_default();
        let fraction = parts.next().unwrap_or_default();
        ensure!(
            !whole.is_empty()
                && whole.bytes().all(|c| c.is_ascii_digit())
                && fraction.bytes().all(|c| c.is_ascii_digit())
                && parts.next().is_none(),
            "invalid exact decimal"
        );
        ensure!(
            whole.trim_start_matches('0').len() <= usize::from(*precision - u16::from(*scale))
                && fraction.len() <= usize::from(*scale),
            "decimal exceeds source precision/scale"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceColumn {
    pub name: String,
    pub ty: SourceType,
    pub nullable: bool,
}

impl SourceColumn {
    pub fn wire_type(&self) -> String {
        let ty = self.ty.wire_type();
        if self.nullable {
            format!("Nullable({ty})")
        } else {
            ty
        }
    }

    /// Validate the HTTP cell before invoking any native coercion. In particular never
    /// parse integers through f64, or turn invalid strings/NULL into numeric zero.
    pub fn decode(&self, value: &Value) -> Result<DfValue> {
        if value.is_null() {
            ensure!(self.nullable, "NULL in non-nullable column {}", self.name);
            return Ok(DfValue::None);
        }
        let text = value.as_str();
        Ok(match &self.ty {
            SourceType::String => DfValue::from_str_and_collation(
                text.context("expected String")?,
                Collation::Utf8Binary,
            ),
            SourceType::Signed(bits) => {
                let value = match text {
                    Some(s) => s.parse::<i64>()?,
                    None => value.as_i64().context("expected exact signed integer")?,
                };
                if *bits < 64 {
                    ensure!(
                        value >= -(1i64 << (*bits - 1)) && value < (1i64 << (*bits - 1)),
                        "signed integer overflow"
                    );
                }
                value.into()
            }
            SourceType::Unsigned(bits) => {
                let value = match text {
                    Some(s) => s.parse::<u64>()?,
                    None => value.as_u64().context("expected exact unsigned integer")?,
                };
                if *bits < 64 {
                    ensure!(value < (1u64 << bits), "unsigned integer overflow");
                }
                value.into()
            }
            SourceType::Boolean => match (value, text) {
                (Value::Bool(b), _) => (*b).into(),
                (_, Some("true" | "1")) => true.into(),
                (_, Some("false" | "0")) => false.into(),
                _ => bail!("expected Boolean"),
            },
            SourceType::Decimal { .. } => {
                let text = text.context("Decimal must be string encoded")?;
                self.ty.decimal_text(text)?;
                DfValue::from_str_and_collation(text, Collation::Utf8Binary)
                    .coerce_to(&self.ty.native_type(), &DfType::Text(Collation::Utf8Binary))?
            }
        })
    }

    /// Closed syntax/type allowlist only; ReadySet still owns literal binding/coercion.
    pub fn accepts_literal(&self, value: &Literal) -> bool {
        let cell = match value {
            Literal::Integer(v) => Value::String(v.to_string()),
            Literal::UnsignedInteger(v) => Value::String(v.to_string()),
            Literal::Number(v) if matches!(self.ty, SourceType::Decimal { .. }) => {
                Value::String(v.clone())
            }
            Literal::String(v) if matches!(self.ty, SourceType::String) => Value::String(v.clone()),
            Literal::Boolean(v) if matches!(self.ty, SourceType::Boolean) => Value::Bool(*v),
            _ => return false,
        };
        // Integer tokens are not string or boolean predicates, even if their textual forms parse.
        if matches!(value, Literal::Integer(_) | Literal::UnsignedInteger(_)) && !self.ty.numeric()
        {
            return false;
        }
        if matches!(self.ty, SourceType::Decimal { .. })
            && matches!(value, Literal::Integer(_) | Literal::UnsignedInteger(_))
        {
            return self.ty.decimal_text(cell.as_str().unwrap()).is_ok();
        }
        self.decode(&cell).is_ok()
    }

    pub fn encode(&self, value: &DfValue) -> Result<Value> {
        if value.is_none() {
            ensure!(self.nullable, "native returned unexpected NULL");
            return Ok(Value::Null);
        }
        Ok(Value::String(match self.ty {
            SourceType::String => value.as_str().context("native non-text result")?.to_owned(),
            SourceType::Signed(_) => i64::try_from(value.clone())?.to_string(),
            SourceType::Unsigned(_) => u64::try_from(value.clone())?.to_string(),
            // Databend HTTP v1 string fields encode BOOLEAN as "1"/"0", not "true"/"false".
            SourceType::Boolean => u8::from(bool::try_from(value.clone())?).to_string(),
            SourceType::Decimal { scale, .. } => {
                ensure!(
                    matches!(value, DfValue::Numeric(_)),
                    "native non-decimal result"
                );
                // Formatting only, not evaluation: preserve Databend's fixed-scale wire value.
                let mut text = value.to_string();
                if scale > 0 {
                    if !text.contains('.') {
                        text.push('.');
                    }
                    let digits = text.split_once('.').unwrap().1.len();
                    ensure!(digits <= usize::from(scale), "native decimal scale changed");
                    text.extend(std::iter::repeat_n('0', usize::from(scale) - digits));
                }
                text
            }
        }))
    }
}

#[derive(Clone)]
pub struct SourceSchema {
    pub database: String,
    pub table: String,
    pub local: String,
    pub columns: Vec<SourceColumn>,
    pub(crate) context: Arc<SourceContext>,
}

impl SourceSchema {
    /// DESCRIBE is ordered and carries name/type/nullability independently. No source PKs
    /// or defaults are imported: source applies defaults; the local base stores full rows.
    pub fn from_describe(database: &str, table: &str, rows: &[Vec<Value>]) -> Result<Self> {
        ensure!(
            identifier(database) && identifier(table),
            "unsupported source identifier"
        );
        ensure!(
            !rows.is_empty() && rows.len() <= 256,
            "source must have 1..256 columns"
        );
        let mut seen = HashSet::new();
        let columns = rows
            .iter()
            .map(|row| {
                let name = row
                    .first()
                    .and_then(Value::as_str)
                    .context("DESCRIBE column name missing")?;
                ensure!(
                    !name.is_empty()
                        && name.len() <= 128
                        && !name.chars().any(char::is_control)
                        && !name.to_ascii_lowercase().starts_with("change$")
                        && seen.insert(name.to_ascii_lowercase()),
                    "invalid/duplicate/reserved source column"
                );
                let ty = SourceType::parse(
                    row.get(1)
                        .and_then(Value::as_str)
                        .context("DESCRIBE type missing")?,
                )
                .with_context(|| format!("unsupported source column {database}.{table}.{name}"))?;
                let nullable = match row.get(2).and_then(Value::as_str) {
                    Some("YES") => true,
                    Some("NO") => false,
                    _ => bail!("DESCRIBE nullability missing"),
                };
                Ok(SourceColumn {
                    name: name.into(),
                    ty,
                    nullable,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Stable, opaque relations avoid db1.orders/db2.orders and user cache collisions.
        let digest = hex::encode(Sha256::digest(format!("{database}\0{table}").as_bytes()));
        let local = format!("rs_source_{}", &digest[..24]);
        let context = Arc::new(SourceContext::for_columns(&local, columns.clone())?);
        Ok(Self {
            database: database.into(),
            table: table.into(),
            local,
            columns,
            context,
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", quote(&self.database), quote(&self.table))
    }
    pub fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
    pub fn ddl(&self) -> String {
        native_ddl(&self.local, &self.columns)
    }
    pub fn decode_row(&self, row: &[Value]) -> Result<Vec<DfValue>> {
        ensure!(
            row.len() == self.columns.len(),
            "source row arity differs from discovered schema"
        );
        self.columns
            .iter()
            .zip(row)
            .map(|(c, v)| {
                c.decode(v)
                    .with_context(|| format!("invalid source cell {}", c.name))
            })
            .collect()
    }
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(crate) fn native_ddl(local: &str, columns: &[SourceColumn]) -> String {
    format!(
        "CREATE TABLE {} ({})",
        quote(local),
        columns
            .iter()
            .map(|c| format!(
                "{} {}{}",
                quote(&c.name),
                c.ty.ddl(),
                if c.nullable { "" } else { " NOT NULL" }
            ))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Binding is explicit and database-qualified. There is deliberately no search across all
/// databases for an unqualified name, nor any JOIN/routing/distribution implementation.
#[derive(Default)]
pub struct SourceCatalog {
    sources: BTreeMap<(String, String), Arc<SourceSchema>>,
}
impl SourceCatalog {
    pub fn insert(&mut self, source: SourceSchema) -> Result<Arc<SourceSchema>> {
        let key = (source.database.clone(), source.table.clone());
        ensure!(
            !self.sources.contains_key(&key),
            "duplicate configured source"
        );
        let source = Arc::new(source);
        self.sources.insert(key, Arc::clone(&source));
        Ok(source)
    }
    pub fn resolve(&self, relation: &Relation, database: &str) -> Option<&Arc<SourceSchema>> {
        if relation.catalog.is_some() {
            return None;
        }
        self.sources.get(&(
            relation
                .schema
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or(database)
                .into(),
            relation.name.to_string(),
        ))
    }
}

#[cfg(test)]
#[path = "source_schema_tests.rs"]
mod tests;
