use super::*;

// ---------------------------------------------------------------------------
// Temporal value construction
//
// The core decodes temporal columns to their faithful native integer width and
// keeps timezone and precision in the schema's ChType, not in the buffers. The
// binding turns those integers into Python date/datetime objects here, matching
// the value policy of clickhouse-connect's Native reader:
//
//   Date, Date32                          -> datetime.date (naive)
//   DateTime / DateTime64, no tz or       -> datetime.datetime, no tzinfo,
//     a UTC-equivalent tz                     decoded as the UTC wall clock
//   DateTime / DateTime64, named non-UTC  -> datetime.datetime, tz-aware
//     tz                                      (zoneinfo.ZoneInfo)
//
// The naive path is pure epoch arithmetic with no per-row Python datetime
// parsing. The tz-aware path defers to datetime.fromtimestamp, which handles
// DST for the named zone.
// ---------------------------------------------------------------------------

/// Timezone names ClickHouse treats as UTC. A column in one of these renders as
/// a naive datetime, matching clickhouse-connect (tzutil.UTC_EQUIVALENTS).
const UTC_EQUIVALENTS: &[&str] = &[
    "UTC",
    "Etc/UTC",
    "UCT",
    "Etc/UCT",
    "GMT",
    "Etc/GMT",
    "GMT0",
    "GMT-0",
    "GMT+0",
    "Etc/GMT0",
    "Etc/GMT-0",
    "Etc/GMT+0",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Etc/Zulu",
    "Greenwich",
    "Etc/Greenwich",
];

/// Per-column context for materializing a column's host values, resolved once
/// per column rather than per cell. For a temporal column it carries the
/// timezone policy: a naive column (Date/Date32, or a DateTime/DateTime64 with
/// no timezone or a UTC-equivalent one) has `tz` `None` and is built by epoch
/// arithmetic, while a named non-UTC timezone holds its `zoneinfo.ZoneInfo` in
/// `tz` and the bound `datetime.datetime.fromtimestamp` in `fromtimestamp`. For
/// an Enum8/Enum16 column it carries `enum_names`, the value -> label-string map.
pub(crate) struct ColumnCtx<'py> {
    pub(super) tz: Option<Bound<'py, PyAny>>,
    pub(super) fromtimestamp: Option<Bound<'py, PyAny>>,
    /// DateTime64/Time64 fractional precision (decimal digits). 0 otherwise.
    pub(super) precision: u8,
    /// Time64 ticks per second, precomputed once per column. 1 otherwise.
    pub(super) time_scale: u64,
    /// Materialize Time/Time64 leaves as raw signed ticks instead of timedelta.
    pub(super) raw_time_ticks: bool,
    /// Enum value -> pre-built label string, for an Enum8/Enum16 column; `None`
    /// for any other type. A value missing from the map materializes as None,
    /// matching clickhouse-connect's `int_map.get(value, None)`.
    pub(super) enum_names: Option<HashMap<i64, Bound<'py, PyString>>>,
    /// `uuid.UUID` construction machinery, for a UUID column.
    pub(super) uuid: Option<UuidCtx<'py>>,
    /// `ipaddress` class machinery, for an IPv4/IPv6 column.
    pub(super) ip: Option<IpCtx<'py>>,
    /// The `decimal.Decimal` class, for a Decimal column.
    pub(super) decimal_cls: Option<Bound<'py, PyAny>>,
    /// Recursively-prepared context for the element type of an Array column;
    /// `None` for any other type.
    pub(super) element: Option<Box<ColumnCtx<'py>>>,
    /// Recursively-prepared per-field contexts. For a Tuple column, one per
    /// element in declaration order; for a Map column, exactly two (the key
    /// context then the value context); for a Variant column, one per dense
    /// alternative in the server's canonical discriminator order. JSON uses
    /// one entry per declared typed path. Dynamic children are block-local, so
    /// their contexts are prepared once per child in `fill_dynamic` instead of
    /// being stored here. `None` for any other type.
    pub(super) fields: Option<Vec<ColumnCtx<'py>>>,
    /// Pre-built element-name keys for a NAMED Tuple column, materialized as a
    /// `dict` keyed by these (clickhouse-connect's default read format).
    /// `None` for an unnamed Tuple (materialized as a `tuple`) and every
    /// non-Tuple type.
    pub(super) tuple_names: Option<Vec<Bound<'py, PyString>>>,
    /// Pre-split, percent-decoded keys for each declared JSON typed path.
    /// Dynamic and shared paths are block-local and are prepared once per path
    /// by `fill_json`.
    pub(super) json_paths: Option<Vec<JsonPath<'py>>>,
}

/// One ClickHouse JSON path prepared for repeated insertion into Python dicts.
/// Splitting precedes percent decoding because `%2E` represents a literal dot
/// inside one key rather than a nesting separator. `raw` keeps the declared
/// path spelling for the typed-path order guard.
pub(super) struct JsonPath<'py> {
    pub(super) raw: String,
    pub(super) keys: Vec<Bound<'py, PyString>>,
}

/// Cached objects to build a `uuid.UUID` the way the Cython codec does:
/// allocate via `UUID.__new__` and set the fields with `object.__setattr__`,
/// bypassing the parsing constructor and the immutability guard.
pub(super) struct UuidCtx<'py> {
    pub(super) cls: Bound<'py, PyAny>,
    pub(super) new: Bound<'py, PyAny>,
    pub(super) object_setattr: Bound<'py, PyAny>,
    pub(super) unsafe_marker: Bound<'py, PyAny>,
}

/// Cached objects to build an `ipaddress.IPv4Address`/`IPv6Address` via
/// `__new__` plus a plain `_ip` setattr (neither class guards setattr).
pub(super) struct IpCtx<'py> {
    pub(super) cls: Bound<'py, PyAny>,
    pub(super) new: Bound<'py, PyAny>,
    /// `IPv6Address.__slots__` includes `_scope_id` (Python 3.9+); each value
    /// gets `_scope_id = None`.
    pub(super) set_scope_id: bool,
}

/// Build an enum's value -> label-string map, one Python str per variant created
/// once for the whole column.
fn enum_name_map<'py, V: Copy + Into<i64>>(
    py: Python<'py>,
    variants: &[(String, V)],
) -> HashMap<i64, Bound<'py, PyString>> {
    variants
        .iter()
        .map(|(name, value)| ((*value).into(), PyString::new(py, name)))
        .collect()
}

pub(super) fn prepare_json_path<'py>(py: Python<'py>, path: &str) -> PyResult<JsonPath<'py>> {
    let unquote = path
        .contains('%')
        .then(|| py.import("urllib.parse")?.getattr("unquote"))
        .transpose()?;
    let keys = path
        .split('.')
        .map(|segment| match &unquote {
            Some(unquote) => unquote
                .call1((segment,))?
                .downcast_into::<PyString>()
                .map_err(Into::into),
            None => Ok(PyString::new(py, segment)),
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(JsonPath {
        raw: path.to_owned(),
        keys,
    })
}

/// Resolve a column's ChType into a ColumnCtx. Cheap for the common naive case
/// (no Python calls); imports zoneinfo only for a named non-UTC zone and builds
/// the enum map only for an enum. Safe to call for any column type; types that
/// need neither yield the naive, non-enum default.
pub(crate) fn prepare_column_ctx<'py>(
    py: Python<'py>,
    ch_type: &ChType,
    raw_time_ticks: bool,
) -> PyResult<ColumnCtx<'py>> {
    // Unwrap LowCardinality to reach the value type before inner() strips any
    // Nullable, so LowCardinality(DateTime(tz)) still applies timezone policy.
    let resolved = match ch_type {
        ChType::LowCardinality(inner) => inner.inner(),
        other => other.inner(),
    };

    // SimpleAggregateFunction, the geo aliases, and Nested carry only a custom
    // name over a physical type; the decoded Column is that physical type's
    // column, so build the context from the delegate and recurse. `resolved`
    // has already stripped any LowCardinality/Nullable, so Nullable(Point)
    // reaches here as Geo(Point).
    if let Some(delegate) = resolved.physical_delegate() {
        return prepare_column_ctx(py, &delegate, raw_time_ticks);
    }

    let enum_names = match resolved {
        ChType::Enum8 { variants } => Some(enum_name_map(py, variants)),
        ChType::Enum16 { variants } => Some(enum_name_map(py, variants)),
        _ => None,
    };

    let (timezone, precision, time_scale) = match resolved {
        ChType::DateTime { timezone } => (timezone.as_deref(), 0u8, 1),
        ChType::DateTime64 {
            precision,
            timezone,
        } => (timezone.as_deref(), *precision, 1),
        ChType::Time64 { precision } => (None, *precision, 10u64.pow(u32::from(*precision))),
        _ => (None, 0u8, 1),
    };

    let (tz, fromtimestamp) = match timezone {
        Some(tz) if !UTC_EQUIVALENTS.contains(&tz) => {
            let zone = py.import("zoneinfo")?.getattr("ZoneInfo")?.call1((tz,))?;
            let fromtimestamp = py
                .import("datetime")?
                .getattr("datetime")?
                .getattr("fromtimestamp")?;
            (Some(zone), Some(fromtimestamp))
        }
        _ => (None, None),
    };

    let (uuid, ip, decimal_cls) = match resolved {
        ChType::Uuid => {
            let module = py.import("uuid")?;
            let cls = module.getattr("UUID")?;
            let new = cls.getattr("__new__")?;
            let object_setattr = py
                .import("builtins")?
                .getattr("object")?
                .getattr("__setattr__")?;
            let unsafe_marker = module.getattr("SafeUUID")?.getattr("unsafe")?;
            (
                Some(UuidCtx {
                    cls,
                    new,
                    object_setattr,
                    unsafe_marker,
                }),
                None,
                None,
            )
        }
        ChType::Ipv4 => {
            let cls = py.import("ipaddress")?.getattr("IPv4Address")?;
            let new = cls.getattr("__new__")?;
            (
                None,
                Some(IpCtx {
                    cls,
                    new,
                    set_scope_id: false,
                }),
                None,
            )
        }
        ChType::Ipv6 => {
            let cls = py.import("ipaddress")?.getattr("IPv6Address")?;
            let new = cls.getattr("__new__")?;
            let set_scope_id = cls
                .getattr("__slots__")?
                .contains(intern!(py, "_scope_id"))?;
            (
                None,
                Some(IpCtx {
                    cls,
                    new,
                    set_scope_id,
                }),
                None,
            )
        }
        ChType::Decimal { .. } => (None, None, Some(py.import("decimal")?.getattr("Decimal")?)),
        _ => (None, None, None),
    };

    // `.inner()` only strips Nullable, so an Array column's resolved type is the
    // `Array(elem)` itself. Recurse into the element to build its machinery,
    // which transparently covers every element shape (Nullable, LowCardinality,
    // nested Array, temporal-with-tz, enum, uuid, ip, decimal).
    let element = match resolved {
        ChType::Array(elem) => Some(Box::new(prepare_column_ctx(py, elem, raw_time_ticks)?)),
        _ => None,
    };

    // A Tuple builds one field context per element and, for a named tuple, the
    // pre-built name keys. A Map builds exactly two field contexts (key then
    // value); its entries live in a nested Tuple column reached directly, so it
    // needs no tuple_names. `resolved` is the Nullable-unwrapped type, so a
    // `Nullable(Tuple(...))` reaches the Tuple arm here.
    let (fields, tuple_names, json_paths) = match resolved {
        ChType::Tuple(elements) => {
            let fields = elements
                .iter()
                .map(|(_, t)| prepare_column_ctx(py, t, raw_time_ticks))
                .collect::<PyResult<Vec<_>>>()?;
            let named = !elements.is_empty() && elements.iter().all(|(name, _)| name.is_some());
            // `named` guarantees every element has a name; the empty-string
            // fallback keeps the map total (one key per field) without an
            // unwrap even though it is never taken.
            let names = if named {
                Some(
                    elements
                        .iter()
                        .map(|(name, _)| PyString::new(py, name.as_deref().unwrap_or_default()))
                        .collect(),
                )
            } else {
                None
            };
            (Some(fields), names, None)
        }
        ChType::Map(key, value) => {
            let fields = vec![
                prepare_column_ctx(py, key, raw_time_ticks)?,
                prepare_column_ctx(py, value, raw_time_ticks)?,
            ];
            (Some(fields), None, None)
        }
        // Variant alternatives always materialize finalized values; the
        // driver's raw-ticks materializer does not walk Variant cells.
        ChType::Variant(alternatives) => {
            let fields = alternatives
                .iter()
                .map(|alternative| prepare_column_ctx(py, alternative, false))
                .collect::<PyResult<Vec<_>>>()?;
            (Some(fields), None, None)
        }
        ChType::Json { typed_paths, .. } => {
            let fields = typed_paths
                .iter()
                .map(|(_, ch_type)| prepare_column_ctx(py, ch_type, raw_time_ticks))
                .collect::<PyResult<Vec<_>>>()?;
            let paths = typed_paths
                .iter()
                .map(|(path, _)| prepare_json_path(py, path))
                .collect::<PyResult<Vec<_>>>()?;
            (Some(fields), None, Some(paths))
        }
        _ => (None, None, None),
    };

    Ok(ColumnCtx {
        tz,
        fromtimestamp,
        precision,
        time_scale,
        raw_time_ticks,
        enum_names,
        uuid,
        ip,
        decimal_cls,
        element,
        fields,
        tuple_names,
        json_paths,
    })
}
