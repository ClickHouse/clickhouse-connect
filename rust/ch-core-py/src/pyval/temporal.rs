use super::*;

/// Split DateTime64 ticks at `precision` into whole seconds and microseconds.
/// Euclidean division so pre-epoch (negative) ticks floor correctly and the
/// microsecond remainder stays in `0..1_000_000`. Sub-microsecond digits
/// (precision 7..9) are truncated, since Python datetime resolves to
/// microseconds, matching clickhouse-connect.
pub(super) fn dt64_secs_micros(ticks: i64, precision: u8) -> (i64, u32) {
    let scale = 10i64.pow(precision as u32);
    let secs = ticks.div_euclid(scale);
    let frac = ticks.rem_euclid(scale);
    let micros = if precision <= 6 {
        frac * 10i64.pow(6 - precision as u32)
    } else {
        frac / 10i64.pow(precision as u32 - 6)
    };
    (secs, micros as u32)
}

/// Civil date (year, month, day) from a day count since 1970-01-01. Howard
/// Hinnant's `civil_from_days`; valid across the full Date/Date32 range. Month
/// and day are 1-based.
fn civil_from_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u8; // [1, 12]
    let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (year, m, d)
}

/// Calendar components (year, month, day, hour, minute, second) from seconds
/// since 1970-01-01 00:00:00 UTC. Euclidean split keeps the time of day in
/// `0..86_400` for negative (pre-epoch) inputs.
fn civil_from_secs(secs: i64) -> (i32, u8, u8, u8, u8, u8) {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = (tod / 3600) as u8;
    let minute = ((tod % 3600) / 60) as u8;
    let second = (tod % 60) as u8;
    (year, month, day, hour, minute, second)
}

/// Build a `datetime.date` from a day count since the epoch.
pub(super) fn make_date(py: Python<'_>, days: i64) -> PyResult<Bound<'_, PyAny>> {
    let (year, month, day) = civil_from_days(days);
    Ok(PyDate::new(py, year, month, day)?.into_any())
}

/// Build a `datetime.datetime` from epoch seconds plus microseconds, honoring
/// the column's ColumnCtx (naive UTC arithmetic, or tz-aware fromtimestamp).
pub(super) fn make_datetime<'py>(
    py: Python<'py>,
    secs: i64,
    micros: u32,
    ctx: &ColumnCtx<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    match (&ctx.tz, &ctx.fromtimestamp) {
        (Some(tz), Some(fromtimestamp)) => {
            if micros == 0 {
                fromtimestamp.call1((secs, tz))
            } else if secs.unsigned_abs() < (1 << 32) {
                // Single call on a float timestamp. Exact: |secs| < 2^32 keeps
                // the f64 ulp of the sum under one microsecond, so CPython's
                // round-to-nearest-microsecond recovers `micros` exactly.
                fromtimestamp.call1((secs as f64 + micros as f64 / 1e6, tz))
            } else {
                // Distant timestamps lose sub-microsecond float precision;
                // set the exact microsecond on the aware datetime instead.
                let dt = fromtimestamp.call1((secs, tz))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("microsecond", micros)?;
                dt.call_method("replace", (), Some(&kwargs))
            }
        }
        _ => {
            let (year, month, day, hour, minute, second) = civil_from_secs(secs);
            Ok(
                PyDateTime::new(py, year, month, day, hour, minute, second, micros, None)?
                    .into_any(),
            )
        }
    }
}

/// Build a Python timedelta from a signed second and microsecond total. The
/// components may be negative; normalize them into Python's canonical
/// day/second/microsecond representation without a Python arithmetic call.
fn make_timedelta<'py>(
    py: Python<'py>,
    seconds: i128,
    microseconds: i128,
) -> PyResult<Bound<'py, PyDelta>> {
    const MICROS_PER_SECOND: i128 = 1_000_000;
    const MICROS_PER_DAY: i128 = 86_400 * MICROS_PER_SECOND;

    let total_micros = seconds
        .checked_mul(MICROS_PER_SECOND)
        .and_then(|v| v.checked_add(microseconds))
        .ok_or_else(|| PyValueError::new_err("Time value overflows datetime.timedelta"))?;
    let days = total_micros.div_euclid(MICROS_PER_DAY);
    let day_micros = total_micros.rem_euclid(MICROS_PER_DAY);
    let day_seconds = day_micros / MICROS_PER_SECOND;
    let micros = day_micros % MICROS_PER_SECOND;
    let days = i32::try_from(days)
        .map_err(|_| PyValueError::new_err("Time value is outside datetime.timedelta range"))?;
    // day_seconds and micros are normalized to Python's documented ranges.
    PyDelta::new(py, days, day_seconds as i32, micros as i32, false)
}

/// Materialize Time64 ticks as timedelta with microsecond precision. Fractional
/// ticks below one microsecond are truncated toward zero, including negatives,
/// matching clickhouse_connect.datatypes.temporal.Time64._ticks_to_timedelta.
pub(super) fn make_time64<'py>(
    py: Python<'py>,
    ticks: i64,
    scale: u64,
) -> PyResult<Bound<'py, PyDelta>> {
    let negative = ticks < 0;
    let magnitude = ticks.unsigned_abs();
    let seconds = magnitude / scale;
    let frac = magnitude % scale;
    // scale <= 1e9, so the numerator fits u64 and avoids software u128 division
    // in this per-cell materialization path.
    let micros = frac * 1_000_000 / scale;
    let sign = if negative { -1i128 } else { 1i128 };
    if seconds <= i32::MAX as u64 {
        return PyDelta::new(
            py,
            0,
            (sign * i128::from(seconds)) as i32,
            (sign * i128::from(micros)) as i32,
            true,
        );
    }
    make_timedelta(py, sign * i128::from(seconds), sign * i128::from(micros))
}
